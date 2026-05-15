// backend/apps/api-gateway/apply.go
// Prexus Intelligence — Access Application Handler
//
// Recipient email addresses are loaded exclusively from environment variables
// and never transmitted to or visible in the client / frontend.
//
// Required env vars:
//   SMTP_HOST   — e.g. smtp.gmail.com
//   SMTP_PORT   — e.g. 587
//   SMTP_USER   — sending Gmail address (needs an App Password)
//   SMTP_PASS   — Gmail App Password (not your account password)
//
// Recipient addresses are hardcoded here (server-side only).
// To override them without redeploying, set NOTIFY_EMAILS as a
// comma-separated list in Render's environment variables.

package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/smtp"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// ── Recipient config ──────────────────────────────────────────────────────────
// These addresses never leave the server. The frontend only receives a
// reference number — it never sees who the mail is sent to.

func notifyRecipients() []string {
	if raw := os.Getenv("NOTIFY_EMAILS"); raw != "" {
		var list []string
		for _, e := range strings.Split(raw, ",") {
			if t := strings.TrimSpace(e); t != "" {
				list = append(list, t)
			}
		}
		return list
	}
	// Fallback: hardcoded server-side — NOT exposed to any client
	return []string{
		"devmani@prexus.in",
		"tripathidevmani023@gmail.com",
	}
}

// ── Request model ─────────────────────────────────────────────────────────────

type ApplyRequest struct {
	// Plan selection (set by hidden fields)
	Module     string `json:"module"     binding:"required"`
	Plan       string `json:"plan"       binding:"required"`
	Deployment string `json:"deployment"`

	// Applicant
	Name    string `json:"name"    binding:"required"`
	Title   string `json:"title"`
	Email   string `json:"email"   binding:"required,email"`
	Org     string `json:"org"     binding:"required"`
	Country string `json:"country" binding:"required"`
	OrgType string `json:"org_type" binding:"required"`
	UseCase string `json:"use_case"`
}

// ── Handler ──────────────────────────────────────────────────────────────────

func handleApply(c *gin.Context) {
	var req ApplyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Generate reference number
	ref := fmt.Sprintf("PRX-%s-%s",
		strings.ToUpper(req.Module[:min3(len(req.Module))]),
		strings.ToUpper(fmt.Sprintf("%x", time.Now().UnixNano()))[len(fmt.Sprintf("%x", time.Now().UnixNano()))-6:],
	)

	// Fire email in background — don't block the HTTP response
	go func() {
		if err := sendApplicationEmail(req, ref); err != nil {
			log.Printf("[apply] email send error ref=%s: %v", ref, err)
		} else {
			log.Printf("[apply] notification sent ref=%s module=%s plan=%s", ref, req.Module, req.Plan)
		}
	}()

	c.JSON(http.StatusOK, gin.H{
		"status": "submitted",
		"ref":    ref,
	})
}

// ── Email composition ─────────────────────────────────────────────────────────

func sendApplicationEmail(req ApplyRequest, ref string) error {
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")
	smtpUser := os.Getenv("SMTP_USER")
	smtpPass := os.Getenv("SMTP_PASS")

	if smtpHost == "" || smtpUser == "" || smtpPass == "" {
		return fmt.Errorf("SMTP env vars not configured (SMTP_HOST, SMTP_USER, SMTP_PASS)")
	}
	if smtpPort == "" {
		smtpPort = "587"
	}

	recipients := notifyRecipients()
	subject := fmt.Sprintf("New Access Application — %s · %s [%s]",
		req.Module, req.Plan, ref)

	useCase := req.UseCase
	if useCase == "" {
		useCase = "(not provided)"
	}

	body := fmt.Sprintf(`PREXUS INTELLIGENCE PLATFORM
Access Application Received
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Reference:    %s
Submitted:    %s UTC

MODULE & PLAN
  Module:     %s
  Plan:       %s
  Deployment: %s

APPLICANT
  Full Name:  %s
  Job Title:  %s
  Email:      %s

ORGANIZATION
  Name:       %s
  Country:    %s
  Type:       %s

USE CASE / CONTEXT
%s

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Prexus Intelligence Platform — Automated Notification
Do not reply to this message.
`,
		ref,
		time.Now().UTC().Format("2006-01-02 15:04:05"),
		req.Module, req.Plan, req.Deployment,
		req.Name, req.Title, req.Email,
		req.Org, req.Country, req.OrgType,
		useCase,
	)

	msg := buildMIMEMessage(smtpUser, recipients, subject, body)
	auth := smtp.PlainAuth("", smtpUser, smtpPass, smtpHost)

	// Use STARTTLS on port 587
	addr := net.JoinHostPort(smtpHost, smtpPort)

	tlsConf := &tls.Config{
		InsecureSkipVerify: false,
		ServerName:         smtpHost,
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	client, err := smtp.NewClient(conn, smtpHost)
	if err != nil {
		return fmt.Errorf("smtp client: %w", err)
	}
	defer client.Close()

	if err = client.StartTLS(tlsConf); err != nil {
		return fmt.Errorf("starttls: %w", err)
	}
	if err = client.Auth(auth); err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	if err = client.Mail(smtpUser); err != nil {
		return fmt.Errorf("mail from: %w", err)
	}
	for _, r := range recipients {
		if err = client.Rcpt(r); err != nil {
			return fmt.Errorf("rcpt %s: %w", r, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("data: %w", err)
	}
	_, err = fmt.Fprint(w, msg)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return w.Close()
}

func buildMIMEMessage(from string, to []string, subject, body string) string {
	return fmt.Sprintf(
		"From: Prexus Applications <%s>\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		from,
		strings.Join(to, ", "),
		subject,
		body,
	)
}

// min3 returns the minimum of n and 3 (for safe module prefix slicing).
func min3(n int) int {
	if n < 3 {
		return n
	}
	return 3
}
