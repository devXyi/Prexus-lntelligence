// backend/apps/api-gateway/claude.go
// Prexus Intelligence — AI Provider Bridge
// Routes AnalyzeProbability calls to Claude / Gemini / OpenAI.
//
// Fixes applied:
//   [BUG-1] http.DefaultClient had no timeout → goroutine leak on hung responses
//   [BUG-2] json.Marshal / json.Unmarshal errors silently dropped throughout
//   [BUG-3] No context propagation — context now threaded from caller end-to-end
//   [BUG-4] Double timeout removed — caller owns the context, sub-functions use it

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// sharedAIClient is a single HTTP client reused across all AI calls.
// No Timeout set here — context deadline passed by the caller controls cutoff.
// [FIX-BUG-1] Still avoids http.DefaultClient which has no timeout at all.
var sharedAIClient = &http.Client{}

// AnalyzeProbability dispatches to the correct AI provider.
// ctx is owned by the caller (handleClaude sets a 30s deadline).
func AnalyzeProbability(ctx context.Context, prompt string, model string) (string, error) {
	switch model {
	case "gemini":
		return callGemini(ctx, prompt)
	case "chatgpt":
		return callOpenAI(ctx, prompt)
	default:
		return callClaude(ctx, prompt)
	}
}

// ── Claude (Anthropic) ────────────────────────────────────────────────────────

func callClaude(ctx context.Context, prompt string) (string, error) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("ANTHROPIC_API_KEY not set")
	}

	body, err := json.Marshal(map[string]interface{}{
		"model":      "claude-haiku-4-5",
		"max_tokens": 1024,
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
	})
	if err != nil {
		return "", fmt.Errorf("claude: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.anthropic.com/v1/messages", bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("claude: create request: %w", err)
	}
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")

	resp, err := sharedAIClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("claude: http error: %w", err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("claude: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("claude: http %d: %s", resp.StatusCode, truncate(b, 200))
	}

	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return "", fmt.Errorf("claude: unmarshal response (status %d): %w | body: %s",
			resp.StatusCode, err, truncate(b, 200))
	}
	if result.Error != nil {
		return "", fmt.Errorf("claude API error: %s", result.Error.Message)
	}
	if len(result.Content) == 0 {
		return "", fmt.Errorf("claude: empty response (status %d): %s",
			resp.StatusCode, truncate(b, 200))
	}
	return result.Content[0].Text, nil
}

// ── Gemini (Google) ───────────────────────────────────────────────────────────

func callGemini(ctx context.Context, prompt string) (string, error) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("GEMINI_API_KEY not set")
	}

	url := fmt.Sprintf(
		"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=%s",
		apiKey,
	)

	body, err := json.Marshal(map[string]interface{}{
		"contents": []map[string]interface{}{
			{"parts": []map[string]string{{"text": prompt}}},
		},
	})
	if err != nil {
		return "", fmt.Errorf("gemini: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("gemini: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := sharedAIClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("gemini: http error: %w", err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("gemini: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("gemini: http %d: %s", resp.StatusCode, truncate(b, 200))
	}

	var errResp struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(b, &errResp); err != nil {
		return "", fmt.Errorf("gemini: unmarshal error envelope (status %d): %w | body: %s",
			resp.StatusCode, err, truncate(b, 200))
	}
	if errResp.Error != nil {
		return "", fmt.Errorf("gemini API error %d: %s", errResp.Error.Code, errResp.Error.Message)
	}

	var result struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return "", fmt.Errorf("gemini: unmarshal candidates (status %d): %w | body: %s",
			resp.StatusCode, err, truncate(b, 200))
	}
	if len(result.Candidates) == 0 || len(result.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("gemini: empty candidates (status %d): %s",
			resp.StatusCode, truncate(b, 200))
	}
	return result.Candidates[0].Content.Parts[0].Text, nil
}

// ── OpenAI (GPT-4o) ───────────────────────────────────────────────────────────

func callOpenAI(ctx context.Context, prompt string) (string, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("OPENAI_API_KEY not set")
	}

	body, err := json.Marshal(map[string]interface{}{
		"model":      "gpt-4o",
		"max_tokens": 1024,
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
	})
	if err != nil {
		return "", fmt.Errorf("openai: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.openai.com/v1/chat/completions", bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("openai: create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := sharedAIClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("openai: http error: %w", err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("openai: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("openai: http %d: %s", resp.StatusCode, truncate(b, 200))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return "", fmt.Errorf("openai: unmarshal response (status %d): %w | body: %s",
			resp.StatusCode, err, truncate(b, 200))
	}
	if result.Error != nil {
		return "", fmt.Errorf("openai API error: %s", result.Error.Message)
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("openai: empty choices (status %d): %s",
			resp.StatusCode, truncate(b, 200))
	}
	return result.Choices[0].Message.Content, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
