package main

import (
	"fmt"
	"log"

	// Update this import path to match your actual module name in go.mod
	"github.com/devXyi/Prexus-Intelligence-/backend/internal/kernel"
)

func main() {
	fmt.Println("🚀 Prexus Intelligence — Claude Integration Test")
	fmt.Println("─────────────────────────────────────────────────")

	// Example: Ask Claude to analyze a probability scenario
	prompt := `You are a probability intelligence engine.
Analyze this scenario and give a probability score from 0 to 100:
"What is the probability that a new tech startup in India succeeds within 2 years?"`

	fmt.Println("📤 Sending to Claude...")

	response, err := kernel.AnalyzeProbability(prompt)
	if err != nil {
		log.Fatalf("❌ Error: %v", err)
	}

	fmt.Println("✅ Claude Response:")
	fmt.Println("─────────────────────────────────────────────────")
	fmt.Println(response)
}
