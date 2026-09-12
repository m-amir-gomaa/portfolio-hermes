package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/qwerty/hermes/internal/agent"
	"github.com/qwerty/hermes/internal/handlers"
	"github.com/qwerty/hermes/internal/httpclient"
	"github.com/qwerty/hermes/internal/rag"
	openai "github.com/sashabaranov/go-openai"
)

func main() {
	log.Println("Starting Hermes Agentic RAG Pipeline...")

	// 1. Setup API Client pointing to DeepSeek (or OpenAI)
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		log.Fatal("DEEPSEEK_API_KEY environment variable is not set")
	}

	config := openai.DefaultConfig(apiKey)
	config.BaseURL = "https://api.deepseek.com/v1" // Use DeepSeek compatible endpoint
	config.HTTPClient = httpclient.NewResilientClient()

	client := openai.NewClientWithConfig(config)

	// 2. Initialize the In-Memory Vector Store
	vectorStore := rag.NewVectorStore()

	// 3. Initialize the Concurrent Embedder (Worker Pool: 50)
	embedder := rag.NewEmbedder(client, vectorStore, 50)

	// 4. Initialize the Agent Orchestrator
	orchestrator := agent.NewOrchestrator(client, vectorStore)

	// 5. Setup HTTP Routes
	api := handlers.NewAPI(embedder, orchestrator)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/ingest", api.HandleIngest)
	mux.HandleFunc("/api/v1/ask", api.HandleAsk)

	// 6. Graceful Shutdown Setup (CSAPP Concept)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("Listening on :8080...")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Intercept SIGINT (Ctrl+C) and SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting")
}
