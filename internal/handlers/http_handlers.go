package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/qwerty/hermes/internal/agent"
	"github.com/qwerty/hermes/internal/domain"
	"github.com/qwerty/hermes/internal/rag"
)

type API struct {
	Embedder     *rag.Embedder
	Orchestrator *agent.Orchestrator
}

func NewAPI(e *rag.Embedder, o *agent.Orchestrator) *API {
	return &API{
		Embedder:     e,
		Orchestrator: o,
	}
}

// HandleIngest accepts a batch of documents and kicks off concurrent ingestion.
func (api *API) HandleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req domain.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// We process this asynchronously so the client doesn't block waiting for LLM embeddings
	go api.Embedder.Ingest(context.Background(), req) // In real prod, use a worker queue for this boundary

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "accepted", "message": "ingestion started concurrently"}`))
}

// HandleAsk accepts a question and returns an AI response using RAG.
func (api *API) HandleAsk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req domain.AgentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	answer, err := api.Orchestrator.Ask(r.Context(), req.Query)
	if err != nil {
		http.Error(w, "Internal server error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"query":  req.Query,
		"answer": answer,
	})
}
