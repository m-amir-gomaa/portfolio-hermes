package agent

import (
	"context"
	"fmt"

	"github.com/qwerty/hermes/internal/rag"
	openai "github.com/sashabaranov/go-openai"
)

// Orchestrator coordinates the interaction between the user prompt,
// the LLM (for reasoning/agentic decisions), and the Vector Store (for context).
type Orchestrator struct {
	client      *openai.Client
	vectorStore *rag.VectorStore
}

func NewOrchestrator(client *openai.Client, store *rag.VectorStore) *Orchestrator {
	return &Orchestrator{
		client:      client,
		vectorStore: store,
	}
}

// Ask evaluates a user query, retrieves relevant context from the vector store,
// and returns the LLM's synthesized response.
func (o *Orchestrator) Ask(ctx context.Context, query string) (string, error) {
	// 1. Embed the user's query
	req := openai.EmbeddingRequest{
		Input: []string{query},
		Model: openai.AdaEmbeddingV2,
	}
	resp, err := o.client.CreateEmbeddings(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to embed query: %w", err)
	}
	queryEmbedding := resp.Data[0].Embedding

	// 2. Perform Cosine Similarity search in the Vector Store
	results, err := o.vectorStore.Search(queryEmbedding, 3) // Top 3 most relevant chunks
	if err != nil {
		// If store is empty, fallback to pure LLM
		results = nil
	}

	// 3. Construct the augmented context
	var contextText string
	for _, res := range results {
		contextText += fmt.Sprintf("- %s\n", res.Chunk.Text)
	}

	systemPrompt := "You are a helpful AI assistant. Answer the user's question using ONLY the provided context. If the answer is not in the context, say 'I don't know'."
	if contextText == "" {
		systemPrompt = "You are a helpful AI assistant."
	}

	userMessage := fmt.Sprintf("Context:\n%s\n\nQuestion: %s", contextText, query)

	// 4. Send the augmented prompt to the LLM
	chatReq := openai.ChatCompletionRequest{
		Model: openai.GPT3Dot5Turbo, // Or DeepSeek Chat
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleSystem,
				Content: systemPrompt,
			},
			{
				Role:    openai.ChatMessageRoleUser,
				Content: userMessage,
			},
		},
	}

	chatResp, err := o.client.CreateChatCompletion(ctx, chatReq)
	if err != nil {
		return "", fmt.Errorf("failed to generate response: %w", err)
	}

	return chatResp.Choices[0].Message.Content, nil
}
