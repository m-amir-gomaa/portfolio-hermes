package rag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/qwerty/hermes/internal/domain"
	openai "github.com/sashabaranov/go-openai"
)

// Embedder handles concurrent ingestion of documents.
type Embedder struct {
	client      *openai.Client
	vectorStore *VectorStore
	numWorkers  int
}

func NewEmbedder(client *openai.Client, store *VectorStore, numWorkers int) *Embedder {
	return &Embedder{
		client:      client,
		vectorStore: store,
		numWorkers:  numWorkers,
	}
}

// Ingest asynchronously chunks documents and embeds them using a bounded worker pool.
func (e *Embedder) Ingest(ctx context.Context, req domain.IngestRequest) error {
	jobs := make(chan domain.Chunk, 100)
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < e.numWorkers; i++ {
		wg.Add(1)
		go e.worker(ctx, &wg, jobs)
	}

	// Chunk and enqueue jobs
	for _, doc := range req.Documents {
		// Naive chunking for V1: split by paragraphs or arbitrary size
		// In a real system, you'd use a sliding window token chunker
		chunks := chunkText(doc.Text, 500)
		for i, text := range chunks {
			jobs <- domain.Chunk{
				ID:         fmt.Sprintf("%s-%d", doc.ID, i),
				DocumentID: doc.ID,
				Text:       text,
			}
		}
	}

	// Close channel to signal workers no more jobs are coming
	close(jobs)

	// Wait for all workers to finish
	wg.Wait()
	return nil
}

func (e *Embedder) worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan domain.Chunk) {
	defer wg.Done()

	for chunk := range jobs {
		// Implement exponential backoff for 429 Too Many Requests
		maxRetries := 3
		backoff := 1 * time.Second

		for attempt := 0; attempt <= maxRetries; attempt++ {
			req := openai.EmbeddingRequest{
				Input: []string{chunk.Text},
				Model: openai.AdaEmbeddingV2, // Or DeepSeek equivalent
			}

			resp, err := e.client.CreateEmbeddings(ctx, req)
			if err != nil {
				// Naive error check. In production, inspect specifically for HTTP 429
				if attempt == maxRetries {
					fmt.Printf("Failed to embed chunk %s after %d attempts: %v\n", chunk.ID, maxRetries, err)
					break
				}
				time.Sleep(backoff)
				backoff *= 2
				continue
			}

			if len(resp.Data) > 0 {
				chunk.Embedding = resp.Data[0].Embedding
				e.vectorStore.Insert(chunk)
			}
			break
		}
	}
}

func chunkText(text string, maxChars int) []string {
	var chunks []string
	runes := []rune(text)
	for i := 0; i < len(runes); i += maxChars {
		end := i + maxChars
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[i:end]))
	}
	return chunks
}
