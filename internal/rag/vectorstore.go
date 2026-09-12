package rag

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/qwerty/hermes/internal/domain"
)

// VectorStore handles storing and searching embeddings.
// For V1, this is an in-memory store using Cosine Similarity.
// In V2, this will be swapped for PostgreSQL + pgvector.
type VectorStore struct {
	mu     sync.RWMutex
	chunks []domain.Chunk
}

func NewVectorStore() *VectorStore {
	return &VectorStore{
		chunks: make([]domain.Chunk, 0),
	}
}

// Insert adds a new chunk with its embedding
func (vs *VectorStore) Insert(chunk domain.Chunk) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.chunks = append(vs.chunks, chunk)
}

type SearchResult struct {
	Chunk      domain.Chunk
	Similarity float32
}

// Search returns the top K most similar chunks using Cosine Similarity
func (vs *VectorStore) Search(queryEmbedding []float32, topK int) ([]SearchResult, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if len(vs.chunks) == 0 {
		return nil, errors.New("vector store is empty")
	}

	var results []SearchResult

	for _, chunk := range vs.chunks {
		sim := cosineSimilarity(queryEmbedding, chunk.Embedding)
		results = append(results, SearchResult{
			Chunk:      chunk,
			Similarity: sim,
		})
	}

	// Sort by similarity descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Similarity > results[j].Similarity
	})

	if topK > len(results) {
		topK = len(results)
	}

	return results[:topK], nil
}

func cosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0.0
	}
	var dotProduct, normA, normB float32
	for i := 0; i < len(a); i++ {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0.0
	}
	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}
