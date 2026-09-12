package domain

// IngestRequest represents an incoming payload of documents to embed
type IngestRequest struct {
	BatchID   string     `json:"batch_id"`
	Documents []Document `json:"documents"`
}

// Document represents raw text to be embedded
type Document struct {
	ID       string `json:"id"`
	Text     string `json:"text"`
	Metadata string `json:"metadata"`
}

// Chunk represents a segment of a document with its vector embedding
type Chunk struct {
	ID         string
	DocumentID string
	Text       string
	Embedding  []float32
}

// AgentRequest represents a query to the agent
type AgentRequest struct {
	Query string `json:"query"`
}
