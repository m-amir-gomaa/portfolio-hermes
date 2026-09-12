# Hermes: Agentic RAG Pipeline Orchestrator

Hermes is a highly concurrent, zero-dependency (beyond the OpenAI/DeepSeek SDK) RAG (Retrieval-Augmented Generation) pipeline orchestrator written in Go. 

It is designed to solve the two biggest problems in AI infrastructure:
1. **Memory Exhaustion (OOM):** Ingesting 10,000 documents sequentially is too slow. Ingesting them simultaneously via unrestricted goroutines crashes the server.
2. **API Rate Limiting:** Hitting OpenAI/DeepSeek with 10,000 concurrent requests guarantees a ban.

Hermes solves this by applying strict **Computer Systems (CSAPP) principles** to modern AI workflows.

## Core Architecture

### 1. The Bounded Worker Pool (Ingestion)
When the `/api/v1/ingest` endpoint receives a massive payload of documents, it immediately returns a `202 Accepted` response so the client is not blocked. 
Behind the scenes, the documents are chunked and fed into a bounded Go channel (`chan domain.Chunk`). A fixed worker pool of 50 goroutines reads from this channel, calculating embeddings concurrently. This ensures memory usage is strictly bounded, regardless of payload size.

### 2. TCP Port Exhaustion Prevention
When making thousands of HTTP requests to an LLM provider, default HTTP clients will exhaust the server's ephemeral TCP ports (TIME_WAIT state). 
Hermes implements a custom `http.Client` that explicitly controls `MaxIdleConnsPerHost` and connection draining timeouts, ensuring stable network performance under sustained load.

### 3. Graceful Exponential Backoff
Workers intercept `429 Too Many Requests` errors from LLM APIs and automatically execute a jittered exponential backoff strategy (1s → 2s → 4s) locally before re-queueing the job, ensuring zero data loss during rate limits.

### 4. Thread-Safe Vector Store
The RAG embedding store uses `sync.RWMutex` to allow highly concurrent reads during user queries while safely locking during asynchronous document ingestion. Semantic search is performed using Cosine Similarity math directly on the vectors.

### 5. Graceful Shutdown
Hermes intercepts OS signals (`SIGINT`, `SIGTERM`). When a termination signal is received, the HTTP server stops accepting new requests, and the dispatcher waits for all in-flight workers in the channel to drain completely before exiting the process.

## API Usage

**1. Ingest Documents (Async)**
```bash
curl -X POST http://localhost:8080/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{"batch_id": "1", "documents": [{"id": "doc1", "text": "Go is a statically typed, compiled programming language designed at Google."}]}'
```

**2. Query the Agent (RAG)**
```bash
curl -X POST http://localhost:8080/api/v1/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "Who designed Go?"}'
```

## Running Locally
```bash
export DEEPSEEK_API_KEY="your-api-key"
go build -o hermes main.go
./hermes
```
