package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

// WebhookTask represents a single webhook to be dispatched
type WebhookTask struct {
	ID        int
	Payload   string
	CreatedAt time.Time
}

// Dispatcher manages the worker pool
type Dispatcher struct {
	WorkerPool chan chan WebhookTask
	JobQueue   chan WebhookTask
	MaxWorkers int
}

func NewDispatcher(maxWorkers int, maxQueue int) *Dispatcher {
	return &Dispatcher{
		WorkerPool: make(chan chan WebhookTask, maxWorkers),
		JobQueue:   make(chan WebhookTask, maxQueue),
		MaxWorkers: maxWorkers,
	}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go func() {
		for job := range d.JobQueue {
			go func(job WebhookTask) {
				workerChannel := <-d.WorkerPool
				workerChannel <- job
			}(job)
		}
	}()
}

// Worker executes tasks
type Worker struct {
	WorkerPool chan chan WebhookTask
	JobChannel chan WebhookTask
	quit       chan bool
}

func NewWorker(workerPool chan chan WebhookTask) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan WebhookTask),
		quit:       make(chan bool),
	}
}

func (w Worker) Start() {
	go func() {
		for {
			w.WorkerPool <- w.JobChannel
			select {
			case job := <-w.JobChannel:
				// Simulate HTTP request latency (e.g., hitting an external API)
				time.Sleep(50 * time.Millisecond)
				log.Printf("Worker dispatched webhook ID: %d", job.ID)
			case <-w.quit:
				return
			}
		}
	}()
}

// HTTP Handlers
func (d *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}

	if req.Count <= 0 || req.Count > 10000 {
		req.Count = 10
	}

	for i := 1; i <= req.Count; i++ {
		task := WebhookTask{
			ID:        i,
			Payload:   `{"event": "user.created"}`,
			CreatedAt: time.Now(),
		}
		d.JobQueue <- task
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message": fmt.Sprintf("Successfully enqueued %d webhooks for background processing", req.Count),
	})
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Init dispatcher with 50 workers and a buffer of 10,000 jobs
	dispatcher := NewDispatcher(50, 10000)
	dispatcher.Run()

	mux := http.NewServeMux()
	
	// Serve static frontend
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "index.html")
	})

	// Serve API endpoint
	mux.HandleFunc("/api/v1/dispatch", dispatcher.ServeHTTP)

	log.Printf("Hermes Webhook Dispatcher started on port %s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
