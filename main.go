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

// Broker manages SSE connections
type Broker struct {
	clients        map[chan string]bool
	newClients     chan chan string
	defunctClients chan chan string
	messages       chan string
}

func NewBroker() *Broker {
	return &Broker{
		clients:        make(map[chan string]bool),
		newClients:     make(chan chan string),
		defunctClients: make(chan chan string),
		messages:       make(chan string),
	}
}

func (b *Broker) Start() {
	go func() {
		for {
			select {
			case s := <-b.newClients:
				b.clients[s] = true
			case s := <-b.defunctClients:
				delete(b.clients, s)
				close(s)
			case msg := <-b.messages:
				for s := range b.clients {
					s <- msg
				}
			}
		}
	}()
}

// Dispatcher manages the worker pool
type Dispatcher struct {
	WorkerPool chan chan WebhookTask
	JobQueue   chan WebhookTask
	MaxWorkers int
	Broker     *Broker
}

func NewDispatcher(maxWorkers int, maxQueue int, broker *Broker) *Dispatcher {
	return &Dispatcher{
		WorkerPool: make(chan chan WebhookTask, maxWorkers),
		JobQueue:   make(chan WebhookTask, maxQueue),
		MaxWorkers: maxWorkers,
		Broker:     broker,
	}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool, d.Broker)
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
	Broker     *Broker
	quit       chan bool
}

func NewWorker(workerPool chan chan WebhookTask, broker *Broker) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan WebhookTask),
		Broker:     broker,
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
				
				msg := fmt.Sprintf("Worker dispatched webhook ID: %d", job.ID)
				log.Println(msg)
				
				// Push log to frontend via SSE
				w.Broker.messages <- msg
			case <-w.quit:
				return
			}
		}
	}()
}

// HTTP Handlers
func (d *Dispatcher) HandleDispatch(w http.ResponseWriter, r *http.Request) {
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

	d.Broker.messages <- fmt.Sprintf("Received bulk batch of %d webhooks. Initiating dispatch...", req.Count)

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

func (b *Broker) HandleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	messageChan := make(chan string)
	b.newClients <- messageChan

	defer func() {
		b.defunctClients <- messageChan
	}()

	notify := w.(http.CloseNotifier).CloseNotify()

	for {
		select {
		case msg := <-messageChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			w.(http.Flusher).Flush()
		case <-notify:
			return
		}
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	broker := NewBroker()
	broker.Start()

	// Init dispatcher with 50 workers and a buffer of 10,000 jobs
	dispatcher := NewDispatcher(50, 10000, broker)
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

	// Serve API endpoints
	mux.HandleFunc("/api/v1/dispatch", dispatcher.HandleDispatch)
	mux.HandleFunc("/api/v1/stream", broker.HandleSSE)

	log.Printf("Hermes Webhook Dispatcher started on port %s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
