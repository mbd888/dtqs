package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"dtqs/internal/queue"
	"dtqs/internal/task"
)

var q queue.Queue

func main() {
	ctx := context.Background()

	// Redis URL from env or default
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	var err error
	q, err = queue.NewRedisQueue(ctx, redisURL)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	// Handlers
	http.HandleFunc("/", rootHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/tasks", loggingMiddleware(tasksHandler))
	http.HandleFunc("/tasks/", loggingMiddleware(taskHandler))

	// Port from env or default
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server starting on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// Root handler for "/"
func rootHandler(w http.ResponseWriter, _ *http.Request) {
	_, err := w.Write([]byte("DTQS API is running"))
	if err != nil {
		return
	}
}

// Health check endpoint
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	if err != nil {
		return
	}
}

// Task endpoint with method routing
func tasksHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		createTask(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Get a single task by ID
func taskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract ID from path /tasks/{id}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 3 || parts[2] == "" {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	taskID := parts[2]

	// Get task from queue
	t, err := q.Get(r.Context(), taskID)
	if err != nil {
		if errors.Is(err, queue.ErrTaskNotFound) {
			http.Error(w, "Task not found", http.StatusNotFound)
		} else {
			log.Printf("Error getting task %s: %v", taskID, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(t)
	if err != nil {
		return
	}
}

// Create a task handler with priority support
func createTask(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type     string                 `json:"type"`
		Payload  map[string]interface{} `json:"payload"`
		Priority int                    `json:"priority,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate type is provided
	if req.Type == "" {
		http.Error(w, "Task type is required", http.StatusBadRequest)
		return
	}

	// Validate task type
	validTypes := map[string]bool{
		"email":            true,
		"image_processing": true,
		"data_export":      true,
		"notification":     true,
		"cleanup":          true,
	}

	if !validTypes[req.Type] {
		http.Error(w, fmt.Sprintf("Invalid task type. Valid types: %v", getValidTypes(validTypes)), http.StatusBadRequest)
		return
	}

	t := task.New(req.Type, req.Payload)

	// Set priority with validation (0-3 range)
	switch req.Priority {
	case 0:
		t.Priority = task.PriorityLow
	case 1:
		t.Priority = task.PriorityNormal
	case 2:
		t.Priority = task.PriorityHigh
	case 3:
		t.Priority = task.PriorityCritical
	default:
		if req.Priority != 0 { // 0 is default, so don't error for omitted priority
			http.Error(w, "Priority must be between 0 (low) and 3 (critical)", http.StatusBadRequest)
			return
		}
		t.Priority = task.PriorityNormal
	}

	// Validate payload based on the task type
	if err := validateTaskPayload(req.Type, req.Payload); err != nil {
		http.Error(w, fmt.Sprintf("Invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	if err := q.Enqueue(r.Context(), t); err != nil {
		log.Printf("Failed to enqueue task: %v", err)
		http.Error(w, "Failed to enqueue task", http.StatusInternalServerError)
		return
	}

	// Return comprehensive response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	err := json.NewEncoder(w).Encode(map[string]interface{}{
		"id":         t.ID,
		"type":       t.Type,
		"status":     string(t.Status),
		"priority":   t.Priority,
		"created_at": t.CreatedAt,
	})
	if err != nil {
		log.Printf("Failed to encode response: %v", err)
		return
	}
}

// Simple logging middleware
func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next(w, r)
	}
}

func getValidTypes(validTypes map[string]bool) []string {
	types := make([]string, 0, len(validTypes))
	for t := range validTypes {
		types = append(types, t)
	}
	return types
}

func validateTaskPayload(taskType string, payload map[string]interface{}) error {
	switch taskType {
	case "email":
		if to, ok := payload["to"].(string); !ok || to == "" {
			return errors.New("email tasks require 'to' field with valid email address")
		}
		if subject, ok := payload["subject"].(string); !ok || subject == "" {
			return errors.New("email tasks require 'subject' field")
		}
	case "image_processing":
		if imageURL, ok := payload["image_url"].(string); !ok || imageURL == "" {
			return errors.New("image_processing tasks require 'image_url' field")
		}
	case "data_export":
		format, ok := payload["format"].(string)
		if !ok || format == "" {
			return errors.New("data_export tasks require 'format' field")
		}
		validFormats := map[string]bool{"csv": true, "json": true, "xlsx": true}
		if !validFormats[format] {
			return errors.New("data_export format must be one of: csv, json, xlsx")
		}
	}
	return nil
}
