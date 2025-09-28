package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"dtqs/internal/queue"
	"dtqs/internal/task"
)

type Worker struct {
	id    int
	queue queue.Queue
}

func NewWorker(id int, q queue.Queue) *Worker {
	return &Worker{
		id:    id,
		queue: q,
	}
}

func (w *Worker) Start(ctx context.Context) {
	log.Printf("worker %d started", w.id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d stopping", w.id)
			return
		default:
		}

		// try to get a task
		t, err := w.queue.Dequeue(ctx)
		if err != nil {
			if errors.Is(err, queue.ErrQueueEmpty) {
				// no tasks, wait a bit
				time.Sleep(1 * time.Second)
				continue
			}
			log.Printf("worker %d error: %v", w.id, err)
			continue
		}

		// process the task
		w.processTask(ctx, t)
	}
}

func (w *Worker) processTask(ctx context.Context, t *task.Task) {
	log.Printf("worker %d processing task %s (priority: %d)", w.id, t.ID, t.Priority)

	t.Status = task.StatusRunning
	t.UpdatedAt = time.Now()
	err := w.queue.Update(ctx, t)
	if err != nil {
		log.Printf("worker %d failed to update task status: %v", w.id, err)
		return
	}

	// Simulate different processing times based on priority
	processingTime := w.getProcessingTime(t.Priority)

	// Simulate work with potential failure
	if err := w.simulateWork(ctx, t, processingTime); err != nil {
		log.Printf("worker %d task %s failed: %v", w.id, t.ID, err)
		t.Status = task.StatusFailed
	} else {
		log.Printf("worker %d completed task %s", w.id, t.ID)
		t.Status = task.StatusCompleted
	}

	t.UpdatedAt = time.Now()
	if err := w.queue.Update(ctx, t); err != nil {
		log.Printf("worker %d failed to update task: %v", w.id, err)
	}
}

func (w *Worker) getProcessingTime(priority task.Priority) time.Duration {
	// Higher priority tasks get processed faster
	switch priority {
	case task.PriorityCritical:
		return 500 * time.Millisecond
	case task.PriorityHigh:
		return 1 * time.Second
	case task.PriorityNormal:
		return 2 * time.Second
	case task.PriorityLow:
		return 5 * time.Second
	default:
		return 2 * time.Second
	}
}

func (w *Worker) simulateWork(ctx context.Context, t *task.Task, duration time.Duration) error {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Simulate different task types
	switch t.Type {
	case "email":
		return w.processEmailTask(t, duration)
	case "image_processing":
		return w.processImageTask(t, duration)
	case "data_export":
		return w.processDataExportTask(t, duration)
	default:
		// Generic processing
		time.Sleep(duration)

		// Simulate 10% failure rate for demonstration
		if rand.Float32() < 0.1 {
			return fmt.Errorf("random processing failure")
		}

		return nil
	}
}

func (w *Worker) processEmailTask(t *task.Task, duration time.Duration) error {
	log.Printf("worker %d: sending email", w.id)
	time.Sleep(duration)

	// Check if email address exists in payload
	if email, ok := t.Payload["to"].(string); !ok || email == "" {
		return fmt.Errorf("missing or invalid email address")
	}

	return nil
}

func (w *Worker) processImageTask(t *task.Task, duration time.Duration) error {
	log.Printf("worker %d: processing image", w.id)
	time.Sleep(duration)

	// Check if image URL exists
	if imageURL, ok := t.Payload["image_url"].(string); !ok || imageURL == "" {
		return fmt.Errorf("missing or invalid image URL")
	}

	return nil
}

func (w *Worker) processDataExportTask(t *task.Task, duration time.Duration) error {
	log.Printf("worker %d: exporting data", w.id)
	time.Sleep(duration)

	// Check if the export format is specified
	if format, ok := t.Payload["format"].(string); !ok || format == "" {
		return fmt.Errorf("missing export format")
	}

	return nil
}
