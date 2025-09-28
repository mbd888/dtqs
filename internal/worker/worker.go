package worker

import (
	"context"
	"errors"
	"log"
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
	log.Printf("worker %d processing task %s", w.id, t.ID)

	t.Status = task.StatusRunning
	t.UpdatedAt = time.Now()
	err := w.queue.Update(ctx, t)
	if err != nil {
		return
	}

	// simulate work
	time.Sleep(2 * time.Second)

	// for now, always succeed
	t.Status = task.StatusCompleted
	t.UpdatedAt = time.Now()

	if err := w.queue.Update(ctx, t); err != nil {
		log.Printf("worker %d failed to update task: %v", w.id, err)
	}

	log.Printf("worker %d completed task %s", w.id, t.ID)
}
