package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"dtqs/internal/queue"
	"dtqs/internal/worker"
)

func main() {
	ctx := context.Background()

	// Redis URL from env or default
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	q, err := queue.NewRedisQueue(ctx, redisURL)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer func(q *queue.RedisQueue) {
		err := q.Close()
		if err != nil {

		}
	}(q)

	// Get worker count from env or default to 5
	workerCount := 5
	if count := os.Getenv("WORKER_COUNT"); count != "" {
		if c, err := strconv.Atoi(count); err == nil && c > 0 {
			workerCount = c
		}
	}

	// Create and start worker pool
	pool := worker.NewPool(workerCount, q)

	// Start all workers
	ctx, cancel := context.WithCancel(ctx)
	pool.Start(ctx)

	log.Printf("Started %d workers", workerCount)

	// Wait for interrupt signal (Ctrl+C)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down workers...")
	cancel()

	// Wait for all workers to finish
	pool.Wait()
	log.Println("All workers stopped")
}
