package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"dtqs/internal/task"

	"github.com/redis/go-redis/v9"
)

const (
	queueKey   = "taskflow:queue:pending"
	taskPrefix = "taskflow:task:"
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(ctx context.Context, redisURL string) (*RedisQueue, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %w", err)
	}

	client := redis.NewClient(opts)

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &RedisQueue{client: client}, nil
}

func (q *RedisQueue) Enqueue(ctx context.Context, t *task.Task) error {
	data, err := t.Marshal()
	if err != nil {
		return err
	}

	err = q.client.Set(ctx, taskPrefix+t.ID, data, 24*time.Hour).Err()
	if err != nil {
		return err
	}

	// Lower score = higher priority (will be dequeued first)
	// Score = timestamp - (priority * 1000000)
	score := float64(time.Now().Unix()) - float64(t.Priority)*1000000

	return q.client.ZAdd(ctx, queueKey, redis.Z{
		Score:  score,
		Member: t.ID,
	}).Err()
}

func (q *RedisQueue) Dequeue(ctx context.Context) (*task.Task, error) {
	// Pop from a sorted set (the lowest score = highest priority)
	result := q.client.ZPopMin(ctx, queueKey, 1)
	if err := result.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrQueueEmpty
		}
		return nil, err
	}

	items := result.Val()
	if len(items) == 0 {
		return nil, ErrQueueEmpty
	}

	taskID := items[0].Member

	// Get task data
	data, err := q.client.Get(ctx, taskPrefix+taskID).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Task data missing shouldn't happen but handle gracefully
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	return task.Unmarshal(data)
}

func (q *RedisQueue) Get(ctx context.Context, taskID string) (*task.Task, error) {
	data, err := q.client.Get(ctx, taskPrefix+taskID).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	return task.Unmarshal(data)
}

func (q *RedisQueue) Update(ctx context.Context, t *task.Task) error {
	data, err := t.Marshal()
	if err != nil {
		return err
	}

	t.UpdatedAt = time.Now()
	return q.client.Set(ctx, taskPrefix+t.ID, data, 24*time.Hour).Err()
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}
