package workers

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Job represents a unit of work for a worker
type Job interface {
	Process(ctx context.Context) error
	ID() int
}

// WorkerPool manages a pool of workers for concurrent job processing
type WorkerPool struct {
	workerCount      int
	jobs             chan Job
	results          chan error
	wg               sync.WaitGroup
	stopOnce         sync.Once
	logger           *zap.Logger
	activeWorkers    int32
	processedCount   int64
	successCount     int64
	failedCount      int64
	lastProcessedID  int64
	processingTimes  []time.Duration
	processingTimesMu sync.Mutex
}

// Stats holds statistics about the worker pool
type Stats struct {
	ActiveWorkers   int32
	TotalProcessed  int64
	SuccessCount    int64
	FailedCount     int64
	LastProcessedID int64
	AvgProcessingMs int64
	MaxProcessingMs int64
	MinProcessingMs int64
}

// NewWorkerPool creates a new worker pool with the specified number of workers
func NewWorkerPool(workerCount int, jobBufferSize int, logger *zap.Logger) *WorkerPool {
	return &WorkerPool{
		workerCount:     workerCount,
		jobs:            make(chan Job, jobBufferSize),
		results:         make(chan error, jobBufferSize),
		logger:          logger,
		processingTimes: make([]time.Duration, 0, 1000),
	}
}

// Start starts the worker pool
func (p *WorkerPool) Start(ctx context.Context) {
	p.logger.Info("Starting worker pool", zap.Int("worker_count", p.workerCount))

	// Start workers
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		workerID := i
		
		go func() {
			defer p.wg.Done()
			p.runWorker(ctx, workerID)
		}()
	}
}

// runWorker is the main worker routine
func (p *WorkerPool) runWorker(ctx context.Context, workerID int) {
	p.logger.Debug("Worker started", zap.Int("worker_id", workerID))

	for {
		select {
		case <-ctx.Done():
			// Context was canceled, exit worker
			p.logger.Debug("Worker stopping due to context cancellation", zap.Int("worker_id", workerID))
			return
			
		case job, ok := <-p.jobs:
			if !ok {
				// Channel was closed, exit worker
				p.logger.Debug("Worker stopping due to closed job channel", zap.Int("worker_id", workerID))
				return
			}

			// Increment active workers counter
			atomic.AddInt32(&p.activeWorkers, 1)

			// Process the job
			start := time.Now()
			err := job.Process(ctx)
			duration := time.Since(start)

			// Record processing time
			p.processingTimesMu.Lock()
			p.processingTimes = append(p.processingTimes, duration)
			if len(p.processingTimes) > 10000 {
				// Keep only the most recent times to avoid memory growth
				p.processingTimes = p.processingTimes[len(p.processingTimes)-1000:]
			}
			p.processingTimesMu.Unlock()

			// Update statistics
			atomic.AddInt64(&p.processedCount, 1)
			if err == nil {
				atomic.AddInt64(&p.successCount, 1)
			} else {
				atomic.AddInt64(&p.failedCount, 1)
			}
			atomic.StoreInt64(&p.lastProcessedID, int64(job.ID()))

			// Log job completion
			p.logger.Debug("Job processed",
				zap.Int("worker_id", workerID),
				zap.Int("job_id", job.ID()),
				zap.Duration("duration", duration),
				zap.Error(err),
			)

			// Send result to results channel
			select {
			case p.results <- err:
				// Result was sent
			case <-ctx.Done():
				// Context was canceled while sending result
				p.logger.Warn("Context canceled while sending job result",
					zap.Int("worker_id", workerID),
					zap.Int("job_id", job.ID()),
				)
				return
			}

			// Decrement active workers counter
			atomic.AddInt32(&p.activeWorkers, -1)
		}
	}
}

// Submit adds a job to the worker pool
func (p *WorkerPool) Submit(ctx context.Context, job Job) error {
	select {
	case p.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Results returns a channel for receiving job results
func (p *WorkerPool) Results() <-chan error {
	return p.results
}

// Stop stops the worker pool and waits for all workers to finish
func (p *WorkerPool) Stop() {
	p.stopOnce.Do(func() {
		close(p.jobs)
		p.wg.Wait()
		close(p.results)
		p.logger.Info("Worker pool stopped")
	})
}

// GetStats returns statistics about the worker pool
func (p *WorkerPool) GetStats() Stats {
	var avgMs, maxMs, minMs int64

	p.processingTimesMu.Lock()
	if len(p.processingTimes) > 0 {
		var sum time.Duration
		max := p.processingTimes[0]
		min := p.processingTimes[0]

		for _, d := range p.processingTimes {
			sum += d
			if d > max {
				max = d
			}
			if d < min {
				min = d
			}
		}

		avgMs = sum.Milliseconds() / int64(len(p.processingTimes))
		maxMs = max.Milliseconds()
		minMs = min.Milliseconds()
	}
	p.processingTimesMu.Unlock()

	return Stats{
		ActiveWorkers:   atomic.LoadInt32(&p.activeWorkers),
		TotalProcessed:  atomic.LoadInt64(&p.processedCount),
		SuccessCount:    atomic.LoadInt64(&p.successCount),
		FailedCount:     atomic.LoadInt64(&p.failedCount),
		LastProcessedID: atomic.LoadInt64(&p.lastProcessedID),
		AvgProcessingMs: avgMs,
		MaxProcessingMs: maxMs,
		MinProcessingMs: minMs,
	}
}
