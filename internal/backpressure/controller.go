package backpressure

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vamsi-arumalla/log-platform/internal/metrics"
	"go.uber.org/zap"
)

type Controller struct {
	maxQueueDepth int64
	currentDepth  atomic.Int64
	active        atomic.Bool
	cooldown      time.Duration
	lastTriggered atomic.Int64
	mu            sync.RWMutex
	logger        *zap.Logger
}

func NewController(maxQueueDepth int64, cooldown time.Duration, logger *zap.Logger) *Controller {
	return &Controller{
		maxQueueDepth: maxQueueDepth,
		cooldown:      cooldown,
		logger:        logger,
	}
}

func (c *Controller) ShouldAccept() bool {
	depth := c.currentDepth.Load()
	threshold := c.maxQueueDepth

	if depth > threshold {
		if !c.active.Load() {
			c.active.Store(true)
			c.lastTriggered.Store(time.Now().UnixNano())
			metrics.BackpressureActive.Set(1)
			c.logger.Warn("backpressure engaged",
				zap.Int64("queue_depth", depth),
				zap.Int64("threshold", threshold),
			)
		}
		metrics.BackpressureDropped.Inc()
		return false
	}

	if c.active.Load() && depth < threshold/2 {
		last := time.Unix(0, c.lastTriggered.Load())
		if time.Since(last) > c.cooldown {
			c.active.Store(false)
			metrics.BackpressureActive.Set(0)
			c.logger.Info("backpressure released",
				zap.Int64("queue_depth", depth),
			)
		}
	}

	return true
}

func (c *Controller) IncrementDepth(n int64) {
	c.currentDepth.Add(n)
}

func (c *Controller) DecrementDepth(n int64) {
	c.currentDepth.Add(-n)
}

func (c *Controller) IsActive() bool {
	return c.active.Load()
}

func (c *Controller) CurrentDepth() int64 {
	return c.currentDepth.Load()
}
