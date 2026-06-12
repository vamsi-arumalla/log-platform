package backpressure

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestAcceptsUnderThreshold(t *testing.T) {
	c := NewController(100, time.Second, zap.NewNop())
	c.IncrementDepth(50)
	if !c.ShouldAccept() {
		t.Error("should accept under threshold")
	}
	if c.IsActive() {
		t.Error("backpressure should not be active")
	}
}

func TestRejectsOverThreshold(t *testing.T) {
	c := NewController(100, time.Second, zap.NewNop())
	c.IncrementDepth(150)
	if c.ShouldAccept() {
		t.Error("should reject over threshold")
	}
	if !c.IsActive() {
		t.Error("backpressure should be active")
	}
}

func TestReleasesAfterCooldown(t *testing.T) {
	c := NewController(100, 50*time.Millisecond, zap.NewNop())
	c.IncrementDepth(150)
	c.ShouldAccept() // engage

	c.DecrementDepth(120) // depth now 30, below half threshold
	time.Sleep(80 * time.Millisecond)

	if !c.ShouldAccept() {
		t.Error("should accept after drain")
	}
	if c.IsActive() {
		t.Error("backpressure should be released after cooldown")
	}
}

func TestStaysActiveDuringCooldown(t *testing.T) {
	c := NewController(100, 10*time.Second, zap.NewNop())
	c.IncrementDepth(150)
	c.ShouldAccept() // engage

	c.DecrementDepth(120)
	c.ShouldAccept()

	if !c.IsActive() {
		t.Error("backpressure should remain flagged active during cooldown window")
	}
}
