package util

import (
	"fmt"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

func Die(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	log.Error(formatted)
	os.Exit(1)
}

func Chk(err error, msg string, args ...interface{}) {
	if err != nil {
		Die(msg, args...)
	}
}

// loopState is a helper struct holding common state for managing consumer loops.
type loopState struct {
	mu    sync.RWMutex
	loop  bool
	fused bool

	lastPass bool
}

// NewLoopState creates a state object for managing a consumer loops.
func NewLoopState(loop bool) *loopState {
	return &loopState{loop: loop}
}

// RequestLastPass requests for the loop to run one more time before exiting.
func (ls *loopState) RequestLastPass() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.loop = false
}

// Next returns true if current loop iteration should run.
func (ls *loopState) Next() bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if ls.fused {
		panic("invariant: Next must not be called after it returned false previously")
	}

	if ls.lastPass {
		ls.fused = true
		return false
	} else if !ls.loop {
		log.Info("This is the last pass.")
		ls.lastPass = true
	}

	return true
}
