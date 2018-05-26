package restic

import "sync"

// repoLocker manages exclusive/non-exclusive locks for
// operations against restic repositories. The semantics
// of exclusive/non-exclusive locks are the same as for
// a sync.RWMutex, where a non-exclusive lock is equivalent
// to a read lock, and an exclusive lock is equivalent to
// a write lock.
type repoLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.RWMutex
}

func newRepoLocker() *repoLocker {
	return &repoLocker{
		locks: make(map[string]*sync.RWMutex),
	}
}

// Lock acquires a lock for the specified repository. If
// exclusive is true, this function blocks until no other
// locks exist for the repo. If exclusive is false, this
// function blocks until no exclusive locks exist for this
// repository.
func (rl *repoLocker) Lock(name string, exclusive bool) {
	switch exclusive {
	case true:
		rl.ensureLock(name).Lock()
	case false:
		rl.ensureLock(name).RLock()
	}
}

// Unlock releases a lock for the specified repository.
func (rl *repoLocker) Unlock(name string, exclusive bool) {
	switch exclusive {
	case true:
		rl.ensureLock(name).Unlock()
	case false:
		rl.ensureLock(name).RUnlock()
	}
}

func (rl *repoLocker) ensureLock(name string) *sync.RWMutex {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if _, ok := rl.locks[name]; !ok {
		rl.locks[name] = new(sync.RWMutex)
	}

	return rl.locks[name]
}
