package restic

import "sync"

type repoLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.RWMutex
}

func newRepoLocker() *repoLocker {
	return &repoLocker{
		locks: make(map[string]*sync.RWMutex),
	}
}

func (rl *repoLocker) Lock(name string, exclusive bool) {
	switch exclusive {
	case true:
		rl.ensureLock(name).Lock()
	case false:
		rl.ensureLock(name).RLock()
	}
}

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
