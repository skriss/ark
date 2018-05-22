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

func (rl *repoLocker) RLock(name string) {
	rl.ensureLock(name).RLock()
}

func (rl *repoLocker) RUnlock(name string) {
	rl.ensureLock(name).RUnlock()
}

func (rl *repoLocker) Lock(name string) {
	rl.ensureLock(name).Lock()
}

func (rl *repoLocker) Unlock(name string) {
	rl.ensureLock(name).Unlock()
}

func (rl *repoLocker) ensureLock(name string) *sync.RWMutex {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if _, ok := rl.locks[name]; !ok {
		rl.locks[name] = new(sync.RWMutex)
	}

	return rl.locks[name]
}
