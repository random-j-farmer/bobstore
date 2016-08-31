package bobstore

import (
	"fmt"
	"os"
	"sync"
	"syscall"
)

// fcntl locks seem to allow locking the same file multiple times
// from the same process.  flock did not but it hangs instead
var (
	rwl      sync.Mutex
	rwlNames = make(map[string]bool)
)

// LockFile exclusively locks the file, also in process
func lockFile(name string, f *os.File) error {
	rwl.Lock()
	defer rwl.Unlock()

	if rwlNames[name] {
		return fmt.Errorf("already locked: %s", name)
	}

	rwlNames[name] = true

	fcntlLock := &syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: int16(os.SEEK_SET),
		Start:  0, // file start
		Len:    0, // until end of file
	}

	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, fcntlLock)
}

// UnlockFile - unlock the file
func unlockFile(name string, f *os.File) error {
	rwl.Lock()
	defer rwl.Unlock()

	delete(rwlNames, name)

	fcntlLock := &syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: int16(os.SEEK_SET),
		Start:  0, // file start
		Len:    0, // until end of file
	}

	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, fcntlLock)
}
