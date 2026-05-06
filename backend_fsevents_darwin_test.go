//go:build darwin && cgo

package fsnotify

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFSEventsRecursiveWatch(t *testing.T) {
	tmp := t.TempDir()
	w := newWatcher(t)
	defer w.Close()

	addWatch(t, w, tmp, "...")
	mkdirAll(t, tmp, "a/b")
	touch(t, tmp, "a/b/file")

	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-w.Events:
			if ev.Has(Create) && filepath.Clean(ev.Name) == filepath.Join(tmp, "a/b/file") {
				return
			}
		case err := <-w.Errors:
			t.Fatal(err)
		case <-deadline:
			t.Fatal("timed out waiting for recursive create event")
		}
	}
}
