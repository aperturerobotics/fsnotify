//go:build freebsd || openbsd || netbsd || dragonfly || (darwin && !cgo)

package fsnotify

func newBackend(ev chan Event, errs chan error) (backend, error) {
	return newKqueueBackend(ev, errs)
}
