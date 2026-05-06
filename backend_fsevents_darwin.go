//go:build darwin && cgo

package fsnotify

/*
#cgo LDFLAGS: -framework CoreServices
#include <CoreServices/CoreServices.h>
#include <dispatch/dispatch.h>
#include <stdlib.h>

extern void goFSEventsCallback(
	uintptr_t info,
	size_t numEvents,
	char **paths,
	FSEventStreamEventFlags *flags,
	FSEventStreamEventId *ids);

static void bridgeCallback(
	ConstFSEventStreamRef ref,
	void *info,
	size_t numEvents,
	void *eventPaths,
	const FSEventStreamEventFlags eventFlags[],
	const FSEventStreamEventId eventIds[])
{
	goFSEventsCallback(
		(uintptr_t)info,
		numEvents,
		(char **)eventPaths,
		(FSEventStreamEventFlags *)eventFlags,
		(FSEventStreamEventId *)eventIds);
}

static FSEventStreamRef createStream(
	uintptr_t info,
	CFArrayRef paths,
	FSEventStreamEventId sinceWhen,
	CFTimeInterval latency,
	FSEventStreamCreateFlags flags)
{
	FSEventStreamContext ctx = {0, (void *)info, NULL, NULL, NULL};
	return FSEventStreamCreate(
		NULL,
		bridgeCallback,
		&ctx,
		paths,
		sinceWhen,
		latency,
		flags);
}

static void releaseQueue(dispatch_queue_t q) {
	dispatch_release(q);
}

static void cfArrayAppendCFString(CFMutableArrayRef a, CFStringRef s) {
	CFArrayAppendValue(a, s);
}
*/
import "C"

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	fseRootChanged  = 0x00000020
	fseMustScanSubs = 0x00000001
)

const (
	fseItemCreated      = 0x00000100
	fseItemRemoved      = 0x00000200
	fseItemInodeMetaMod = 0x00000400
	fseItemRenamed      = 0x00000800
	fseItemModified     = 0x00001000
	fseItemChangeOwner  = 0x00004000
	fseItemXattrMod     = 0x00008000
)

const (
	fseCreateNoDefer    = 0x02
	fseCreateWatchRoot  = 0x04
	fseCreateFileEvents = 0x10
)

const fseventsLatency = 0.01

type fsevents struct {
	*shared
	Events chan Event
	Errors chan error

	mu            sync.Mutex
	id            uintptr
	queue         C.dispatch_queue_t
	streams       map[string]*fseventsStream
	cleanupW      sync.WaitGroup
	closeChannels bool
}

type darwinBackend struct {
	kqueue   backend
	fsevents backend
}

type fseventsStream struct {
	stream C.FSEventStreamRef
	path   string
	name   string
	op     Op
	isDir  bool
	recur  bool
	known  map[string]fseventsFile
}

type fseventsFile struct {
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	dev     uint64
	ino     uint64
}

type fseventsReg struct {
	path  string
	name  string
	op    Op
	isDir bool
	recur bool
}

var (
	fseventsRegistryMu sync.Mutex
	fseventsRegistry   = make(map[uintptr]*fsevents)
	fseventsNextID     uintptr
)

func newBackend(ev chan Event, errs chan error) (backend, error) {
	kq, err := newKqueueBackend(ev, errs)
	if err != nil {
		return nil, err
	}
	fse, err := newFSEventsBackend(ev, errs, false)
	if err != nil {
		kq.Close()
		return nil, err
	}
	return &darwinBackend{kqueue: kq, fsevents: fse}, nil
}

func newFSEventsBackend(ev chan Event, errs chan error, closeChannels bool) (backend, error) {
	label := C.CString("github.com/aperturerobotics/fsnotify")
	defer C.free(unsafe.Pointer(label))

	w := &fsevents{
		shared:  newShared(ev, errs),
		Events:  ev,
		Errors:  errs,
		queue:   C.dispatch_queue_create(label, nil),
		streams: make(map[string]*fseventsStream),
	}
	w.closeChannels = closeChannels
	w.id = registerFSEventsWatcher(w)
	return w, nil
}

func (w *darwinBackend) Add(name string) error { return w.AddWith(name) }

func (w *darwinBackend) AddWith(name string, opts ...addOpt) error {
	_, recur := recursivePath(name)
	if recur {
		return w.fsevents.AddWith(name, opts...)
	}
	return w.kqueue.AddWith(name, opts...)
}

func (w *darwinBackend) Remove(name string) error {
	_, recur := recursivePath(name)
	if recur {
		return w.fsevents.Remove(name)
	}
	return w.kqueue.Remove(name)
}

func (w *darwinBackend) WatchList() []string {
	entries := w.kqueue.WatchList()
	entries = append(entries, w.fsevents.WatchList()...)
	return entries
}

func (w *darwinBackend) Close() error {
	fseErr := w.fsevents.Close()
	kqErr := w.kqueue.Close()
	if fseErr != nil {
		return fseErr
	}
	return kqErr
}

func (w *darwinBackend) xSupports(op Op) bool {
	return w.kqueue.xSupports(op) && w.fsevents.xSupports(op)
}

func (w *fsevents) Add(name string) error { return w.AddWith(name) }

func (w *fsevents) AddWith(name string, opts ...addOpt) error {
	if debug {
		fmt.Fprintf(os.Stderr, "FSNOTIFY_DEBUG: %s  AddWith(%q)\n",
			time.Now().Format("15:04:05.000000000"), name)
	}

	with := getOptions(opts...)
	if !w.xSupports(with.op) {
		return fmt.Errorf("%w: %s", xErrUnsupported, with.op)
	}

	name, recur := recursivePath(name)
	name = filepath.Clean(name)
	path, isDir, err := fseventsWatchPath(name)
	if err != nil {
		return err
	}
	known, err := fseventsKnown(name, path, isDir, recur)
	if err != nil {
		return err
	}
	key := fseventsKey(path)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isClosed() {
		return ErrClosed
	}
	if _, exists := w.streams[key]; exists {
		return nil
	}

	stream, err := w.createStreamLocked(path)
	if err != nil {
		return err
	}
	w.streams[key] = &fseventsStream{
		stream: stream,
		path:   path,
		name:   name,
		op:     with.op,
		isDir:  isDir,
		recur:  recur,
		known:  known,
	}
	return nil
}

func (w *fsevents) Remove(name string) error {
	if debug {
		fmt.Fprintf(os.Stderr, "FSNOTIFY_DEBUG: %s  Remove(%q)\n",
			time.Now().Format("15:04:05.000000000"), name)
	}
	if w.isClosed() {
		return nil
	}

	name, _ = recursivePath(name)
	path, _, err := fseventsWatchPath(name)
	if err != nil {
		return err
	}
	key := fseventsKey(path)

	w.mu.Lock()
	fs, ok := w.streams[key]
	if ok {
		delete(w.streams, key)
	}
	w.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrNonExistentWatch, filepath.Clean(name))
	}

	stopFSEventsStream(fs.stream)
	return nil
}

func (w *fsevents) WatchList() []string {
	if w.isClosed() {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	entries := make([]string, 0, len(w.streams))
	for _, fs := range w.streams {
		entries = append(entries, fs.name)
	}
	return entries
}

func (w *fsevents) Close() error {
	if w.shared.close() {
		return nil
	}

	w.mu.Lock()
	streams := make([]C.FSEventStreamRef, 0, len(w.streams))
	for _, fs := range w.streams {
		streams = append(streams, fs.stream)
	}
	w.streams = nil
	queue := w.queue
	id := w.id
	w.mu.Unlock()

	for _, stream := range streams {
		stopFSEventsStream(stream)
	}
	w.cleanupW.Wait()
	if w.closeChannels {
		close(w.Events)
		close(w.Errors)
	}
	C.releaseQueue(queue)
	unregisterFSEventsWatcher(id)
	return nil
}

func (w *fsevents) createStreamLocked(path string) (C.FSEventStreamRef, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	cfpath := C.CFStringCreateWithCString(0, cpath, C.kCFStringEncodingUTF8)
	if unsafe.Pointer(cfpath) == nil {
		return nil, fmt.Errorf("FSEventStreamCreate: failed to create path string")
	}
	defer C.CFRelease(C.CFTypeRef(cfpath))

	paths := C.CFArrayCreateMutable(0, 1, &C.kCFTypeArrayCallBacks)
	if unsafe.Pointer(paths) == nil {
		return nil, fmt.Errorf("FSEventStreamCreate: failed to create path array")
	}
	defer C.CFRelease(C.CFTypeRef(paths))
	C.cfArrayAppendCFString(paths, cfpath)

	flags := C.FSEventStreamCreateFlags(fseCreateFileEvents | fseCreateNoDefer | fseCreateWatchRoot)
	stream := C.createStream(
		C.uintptr_t(w.id),
		C.CFArrayRef(paths),
		C.FSEventStreamEventId(C.kFSEventStreamEventIdSinceNow),
		C.CFTimeInterval(fseventsLatency),
		flags,
	)
	if stream == nil {
		return nil, fmt.Errorf("FSEventStreamCreate failed")
	}

	C.FSEventStreamSetDispatchQueue(stream, w.queue)
	if C.FSEventStreamStart(stream) == 0 {
		C.FSEventStreamInvalidate(stream)
		C.FSEventStreamRelease(stream)
		return nil, fmt.Errorf("FSEventStreamStart failed")
	}
	return stream, nil
}

func (w *fsevents) xSupports(op Op) bool {
	if op.Has(xUnportableOpen) || op.Has(xUnportableRead) ||
		op.Has(xUnportableCloseWrite) || op.Has(xUnportableCloseRead) {
		return false
	}
	return true
}

func (w *fsevents) snapshot() []fseventsReg {
	w.mu.Lock()
	defer w.mu.Unlock()
	regs := make([]fseventsReg, 0, len(w.streams))
	for _, fs := range w.streams {
		regs = append(regs, fseventsReg{
			path:  fs.path,
			name:  fs.name,
			op:    fs.op,
			isDir: fs.isDir,
			recur: fs.recur,
		})
	}
	return regs
}

func (w *fsevents) removeRootChanged(reg fseventsReg) {
	w.mu.Lock()
	fs, ok := w.streams[fseventsKey(reg.path)]
	if ok {
		delete(w.streams, fseventsKey(reg.path))
		w.cleanupW.Add(1)
	}
	w.mu.Unlock()
	if !ok {
		return
	}

	go func() {
		defer w.cleanupW.Done()
		stopFSEventsStream(fs.stream)
	}()
}

func stopFSEventsStream(stream C.FSEventStreamRef) {
	C.FSEventStreamStop(stream)
	C.FSEventStreamInvalidate(stream)
	C.FSEventStreamRelease(stream)
}

func registerFSEventsWatcher(w *fsevents) uintptr {
	fseventsRegistryMu.Lock()
	defer fseventsRegistryMu.Unlock()
	fseventsNextID++
	fseventsRegistry[fseventsNextID] = w
	return fseventsNextID
}

func unregisterFSEventsWatcher(id uintptr) {
	fseventsRegistryMu.Lock()
	defer fseventsRegistryMu.Unlock()
	delete(fseventsRegistry, id)
}

func lookupFSEventsWatcher(id uintptr) *fsevents {
	fseventsRegistryMu.Lock()
	defer fseventsRegistryMu.Unlock()
	return fseventsRegistry[id]
}

func fseventsWatchPath(name string) (string, bool, error) {
	name = filepath.Clean(name)
	fi, err := os.Lstat(name)
	if err != nil {
		return "", false, err
	}
	path := name
	path, err = filepath.EvalSymlinks(name)
	if err != nil {
		return "", false, err
	}
	fi, err = os.Lstat(path)
	if err != nil {
		return "", false, err
	}
	return filepath.Clean(path), fi.IsDir(), nil
}

func fseventsKnown(name, path string, isDir, recur bool) (map[string]fseventsFile, error) {
	known := make(map[string]fseventsFile)
	if st, ok := fseventsFileState(path); ok {
		known[path] = st
	}
	if !isDir {
		return known, nil
	}
	if recur {
		return known, nil
	}

	files, err := os.ReadDir(name)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		child := filepath.Join(name, f.Name())
		p, _, err := fseventsWatchPath(child)
		if err != nil {
			if st, ok := fseventsFileState(filepath.Join(path, f.Name())); ok {
				known[filepath.Join(path, f.Name())] = st
			}
			continue
		}
		if st, ok := fseventsFileState(p); ok {
			known[p] = st
		}
	}
	return known, nil
}

func fseventsFileState(path string) (fseventsFile, bool) {
	fi, err := os.Lstat(path)
	if err != nil {
		return fseventsFile{}, false
	}
	st := fseventsFile{
		size:    fi.Size(),
		mode:    fi.Mode(),
		modTime: fi.ModTime(),
		isDir:   fi.IsDir(),
	}
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		st.dev = uint64(stat.Dev)
		st.ino = uint64(stat.Ino)
	}
	return st, true
}

func fseventsFindRenamed(path string, prev fseventsFile) bool {
	if prev.dev == 0 && prev.ino == 0 {
		return false
	}
	files, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		return false
	}
	for _, f := range files {
		child := filepath.Join(filepath.Dir(path), f.Name())
		if child == path {
			continue
		}
		st, ok := fseventsFileState(child)
		if ok && st.dev == prev.dev && st.ino == prev.ino {
			return true
		}
	}
	return false
}

func matchFSEventsRegistration(path string, regs []fseventsReg) (fseventsReg, bool) {
	key := fseventsKey(path)
	best := -1
	for i, reg := range regs {
		regKey := fseventsKey(reg.path)
		if key != regKey && !hasPathPrefix(key, regKey) {
			continue
		}
		if best == -1 || len(regKey) > len(fseventsKey(regs[best].path)) {
			best = i
		}
	}
	if best == -1 {
		return fseventsReg{}, false
	}
	return regs[best], true
}

func fseventsKey(path string) string {
	return filepath.Clean(path)
}

func fseventsEventName(path string, reg fseventsReg) string {
	if reg.path == reg.name {
		return path
	}
	if path == reg.path {
		return reg.name
	}
	rel, err := filepath.Rel(reg.path, path)
	if err != nil {
		return path
	}
	return filepath.Join(reg.name, rel)
}

func fseventsOp(flags uint32) Op {
	var op Op
	if flags&fseItemCreated != 0 {
		op |= Create
	}
	if flags&fseItemRemoved != 0 {
		op |= Remove
	}
	if flags&fseItemRenamed != 0 {
		op |= Rename
	}
	if flags&fseItemModified != 0 {
		op |= Write
	}
	if flags&(fseItemInodeMetaMod|fseItemChangeOwner|fseItemXattrMod) != 0 {
		op |= Chmod
	}
	return op
}

func fseventsOps(flags uint32, mask Op) []Op {
	op := fseventsOp(flags) & mask
	if op == 0 {
		return nil
	}

	if op.Has(Remove) {
		return []Op{Remove}
	}
	if op.Has(Rename) {
		return []Op{Rename}
	}
	if op.Has(Create) {
		return []Op{Create}
	}

	ops := make([]Op, 0, 2)
	if op.Has(Write) {
		ops = append(ops, Write)
	}
	if op.Has(Chmod) {
		ops = append(ops, Chmod)
	}
	return ops
}

func (w *fsevents) classifyFSEvents(path string, flags uint32, reg fseventsReg) []Op {
	op := fseventsOp(flags) & reg.op
	if op == 0 {
		return nil
	}

	w.mu.Lock()
	fs, ok := w.streams[fseventsKey(reg.path)]
	if !ok {
		w.mu.Unlock()
		return nil
	}
	prev, known := fs.known[path]
	next, exists := fseventsFileState(path)

	if op.Has(Remove) {
		delete(fs.known, path)
		w.mu.Unlock()
		if known {
			return []Op{Remove}
		}
		return nil
	}

	if op.Has(Rename) {
		switch {
		case known && exists:
			fs.known[path] = next
			w.mu.Unlock()
			return []Op{Remove, Create}
		case known:
			delete(fs.known, path)
			w.mu.Unlock()
			return []Op{Rename}
		case exists:
			fs.known[path] = next
			w.mu.Unlock()
			return []Op{Create}
		default:
			w.mu.Unlock()
			return nil
		}
	}

	if !known && exists {
		fs.known[path] = next
		w.mu.Unlock()
		ops := []Op{Create}
		if op.Has(Write) {
			ops = append(ops, Write)
		}
		return ops
	}
	if known && exists {
		fs.known[path] = next
	}
	w.mu.Unlock()

	if op.Has(Create) {
		ops := make([]Op, 0, 2)
		if op.Has(Write) && (next.size != prev.size || !next.modTime.Equal(prev.modTime)) {
			ops = append(ops, Write)
		}
		if op.Has(Chmod) && next.mode != prev.mode {
			ops = append(ops, Chmod)
		}
		return ops
	}

	return fseventsOps(flags, reg.op)
}

func (w *fsevents) sendFSEvents(name string, ops []Op) bool {
	for _, op := range ops {
		if !w.sendEvent(Event{Name: name, Op: op}) {
			return false
		}
	}
	return true
}

func (w *fsevents) rootChangedOp(reg fseventsReg) Op {
	w.mu.Lock()
	defer w.mu.Unlock()
	fs, ok := w.streams[fseventsKey(reg.path)]
	if !ok {
		return 0
	}
	prev, known := fs.known[reg.path]
	delete(fs.known, reg.path)
	if known && !prev.isDir && fseventsFindRenamed(reg.path, prev) && reg.op.Has(Rename) {
		return Rename
	}
	if reg.op.Has(Remove) {
		return Remove
	}
	return 0
}

//export goFSEventsCallback
func goFSEventsCallback(
	info C.uintptr_t,
	numEvents C.size_t,
	cpaths **C.char,
	cflags *C.FSEventStreamEventFlags,
	cids *C.FSEventStreamEventId,
) {
	w := lookupFSEventsWatcher(uintptr(info))
	if w == nil || w.isClosed() {
		return
	}

	count := int(numEvents)
	paths := unsafe.Slice(cpaths, count)
	flags := unsafe.Slice(cflags, count)
	_ = unsafe.Slice(cids, count)
	regs := w.snapshot()

	for i := 0; i < count; i++ {
		path := filepath.Clean(C.GoString(paths[i]))
		flag := uint32(flags[i])
		if debug {
			fmt.Fprintf(os.Stderr, "FSNOTIFY_DEBUG: %s  FSEVENT %#x %q\n",
				time.Now().Format("15:04:05.000000000"), flag, path)
		}

		if flag&fseMustScanSubs != 0 {
			w.sendError(fmt.Errorf("fsnotify: events may have been dropped for %s", path))
		}

		reg, ok := matchFSEventsRegistration(path, regs)
		if !ok {
			continue
		}

		if flag&fseRootChanged != 0 {
			if fseventsOp(flag)&reg.op == 0 {
				op := w.rootChangedOp(reg)
				w.removeRootChanged(reg)
				if op != 0 {
					w.sendEvent(Event{Name: reg.name, Op: op})
				}
				continue
			}
			w.removeRootChanged(reg)
			w.sendFSEvents(reg.name, w.classifyFSEvents(path, flag, reg))
			continue
		}

		if path == reg.path && reg.isDir {
			continue
		}
		if !reg.recur && reg.isDir {
			rel, err := filepath.Rel(reg.path, path)
			if err != nil || rel == "." || slices.Contains(strings.Split(rel, string(filepath.Separator)), "..") ||
				strings.ContainsRune(rel, filepath.Separator) {
				continue
			}
		}
		if !reg.isDir && path != reg.path {
			continue
		}

		if !w.sendFSEvents(fseventsEventName(path, reg), w.classifyFSEvents(path, flag, reg)) {
			return
		}
	}
}

// _ is a type assertion
var _ backend = ((*darwinBackend)(nil))

// _ is a type assertion
var _ backend = ((*fsevents)(nil))
