module github.com/aperturerobotics/fsnotify

go 1.19

require golang.org/x/sys v0.13.0

retract (
	v1.5.3 // Published an incorrect branch accidentally https://github.com/aperturerobotics/fsnotify/issues/445
	v1.5.0 // Contains symlink regression https://github.com/aperturerobotics/fsnotify/pull/394
)
