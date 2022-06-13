package fsstore

import (
	"errors"
	"strings"

	"github.com/igumus/go-objectstore-lib"
)

// ErrDataDirNotSpecified is return, when file system objectstore's data directory not specified.
var ErrDataDirNotSpecified = errors.New("fsobjectstore: data directory parameter not specified")

// _defDebug handles the default for debug mode
const _defDebug = false

// _defBucket handles the default bucket name
const _defBucket = "store"

// _defDataDir handles the default data directory
const _defDataDir = "/data"

// Captures/Represents file system based objectstore configuration information
type fsObjectStoreConfig struct {
	dir    string
	bucket string
	debug  bool
}

// validate - returns error if constructed configuration not valid, otherwise returns nil
func (f *fsObjectStoreConfig) validate() error {
	if len(f.dir) == 0 {
		return ErrDataDirNotSpecified
	}
	if len(f.bucket) == 0 {
		return objectstore.ErrBucketNotSpecified
	}
	return nil
}

// defaultFSObjectstoreConfig returns instance of `fsObjectStoreConfig` which reads initial values
// from environmental variables. If specified environment variables not set, replaces
// with its default  values
func defaultFSObjectstoreConfig() *fsObjectStoreConfig {
	return &fsObjectStoreConfig{
		dir:    _defDataDir,
		bucket: _defBucket,
		debug:  _defDebug,
	}
}

// A FSObjectstoreConfigOption sets options such as chunk directory and block directory.
type FSObjectstoreConfigOption func(*fsObjectStoreConfig)

// WithBucket returns a FSObjectstoreConfigOption that specifies where to store objects.
// If not set, the default is `store`
func WithBucket(cd string) FSObjectstoreConfigOption {
	return func(fosc *fsObjectStoreConfig) {
		fosc.bucket = strings.TrimSpace(cd)
	}
}

// WithDataDir returns a FSObjectstoreConfigOption that specifies where to store objects.
// If not set, the default is `/data`
func WithDataDir(d string) FSObjectstoreConfigOption {
	return func(fosc *fsObjectStoreConfig) {
		fosc.dir = strings.TrimSpace(d)
	}
}

// WithDebugMode returns a FSObjectstoreConfigOption that specifies debug mode.
// If not set, the default is `false`
func WithDebugMode(dm bool) FSObjectstoreConfigOption {
	return func(fosc *fsObjectStoreConfig) {
		fosc.debug = dm
	}
}
