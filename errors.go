package fsstore

import "errors"

// ErrBucketNotSpecified is return, when objectstore's data directory not specified.
var ErrDataDirNotSpecified = errors.New("objectstore: data directory parameter not specified")
