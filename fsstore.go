package fsstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/igumus/go-objectstore-lib"
	"github.com/ipfs/go-cid"
)

// ErrDataDigestionFailed is return, when file system objectstore's object digestion failed.
var ErrDataDigestionFailed = errors.New("fsobjectstore: object digestion failed")

// Captures/Represents filesystem backed objectstore service information
type fsObjectStoreService struct {
	debug   bool
	dataDir string
	bucket  string
}

// NewFileSystemObjectStore creates file system backed ObjectStore instance via given configuration options.
// Error returns when creating ObjectStore instance failed.
func NewFileSystemObjectStore(opts ...FSObjectstoreConfigOption) (objectstore.ObjectStore, error) {
	cfg := defaultFSObjectstoreConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	srv := &fsObjectStoreService{
		debug:   cfg.debug,
		dataDir: cfg.dir,
		bucket:  cfg.bucket,
	}

	dir := fmt.Sprintf("%s/%s", srv.dataDir, srv.bucket)
	if !exists(dir) {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, err
		}
	}

	return srv, nil
}

// path - returns file system path of given object link
func (f *fsObjectStoreService) path(objLink string) string {
	return fmt.Sprintf("%s/%s/%s", f.dataDir, f.bucket, objLink)
}

// HasObject - checks whether object exists on file system with specified cid (aka content identifier)
func (f *fsObjectStoreService) HasObject(ctx context.Context, cid cid.Cid) bool {
	objLink := f.path(objectstore.DefaultLinkFunc(cid.String()))
	ret := exists(objLink)
	if f.debug {
		log.Printf("debug: has object: %s, %t\n", objLink, ret)
	}
	return ret
}

// ReadObject - reads object on file system with specified cid (aka content identifier)
func (f *fsObjectStoreService) ReadObject(ctx context.Context, cid cid.Cid) ([]byte, error) {
	if !f.HasObject(ctx, cid) {
		return nil, objectstore.ErrObjectNotExists
	}
	objLink := f.path(objectstore.DefaultLinkFunc(cid.String()))
	if f.debug {
		log.Printf("debug: check object existence: %s\n", objLink)
	}
	if ctxErr := checkContextError(ctx, f.debug); ctxErr != nil {
		return nil, ctxErr
	}
	if f.debug {
		log.Printf("debug: check context error: %s\n", objLink)
	}
	return read(objLink)
}

// CreateObject - creates object to file system with specified data (aka content)
func (f *fsObjectStoreService) CreateObject(ctx context.Context, reader io.Reader) (cid.Cid, error) {
	data, readerErr := ioutil.ReadAll(reader)
	if readerErr != nil {
		return cid.Undef, readerErr
	}

	digest, err := objectstore.DigestPrefix.Sum(data)
	if err != nil {
		log.Printf("err: digesting object failed: %s\n", err.Error())
		return cid.Undef, ErrDataDigestionFailed
	}
	if f.debug {
		log.Printf("debug: created object cid: %s\n", digest)
	}

	if f.HasObject(ctx, digest) {
		return digest, nil
	}

	objLink := f.path(objectstore.DefaultLinkFunc(digest.String()))
	if err := write(objLink, data); err != nil {
		return digest, err
	}
	return digest, nil
}

func (f *fsObjectStoreService) ListObject(ctx context.Context) <-chan objectstore.ListObjectEvent {
	dir := fmt.Sprintf("%s/%s", f.dataDir, f.bucket)
	ch := make(chan objectstore.ListObjectEvent)

	go func() {
		defer close(ch)

		err := filepath.Walk(dir,
			func(path string, info os.FileInfo, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err != nil {
					return err
				}
				if info.Mode().IsRegular() {
					ch <- objectstore.ListObjectEvent{Object: info.Name(), Error: nil}
				}
				return nil
			})
		if err != nil {
			ch <- objectstore.ListObjectEvent{Object: "", Error: err}
			return
		}
	}()
	return ch
}
