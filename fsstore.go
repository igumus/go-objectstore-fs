package fsstore

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/igumus/go-objectstore-lib"
)

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
func (f *fsObjectStoreService) HasObject(ctx context.Context, cid string) bool {
	objLink := f.path(objectstore.DefaultLinkFunc(cid))
	ret := exists(objLink)
	if f.debug {
		log.Printf("debug: has object: %s, %t\n", objLink, ret)
	}
	return ret
}

// ReadObject - reads object on file system with specified cid (aka content identifier)
func (f *fsObjectStoreService) ReadObject(ctx context.Context, cid string) ([]byte, error) {
	if !f.HasObject(ctx, cid) {
		return nil, objectstore.ErrObjectNotExists
	}
	objLink := f.path(objectstore.DefaultLinkFunc(cid))
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
func (f *fsObjectStoreService) CreateObject(ctx context.Context, reader io.Reader) (string, error) {
	data, readerErr := ioutil.ReadAll(reader)
	if readerErr != nil {
		return "", readerErr
	}
	cid := objectstore.DefaultDigestFunc(data)
	if err := checkContextError(ctx, f.debug); err != nil {
		return cid, err
	}
	if f.debug {
		log.Printf("debug: created object cid: %s\n", cid)
	}
	if !f.HasObject(ctx, cid) {
		objLink := f.path(objectstore.DefaultLinkFunc(cid))
		return write(cid, objLink, data)
	} else if f.debug {
		log.Printf("debug: skip writing already exists object: %s\n", cid)
	}
	return cid, nil
}

func (f *fsObjectStoreService) ListObject(ctx context.Context) (<-chan string, <-chan error) {
	dir := fmt.Sprintf("%s/%s", f.dataDir, f.bucket)
	chData := make(chan string)
	chErr := make(chan error, 1)

	go func() {
		defer close(chErr)
		defer close(chData)

		err := filepath.Walk(dir,
			func(path string, info os.FileInfo, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err != nil {
					return err
				}
				if info.Mode().IsRegular() {
					chData <- info.Name()
				}
				return nil
			})
		if err != nil {
			chErr <- err
			return
		}
	}()

	return chData, chErr
}
