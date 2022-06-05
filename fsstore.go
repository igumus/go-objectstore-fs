package fsstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/igumus/go-objectstore-lib"
)

// Captures/Represents filesystem backed objectstore service information
type fsObjectStoreService struct {
	config *fsObjectStoreConfig
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
		config: cfg,
	}

	return srv, nil
}

func (a *fsObjectStoreService) checkContextError(ctx context.Context) error {
	switch ctx.Err() {
	case context.Canceled:
		return objectstore.ErrOperationCancelled
	case context.DeadlineExceeded:
		return objectstore.ErrOperationDeadlineExceeded
	default:
		return nil
	}
}

// toObjectLink - converts cid (aka content identifier) and name to object link for objectstore
func (f *fsObjectStoreService) ToObjectLink(cid, name string) string {
	sepLen := f.config.seperatorLength
	parentFolder := string(cid[:sepLen])
	childFolder := string(cid[sepLen:])
	ret := fmt.Sprintf(_objFormat, f.config.dir, f.config.bucket, parentFolder, childFolder, name)
	if f.config.debug {
		log.Printf("debug: objectLink %s, %s: %s\n", cid, name, ret)
	}
	return ret
}

// HasObject - checks whether object exists on file system with specified cid (aka content identifier)
func (f *fsObjectStoreService) HasObject(ctx context.Context, cid string) bool {
	objLink := f.ToObjectLink(cid, cid)
	_, err := os.Stat(objLink)
	ret := !errors.Is(err, os.ErrNotExist)
	if f.config.debug {
		log.Printf("debug: has object: %s, %t\n", objLink, ret)
	}
	return ret
}

// ReadObject - reads object on file system with specified cid (aka content identifier)
func (f *fsObjectStoreService) ReadObject(ctx context.Context, cid string) ([]byte, error) {
	if !f.HasObject(ctx, cid) {
		return nil, objectstore.ErrObjectNotExists
	}
	objLink := f.ToObjectLink(cid, cid)
	if f.config.debug {
		log.Printf("debug: check object existence: %s\n", objLink)
	}
	if ctxErr := f.checkContextError(ctx); ctxErr != nil {
		return nil, ctxErr
	}
	if f.config.debug {
		log.Printf("debug: check context error: %s\n", objLink)
	}
	file, err := os.Open(objLink)
	if err != nil {
		log.Printf("err: opening object failed: %s, %v\n", objLink, err)
		return nil, objectstore.ErrObjectReadingFailed
	}

	binData := bytes.Buffer{}
	_, err = binData.ReadFrom(file)
	if err != nil {
		log.Printf("err: reading object failed: %s, %v\n", objLink, err)
		return nil, objectstore.ErrObjectReadingFailed
	}

	return binData.Bytes(), nil
}

// DigestObject - calculates SHA256 sum of given data.
func (f *fsObjectStoreService) DigestObject(ctx context.Context, data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

// CreateObject - creates object to file system with specified data (aka content)
func (f *fsObjectStoreService) CreateObject(ctx context.Context, data []byte) (string, error) {
	cid := f.DigestObject(ctx, data)
	if err := f.checkContextError(ctx); err != nil {
		return cid, err
	}
	if f.config.debug {
		log.Printf("debug: created object cid: %s\n", cid)
		log.Printf("debug: checked context error: %s\n", cid)
	}
	if !f.HasObject(ctx, cid) {
		objLink := f.ToObjectLink(cid, cid)
		os.MkdirAll(filepath.Dir(objLink), 0777)
		file, err := os.Create(objLink)
		if err != nil {
			log.Printf("err: creating object failed: %s, %v\n", objLink, err)
			return cid, objectstore.ErrObjectWritingFailed
		}

		binData := bytes.Buffer{}
		binData.Write(data)

		_, err = binData.WriteTo(file)
		if err != nil {
			log.Printf("err: writing object failed: %s, %v\n", objLink, err)
			return cid, objectstore.ErrObjectWritingFailed
		}
	} else if f.config.debug {
		log.Printf("debug: skip writing already exists object: %s\n", cid)
	}
	return cid, nil
}
