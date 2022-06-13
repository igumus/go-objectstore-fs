package fsstore

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/igumus/go-objectstore-lib"
)

// checkContextError - check given context has an error
func checkContextError(ctx context.Context, debug bool) error {
	switch ctx.Err() {
	case context.Canceled:
		if debug {
			log.Println("debug: context canceled")
		}
		return objectstore.ErrOperationCancelled
	case context.DeadlineExceeded:
		if debug {
			log.Println("debug: context deadline exceeded")
		}
		return objectstore.ErrOperationDeadlineExceeded
	default:
		if debug {
			log.Println("debug: context normal")
		}
		return nil
	}
}

// exits - check existence of path value in file system
func exists(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, os.ErrNotExist)
}

// read - reads objLink value as binary
func read(objLink string) ([]byte, error) {
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

func write(objLink string, data []byte) error {
	os.MkdirAll(filepath.Dir(objLink), 0777)
	file, err := os.Create(objLink)
	if err != nil {
		log.Printf("err: creating object failed: %s, %v\n", objLink, err)
		return objectstore.ErrObjectWritingFailed
	}

	binData := bytes.Buffer{}
	binData.Write(data)

	_, err = binData.WriteTo(file)
	if err != nil {
		log.Printf("err: writing object failed: %s, %v\n", objLink, err)
		return objectstore.ErrObjectWritingFailed
	}
	return nil
}
