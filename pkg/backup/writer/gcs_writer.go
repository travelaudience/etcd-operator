package writer

import (
	"context"
	"io"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
)

type gcsWriter struct {
	bucketName string
}

// NewGCSWriter creates a GCS writer.
func NewGCSWriter(bucketName string) Writer {
	return &gcsWriter{bucketName: bucketName}
}

// Write writes the backup file to the given GCS path.
func (sw *gcsWriter) Write(path string, r io.Reader) (int64, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return 0, err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	ow := client.Bucket(sw.bucketName).Object(path).NewWriter(ctx)
	if _, err := ow.Write(data); err != nil {
		return 0, err
	}
	if err := ow.Close(); err != nil {
		return 0, err
	}

	return int64(len(data)), nil
}
