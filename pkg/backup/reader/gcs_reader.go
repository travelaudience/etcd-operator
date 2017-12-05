package reader

import (
	"context"
	"io"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"fmt"
	"bytes"
	"io/ioutil"
)


var (
	gcsPathRegexp = regexp.MustCompile(`^gs://(?P<BucketName>[^/]+)/(?P<ObjectPath>.*)$`)
)

type gcsReader struct {
}

func NewGCSReader() Reader {
	return &gcsReader{}
}

func (gr *gcsReader) Open(path string) (io.ReadCloser, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	matches, err := SplitGCSPath(path)
	if err != nil {
		return nil, err
	}

	or, err := client.Bucket(matches[1]).Object(matches[2]).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer or.Close()

	data, err := ioutil.ReadAll(or)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func SplitGCSPath(path string) ([]string, error) {
	matches := gcsPathRegexp.FindStringSubmatch(path)
	if len(matches) != 3 {
		return nil, fmt.Errorf("failed to parse %s as a GCS path", path)
	}
	return matches, nil
}
