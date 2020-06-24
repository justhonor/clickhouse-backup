package chbackup

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCS - presents methods for manipulate data on GCS
type GCS struct {
	client *storage.Client
	Config *GCSConfig
}

// Connect - connect to GCS
func (gcs *GCS) Connect(overrideBucket string) error {
	var err error
	var clientOption option.ClientOption

	ctx := context.Background()

	if gcs.Config.CredentialsJSON != "" {
		clientOption = option.WithCredentialsJSON([]byte(gcs.Config.CredentialsJSON))
		gcs.client, err = storage.NewClient(ctx, clientOption)
	} else if gcs.Config.CredentialsFile != "" {
		clientOption = option.WithCredentialsFile(gcs.Config.CredentialsFile)
		gcs.client, err = storage.NewClient(ctx, clientOption)
	} else {
		gcs.client, err = storage.NewClient(ctx)
	}

	if err != nil {
		return err
	}

	return nil
}

func (gcs *GCS) Walk(gcsPath, overrideBucket, overridePath string, process func(r RemoteFile)) error {
	ctx := context.Background()
	bucket := gcs.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	it := gcs.client.Bucket(bucket).Objects(ctx, nil)
	for {
		object, err := it.Next()
		switch err {
		case nil:
			process(&gcsFile{object})
		case iterator.Done:
			return nil
		default:
			return err
		}
	}
}

func (gcs *GCS) Kind() string {
	return "GCS"
}

func (gcs *GCS) GetFileReader(key, overrideBucket string) (io.ReadCloser, error) {
	ctx := context.Background()
	bucket := gcs.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	obj := gcs.client.Bucket(bucket).Object(key)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func (gcs *GCS) GetFileWriter(key, overrideBucket string) io.WriteCloser {
	ctx := context.Background()
	bucket := gcs.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	obj := gcs.client.Bucket(bucket).Object(key)
	return obj.NewWriter(ctx)
}

func (gcs *GCS) PutFile(key, overrideBucket string, r io.ReadCloser) error {
	ctx := context.Background()
	bucket := gcs.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	obj := gcs.client.Bucket(bucket).Object(key)
	writer := obj.NewWriter(ctx)

	if _, err := io.Copy(writer, r); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}
	return nil
}

func (gcs *GCS) GetFile(key, overrideBucket string) (RemoteFile, error) {
	ctx := context.Background()
	bucket := gcs.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	objAttr, err := gcs.client.Bucket(bucket).Object(key).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &gcsFile{objAttr}, nil
}

func (gcs *GCS) DeleteFile(key, overrideBucket string) error {
	ctx := context.Background()
	bucket := gcs.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	object := gcs.client.Bucket(bucket).Object(key)
	return object.Delete(ctx)
}

type gcsFile struct {
	objAttr *storage.ObjectAttrs
}

func (f *gcsFile) Size() int64 {
	return f.objAttr.Size
}

func (f *gcsFile) Name() string {
	return f.objAttr.Name
}

func (f *gcsFile) LastModified() time.Time {
	return f.objAttr.Updated
}
