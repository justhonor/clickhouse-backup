package chbackup

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

// S3 - presents methods for manipulate data on s3
type S3 struct {
	Config    *S3Config
	AWSConfig *aws.Config
}

// Connect - connect to s3
func (s *S3) Connect() error {
	awsDefaults := defaults.Get()
	defaultCredProviders := defaults.CredProviders(awsDefaults.Config, awsDefaults.Handlers)

	// Define custom static cred provider
	staticCreds := &credentials.StaticProvider{Value: credentials.Value{
		AccessKeyID:     s.Config.AccessKey,
		SecretAccessKey: s.Config.SecretKey,
	}}

	// Append static creds to the defaults
	customCredProviders := append([]credentials.Provider{staticCreds}, defaultCredProviders...)
	creds := credentials.NewChainCredentials(customCredProviders)

	s.AWSConfig = &aws.Config{
		Credentials:      creds,
		Region:           aws.String(s.Config.Region),
		Endpoint:         aws.String(s.Config.Endpoint),
		DisableSSL:       aws.Bool(s.Config.DisableSSL),
		S3ForcePathStyle: aws.Bool(s.Config.ForcePathStyle),
		MaxRetries:       aws.Int(30),
		LogLevel:         aws.LogLevel(aws.LogDebug), // TODO
	}

	if s.Config.DisableCertVerification {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		s.AWSConfig.HTTPClient = &http.Client{Transport: tr}
	}
	return nil
}

func (s *S3) Kind() string {
	return "S3"
}

func (s *S3) GetFileReader(key string) (io.ReadCloser, error) {
	session, err := session.NewSession(s.AWSConfig)
	if err != nil {
		return nil, err
	}
	svc := s3.New(session)
	req, resp := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	})
	if err := req.Send(); err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (s *S3) PutFile(key string, r io.ReadCloser) error {
	session, err := session.NewSession(s.AWSConfig)
	if err != nil {
		return err
	}
	uploader := s3manager.NewUploader(session)
	uploader.Concurrency = 10
	uploader.PartSize = s.Config.PartSize
	var sse *string
	if s.Config.SSE != "" {
		sse = aws.String(s.Config.SSE)
	}
	if s.Config.PathHostnameInclude != false {
		if hostname, err := os.Hostname(); err == nil {
			key = fmt.Sprintf("%s/%s_%s", path.Dir(key), hostname, path.Base(key))
		}
	}

	_, err = uploader.Upload(&s3manager.UploadInput{
		ACL:                  aws.String(s.Config.ACL),
		Bucket:               aws.String(s.Config.Bucket),
		Key:                  aws.String(key),
		Body:                 r,
		ServerSideEncryption: sse,
	})
	return err
}

func (s *S3) DeleteFile(key string) error {
	session, err := session.NewSession(s.AWSConfig)
	if err != nil {
		return err
	}
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	}
	_, err = s3.New(session).DeleteObject(params)
	if err != nil {
		return errors.Wrapf(err, "DeleteFile, deleting object %+v", params)
	}
	return nil
}

func (s *S3) GetFile(key string) (RemoteFile, error) {
	session, err := session.NewSession(s.AWSConfig)
	if err != nil {
		return nil, err
	}
	svc := s3.New(session)
	head, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == "NotFound" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &s3File{*head.ContentLength, *head.LastModified, key}, nil
}

func (s *S3) Walk(s3Path string, process func(r RemoteFile)) error {
	return s.remotePager(s.Config.Path, false, func(page *s3.ListObjectsV2Output) {
		for _, c := range page.Contents {
			process(&s3File{*c.Size, *c.LastModified, *c.Key})
		}
	})
}

func (s *S3) remotePager(s3Path string, delim bool, pager func(page *s3.ListObjectsV2Output)) error {
	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Config.Bucket), // Required
		MaxKeys: aws.Int64(1000),
	}
	if s3Path != "" && s3Path != "/" {
		params.Prefix = aws.String(s3Path)
	}
	if delim {
		params.Delimiter = aws.String("/")
	}
	wrapper := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		pager(page)
		return true
	}
	session, err := session.NewSession(s.AWSConfig)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.Config.Timeout)*time.Millisecond)
	defer cancel()
	c := make(chan error, 1)
	go func() { c <- s3.New(session).ListObjectsV2PagesWithContext(ctx, params, wrapper) }()
	select {
	case <-ctx.Done():
		<-c
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("S3 request timeout after %dmsec", s.Config.Timeout)
		}
		return ctx.Err()
	case err := <-c:
		return err
	}
}

type s3File struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *s3File) Size() int64 {
	return f.size
}

func (f *s3File) Name() string {
	return f.name
}

func (f *s3File) LastModified() time.Time {
	return f.lastModified
}
