package chbackup

import (
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
	session *session.Session
	Config  *S3Config
}

// Connect - connect to s3
func (s *S3) Connect(overrideBucket string) error {
	var err error

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

	var awsConfig = &aws.Config{
		Credentials:      creds,
		Region:           aws.String(s.Config.Region),
		Endpoint:         aws.String(s.Config.Endpoint),
		DisableSSL:       aws.Bool(s.Config.DisableSSL),
		S3ForcePathStyle: aws.Bool(s.Config.ForcePathStyle),
		MaxRetries:       aws.Int(30),
	}

	if s.Config.DisableCertVerification {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		awsConfig.HTTPClient = &http.Client{Transport: tr}
	}

	if s.session, err = session.NewSession(awsConfig); err != nil {
		return err
	}
	return nil
}

func (s *S3) Kind() string {
	return "S3"
}

func (s *S3) GetFileReader(key, overrideBucket string) (io.ReadCloser, error) {
	svc := s3.New(s.session)

	bucket := s.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	req, resp := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err := req.Send(); err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (s *S3) PutFile(key, overrideBucket string, r io.ReadCloser) error {
	uploader := s3manager.NewUploader(s.session)
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

	bucket := s.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	_, err := uploader.Upload(&s3manager.UploadInput{
		ACL:                  aws.String(s.Config.ACL),
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		Body:                 r,
		ServerSideEncryption: sse,
	})
	return err
}

func (s *S3) DeleteFile(key, overrideBucket string) error {
	bucket := s.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err := s3.New(s.session).DeleteObject(params)
	if err != nil {
		return errors.Wrapf(err, "DeleteFile, deleting object %+v", params)
	}
	return nil
}

func (s *S3) GetFile(key, overrideBucket string) (RemoteFile, error) {
	svc := s3.New(s.session)
	bucket := s.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	head, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
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

func (s *S3) Walk(s3Path, overrideBucket, overridePath string, process func(r RemoteFile)) error {
	usePath := s.Config.Path
	if len(overridePath) > 0 {
		usePath = overridePath
	}
	return s.remotePager(usePath, false, overrideBucket, func(page *s3.ListObjectsV2Output) {
		for _, c := range page.Contents {
			process(&s3File{*c.Size, *c.LastModified, *c.Key})
		}
	})
}

func (s *S3) remotePager(s3Path string, delim bool, overrideBucket string, pager func(page *s3.ListObjectsV2Output)) error {
	bucket := s.Config.Bucket
	if len(overrideBucket) > 0 {
		bucket = overrideBucket
	}
	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket), // Required
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
	return s3.New(s.session).ListObjectsV2Pages(params, wrapper)
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
