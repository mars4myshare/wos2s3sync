package storage

import (
	"errors"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Storage struct {
	Endpoint string
	Ak       string
	Sk       string
	Bucket   string
	Config   *aws.Config
}

func NewS3Storage(endpoint, ak, sk, bucket string) *S3Storage {
	c := S3Storage{
		endpoint,
		ak,
		sk,
		bucket,
		&aws.Config{
			Endpoint:         aws.String(endpoint),
			Region:           aws.String("us-east-1"), // removed?
			DisableSSL:       aws.Bool(true),          // removed?
			S3ForcePathStyle: aws.Bool(true),
		},
	}
	c.Config = c.Config.WithCredentials(credentials.NewStaticCredentials(ak, sk, ""))
	return &c
}

func (t *S3Storage) Write(key string, obj SyncObject) (string, error) {
	body := obj.GetBody()
	pr, pw := io.Pipe()
	tr := io.TeeReader(body, pw)
	defer body.Close()

	type Result struct {
		value string
		err   error
	}
	done := make(chan Result)
	defer close(done)

	go func() {
		defer pw.Close()
		svc := s3.New(session.New(t.Config))
		contentLength := obj.GetContentLength()
		contentType := obj.GetContentType()
		input := &s3.PutObjectInput{
			Body:          aws.ReadSeekCloser(tr),
			Bucket:        aws.String(t.Bucket),
			Key:           aws.String(key),
			ContentLength: &contentLength,
			ContentType:   &contentType,
			//StorageClass: aws.String("STANDARD_IA"),
		}
		_, err := svc.PutObject(input)
		done <- Result{"", err}
	}()
	go func() {
		md5, err := CalcMD5(pr)
		done <- Result{md5, err}
	}()

	var md5 string
	var err error
	for i := 0; i < 2; i++ {
		r := <-done
		if r.err != nil {
			err = r.err
			continue
		}
		if r.value != "" {
			md5 = r.value
		}
	}
	return md5, err
}

func (t *S3Storage) Read(key string) (SyncObject, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(t.Bucket),
		Key:    aws.String(key),
	}

	svc := s3.New(session.New(t.Config))
	output, err := svc.GetObject(input)
	if err != nil {
		return nil, err
	}

	if output.Body == nil {
		return nil, errors.New("No body got from response")
	}
	s3Obj := SyncObjectImp{
		body: output.Body,
	}

	if output.ContentType != nil {
		s3Obj.contentType = *output.ContentType
	}
	if output.ContentLength != nil {
		s3Obj.length = *output.ContentLength
	}
	return &s3Obj, nil
}

func (t *S3Storage) GetBucket() string {
	return t.Bucket
}
