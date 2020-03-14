package storage

import (
	"bytes"
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
	defer body.Close()
	var md5Buf, uploadBuf bytes.Buffer
	mw := io.MultiWriter(&md5Buf, &uploadBuf)

	type md5Result struct {
		value string
		err Error
	}

	uploadDone := make(chan Error)
	md5Done := make(chan md5Result)
	go func() {
		svc := s3.New(session.New(t.Config))
		input := &s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(uploadBuf),
			Bucket: aws.String(t.Bucket),
			Key:    aws.String(key),
			//StorageClass: aws.String("STANDARD_IA"),
		}
		_, err := svc.PutObject(input)
		uploadDone <- err
	}
	go func() {
		md5, err := CalcMD5(md5Buf)
		md5Done <- md5Result{md5, err}
	}
	if _, err := io.Copy(mw, body); err != nil {
		return "", err
	}

	md5 := <- md5Done
	if md5.err != nil {
		return "", md5.err
	}
	uploadedError := <-uploadDone
	if uploadedError != nil {
		return "", uploadedError
	}

	return md5.value, nil
}

func (t *S3Storage) GetObject(key string) (SyncObject, error) {
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
