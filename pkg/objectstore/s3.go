// Package objectstore — S3-совместимое хранилище payload'ов (MinIO в dev).
package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"qrok/pkg/fault"
)

type Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

// Client загружает и читает объекты в S3-совместимом бакете.
type Client struct {
	client *minio.Client
	bucket string
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fault.ErrServiceUnavail.
			Wrap(err, "failed to create S3 client").
			WithOp("objectstore.new").
			WithHint("check s3.endpoint and credentials in config")
	}

	c := &Client{client: client, bucket: cfg.Bucket}
	if err := c.ensureBucket(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) ensureBucket(ctx context.Context) error {
	exists, err := c.client.BucketExists(ctx, c.bucket)
	if err != nil {
		return fault.ErrServiceUnavail.
			Wrap(err, "failed to check S3 bucket").
			WithOp("objectstore.ensure_bucket")
	}
	if exists {
		return nil
	}
	if err := c.client.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{}); err != nil {
		return fault.ErrServiceUnavail.
			Wrap(err, "failed to create S3 bucket").
			WithOp("objectstore.ensure_bucket")
	}
	return nil
}

func (c *Client) Put(ctx context.Context, key string, data []byte) error {
	_, err := c.client.PutObject(ctx, c.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fault.ErrServiceUnavail.
			Wrap(err, "failed to write object to S3").
			WithOp("objectstore.put").
			WithArg("key", key)
	}
	return nil
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := c.client.GetObject(ctx, c.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fault.ErrServiceUnavail.
			Wrap(err, "failed to read object from S3").
			WithOp("objectstore.get").
			WithArg("key", key)
	}
	defer func() { _ = obj.Close() }()

	data, err := io.ReadAll(obj)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, fault.ErrNotFound.
				New("object not found in S3").
				WithOp("objectstore.get").
				WithArg("key", key)
		}
		return nil, fault.ErrServiceUnavail.
			Wrap(err, "failed to read S3 object body").
			WithOp("objectstore.get").
			WithArg("key", key)
	}
	return data, nil
}

// Ping checks that the configured bucket is reachable.
func (c *Client) Ping(ctx context.Context) error {
	exists, err := c.client.BucketExists(ctx, c.bucket)
	if err != nil {
		return fault.ErrServiceUnavail.Wrap(err, "failed to check S3 readiness").WithOp("objectstore.ping")
	}
	if !exists {
		return fault.ErrServiceUnavail.New("S3 bucket is missing").WithOp("objectstore.ping")
	}
	return nil
}

// ObjectKey формирует ключ объекта для события.
func ObjectKey(tunnelID, eventID string) string {
	return fmt.Sprintf("events/%s/%s", tunnelID, eventID)
}
