package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config holds MinIO connection configuration
type Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
}

// Client wraps the MinIO client with helper methods
type Client struct {
	client *minio.Client
}

// NewClient creates a new MinIO client
func NewClient(cfg *Config) (*Client, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	return &Client{client: client}, nil
}

// CreateBucket creates a new bucket if it doesn't exist
func (c *Client) CreateBucket(ctx context.Context, bucketName string) error {
	exists, err := c.client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if exists {
		return nil // Bucket already exists
	}

	err = c.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil
}

// DeleteBucket deletes a bucket and all its contents
func (c *Client) DeleteBucket(ctx context.Context, bucketName string) error {
	// First, remove all objects in the bucket
	objectsCh := c.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true})
	for object := range objectsCh {
		if object.Err != nil {
			return fmt.Errorf("failed to list objects: %w", object.Err)
		}
		err := c.client.RemoveObject(ctx, bucketName, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to remove object %s: %w", object.Key, err)
		}
	}

	// Then remove the bucket
	err := c.client.RemoveBucket(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to remove bucket: %w", err)
	}

	return nil
}

// UploadFile uploads a file to a bucket
func (c *Client) UploadFile(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) error {
	_, err := c.client.PutObject(ctx, bucketName, objectName, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}

	return nil
}

// DeleteFile deletes a file from a bucket
func (c *Client) DeleteFile(ctx context.Context, bucketName, objectName string) error {
	err := c.client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	return nil
}

// ListFiles lists all files in a bucket with optional prefix
func (c *Client) ListFiles(ctx context.Context, bucketName, prefix string) ([]string, error) {
	var files []string

	objectsCh := c.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for object := range objectsCh {
		if object.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}
		files = append(files, object.Key)
	}

	return files, nil
}

// BucketExists checks if a bucket exists
func (c *Client) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return c.client.BucketExists(ctx, bucketName)
}

// ListBuckets lists all buckets
func (c *Client) ListBuckets(ctx context.Context) ([]string, error) {
	buckets, err := c.client.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	var names []string
	for _, bucket := range buckets {
		names = append(names, bucket.Name)
	}
	return names, nil
}

// GetObject retrieves an object's contents as bytes
func (c *Client) GetObject(ctx context.Context, bucketName, objectName string) ([]byte, error) {
	obj, err := c.client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object: %w", err)
	}

	return data, nil
}

// PutObjectBytes uploads bytes as an object
func (c *Client) PutObjectBytes(ctx context.Context, bucketName, objectName string, data []byte, contentType string) error {
	reader := io.NopCloser(io.NewSectionReader(bytes.NewReader(data), 0, int64(len(data))))
	_, err := c.client.PutObject(ctx, bucketName, objectName, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	return nil
}

// ObjectInfo represents information about an object
type ObjectInfo struct {
	Name         string
	Size         int64
	ContentType  string
	LastModified time.Time
}

// ListObjectsInfo lists objects with their info
func (c *Client) ListObjectsInfo(ctx context.Context, bucketName, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo

	objectsCh := c.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for object := range objectsCh {
		if object.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}
		objects = append(objects, ObjectInfo{
			Name:         object.Key,
			Size:         object.Size,
			ContentType:  object.ContentType,
			LastModified: object.LastModified,
		})
	}

	return objects, nil
}
