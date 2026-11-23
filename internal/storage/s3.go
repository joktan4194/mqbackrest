package storage

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/joktan4194/mqbackrest/internal/config"
)

// S3 stores objects in any S3-compatible bucket.
type S3 struct {
	c      *minio.Client
	bucket string
	prefix string
}

// NewS3 builds an S3 client. Empty keys fall back to the standard AWS
// credential chain (env vars, shared config, IAM role).
func NewS3(cfg config.S3) (*S3, error) {
	var creds *credentials.Credentials
	if cfg.AccessKey != "" {
		creds = credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	} else {
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{},
		})
	}
	lookup := minio.BucketLookupAuto
	if cfg.PathStyle {
		lookup = minio.BucketLookupPath
	}
	c, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:        creds,
		Secure:       cfg.SSL(),
		Region:       cfg.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, err
	}
	return &S3{c: c, bucket: cfg.Bucket, prefix: strings.Trim(cfg.Prefix, "/")}, nil
}

func (s *S3) String() string { return fmt.Sprintf("s3://%s/%s", s.bucket, s.prefix) }

func (s *S3) key(k string) string {
	if s.prefix == "" {
		return k
	}
	return path.Join(s.prefix, k)
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader) error {
	_, err := s.c.PutObject(ctx, s.bucket, s.key(key), r, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
		PartSize:    16 << 20,
		// Plain signed requests instead of aws-chunked streaming: works with every
		// S3-compatible server. Integrity is covered by mqBackRest's own SHA-256 + GCM.
		DisableContentSha256: true,
	})
	return err
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.c.GetObject(ctx, s.bucket, s.key(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, s.mapErr(key, err)
	}
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		return nil, s.mapErr(key, err)
	}
	return obj, nil
}

func (s *S3) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	full := s.key(prefix)
	if strings.HasSuffix(prefix, "/") && !strings.HasSuffix(full, "/") {
		full += "/"
	}
	var out []ObjectInfo
	for o := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: full, Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		k := o.Key
		if s.prefix != "" {
			k = strings.TrimPrefix(k, s.prefix+"/")
		}
		out = append(out, ObjectInfo{Key: k, Size: o.Size})
	}
	return out, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	return s.c.RemoveObject(ctx, s.bucket, s.key(key), minio.RemoveObjectOptions{})
}

func (s *S3) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.c.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return false, nil
	}
	return false, err
}

func (s *S3) mapErr(key string, err error) error {
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return fmt.Errorf("%s: %w", key, ErrNotFound)
	}
	return err
}

// EnsureBucket creates the bucket if it does not exist.
func (s *S3) EnsureBucket(ctx context.Context, region string) error {
	ok, err := s.c.BucketExists(ctx, s.bucket)
	if err != nil || ok {
		return err
	}
	return s.c.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: region})
}

// Open builds the Storage described by a repo config.
func Open(cfg config.Repo) (Storage, error) {
	switch cfg.Type {
	case "local":
		return NewLocal(cfg.Path)
	case "s3":
		return NewS3(cfg.S3)
	default:
		return nil, fmt.Errorf("unknown repo type %q", cfg.Type)
	}
}
