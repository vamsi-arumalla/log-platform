package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vamsi-arumalla/log-platform/internal/config"
	"github.com/vamsi-arumalla/log-platform/internal/metrics"
	"github.com/vamsi-arumalla/log-platform/internal/model"
	"go.uber.org/zap"
)

type ColdStore struct {
	client *s3.Client
	bucket string
	logger *zap.Logger
}

func NewColdStore(cfg config.S3Config, logger *zap.Logger) (*ColdStore, error) {
	resolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               cfg.Endpoint,
				HostnameImmutable: true,
			}, nil
		},
	)

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithEndpointResolverWithOptions(resolver),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("loading aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	cs := &ColdStore{
		client: client,
		bucket: cfg.Bucket,
		logger: logger,
	}

	if err := cs.ensureBucket(context.Background()); err != nil {
		return nil, err
	}

	return cs, nil
}

func (c *ColdStore) Archive(ctx context.Context, entries []model.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp.Before(entries[j].Timestamp)
	})

	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshaling entries: %w", err)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return fmt.Errorf("compressing: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("closing gzip: %w", err)
	}

	first := entries[0].Timestamp
	key := fmt.Sprintf("logs/%s/%s/%s/%d.json.gz",
		first.Format("2006"),
		first.Format("01"),
		first.Format("02"),
		first.UnixNano(),
	)

	_, err = c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/gzip"),
		Metadata: map[string]string{
			"entry-count": fmt.Sprintf("%d", len(entries)),
			"start-time":  first.Format(time.RFC3339),
			"end-time":    entries[len(entries)-1].Timestamp.Format(time.RFC3339),
		},
	})
	if err != nil {
		return fmt.Errorf("uploading to s3: %w", err)
	}

	metrics.StorageBytes.WithLabelValues("cold").Add(float64(buf.Len()))
	metrics.StorageObjectCount.WithLabelValues("cold").Inc()

	c.logger.Info("archived entries to cold storage",
		zap.Int("count", len(entries)),
		zap.String("key", key),
		zap.Int("compressed_bytes", buf.Len()),
	)
	return nil
}

func (c *ColdStore) Query(ctx context.Context, req model.QueryRequest) ([]model.LogEntry, error) {
	prefix := fmt.Sprintf("logs/%s/%s/",
		req.StartTime.Format("2006"),
		req.StartTime.Format("01"),
	)

	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	var allEntries []model.LogEntry
	limit := req.Limit
	if limit == 0 {
		limit = 1000
	}

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing objects: %w", err)
		}

		for _, obj := range page.Contents {
			entries, err := c.readObject(ctx, *obj.Key)
			if err != nil {
				c.logger.Warn("failed to read archive", zap.String("key", *obj.Key), zap.Error(err))
				continue
			}

			for _, entry := range entries {
				if entry.Timestamp.Before(req.StartTime) || entry.Timestamp.After(req.EndTime) {
					continue
				}
				if req.Level != "" && entry.Level != req.Level {
					continue
				}
				if req.Source != "" && entry.Source != req.Source {
					continue
				}
				if req.Query != "" && !strings.Contains(entry.Message, req.Query) {
					continue
				}
				if !matchLabels(entry.Labels, req.Labels) {
					continue
				}
				allEntries = append(allEntries, entry)
				if len(allEntries) >= limit {
					return allEntries, nil
				}
			}
		}
	}

	return allEntries, nil
}

func (c *ColdStore) readObject(ctx context.Context, key string) ([]model.LogEntry, error) {
	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("getting object %s: %w", key, err)
	}
	defer resp.Body.Close()

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decompressing %s: %w", key, err)
	}
	defer gz.Close()

	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", key, err)
	}

	var entries []model.LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("unmarshaling %s: %w", key, err)
	}

	return entries, nil
}

func (c *ColdStore) ensureBucket(ctx context.Context) error {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err == nil {
		return nil
	}

	_, err = c.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		return fmt.Errorf("creating bucket %s: %w", c.bucket, err)
	}

	c.logger.Info("created S3 bucket", zap.String("bucket", c.bucket))
	return nil
}
