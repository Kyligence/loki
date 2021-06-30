package huawei

import (
	"context"
	"errors"
	"flag"
	"io"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
)

var (
	ObsRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "obs_request_duration_seconds",
		Help:      "Time spent doing Obs requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"}))
)

type ObsStorageConfig struct {
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
}

func (c *ObsStorageConfig) RegisterFlags(f *flag.FlagSet) {
	c.RegisterFlagsWithPrefix("", f)
}

func (c *ObsStorageConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.AccessKey, prefix+"obs.access-key", "", "Huawei Access Key")
	f.StringVar(&c.SecretKey, prefix+"obs.secret-key", "", "Huawei Secret Key")
	f.StringVar(&c.Endpoint, prefix+"obs.endpoint", "", "Huawei Endpoint")
	f.StringVar(&c.Bucket, prefix+"obs.bucket", "", "Huawei OBS Bucket")
}

func (c *ObsStorageConfig) Validate() error {
	if c.Bucket == "" {
		return errors.New("no Huawei OBS Bucket specified")
	}
	if c.AccessKey != "" && c.SecretKey == "" ||
		c.AccessKey == "" && c.SecretKey != "" {
		return errors.New("must supply both an Access Key and Secret Key or neither")
	}
	if c.Endpoint == "" {
		return errors.New("endpoint must be specified")
	}
	return nil
}

type ObsStorage struct {
	cfg       *ObsStorageConfig
	obsClient *obs.ObsClient
}

func NewObsStorage(cfg *ObsStorageConfig) (*ObsStorage, error) {
	obsClient, err := obs.New(cfg.AccessKey, cfg.SecretKey, cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	obsStorage := &ObsStorage{
		cfg:       cfg,
		obsClient: obsClient,
	}
	return obsStorage, err
}

func (b *ObsStorage) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	var resp *obs.GetObjectOutput
	err := instrument.CollectedRequest(ctx, "OBS.GetObject", ObsRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		resp, err = b.obsClient.GetObject(&obs.GetObjectInput{
			GetObjectMetadataInput: obs.GetObjectMetadataInput{
				Bucket: b.cfg.Bucket,
				Key:    objectKey,
			},
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (b *ObsStorage) DeleteObject(ctx context.Context, objectKey string) error {
	return instrument.CollectedRequest(ctx, "OBS.DeleteObject", ObsRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		_, err := b.obsClient.DeleteObject(&obs.DeleteObjectInput{
			Bucket: b.cfg.Bucket,
			Key:    objectKey,
		})
		return err
	})
}

func (b *ObsStorage) PutObject(ctx context.Context, objectKey string, object io.ReadSeeker) error {
	return instrument.CollectedRequest(ctx, "OBS.PutObject", ObsRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		_, err := b.obsClient.PutObject(&obs.PutObjectInput{
			Body: object,
			PutObjectBasicInput: obs.PutObjectBasicInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket: b.cfg.Bucket,
					Key:    objectKey,
				},
			},
		})
		return err
	})
}

func (b *ObsStorage) List(ctx context.Context, prefix string, delimiter string) ([]chunk.StorageObject, []chunk.StorageCommonPrefix, error) {
	var storageObjects []chunk.StorageObject
	var storageCommonPrefix []chunk.StorageCommonPrefix
	err := instrument.CollectedRequest(ctx, "OBS.ListObject", ObsRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		objListInput := &obs.ListObjectsInput{
			Bucket: b.cfg.Bucket,
			ListObjsInput: obs.ListObjsInput{
				Prefix:    prefix,
				Delimiter: delimiter,
			},
		}
		for {
			output, err := b.obsClient.ListObjects(objListInput)
			if err != nil {
				return err
			}
			for _, content := range output.Contents {
				storageObjects = append(storageObjects, chunk.StorageObject{
					Key:        content.Key,
					ModifiedAt: content.LastModified,
				})
			}
			for _, commonPrefix := range output.CommonPrefixes {
				storageCommonPrefix = append(storageCommonPrefix, chunk.StorageCommonPrefix(commonPrefix))
			}
			if !output.IsTruncated {
				break
			}
			if output.NextMarker == "" {
				break
			}
			objListInput.Marker = output.NextMarker
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return storageObjects, storageCommonPrefix, nil
}

func (b *ObsStorage) Stop() {}
