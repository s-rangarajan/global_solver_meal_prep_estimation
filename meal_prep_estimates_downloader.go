package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	uuid "github.com/satori/go.uuid"
)

func downloadMealPrepTimeEstimates(ctx context.Context, mealPrepTimeEstimatesKey, mealPrepTimeEstimatesBucket string, downloader s3manageriface.DownloaderAPI) (io.Reader, error) {
	tempFile, err := ioutil.TempFile("", uuid.NewV4().String())
	if err != nil {
		return nil, fmt.Errorf("error creating tempfile: %w", err)
	}
	defer tempFile.Close()

	_, err = downloader.DownloadWithContext(
		ctx,
		tempFile,
		&s3.GetObjectInput{
			Key:    aws.String(mealPrepTimeEstimatesKey),
			Bucket: aws.String(mealPrepTimeEstimatesBucket),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error downloading meal prep estimates: %w", err)
	}

	return tempFile, nil
}
