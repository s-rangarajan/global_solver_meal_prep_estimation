package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	mealPrepTimeEstimatesS3KeyEnvVar    = "MEAL_PREP_TIME_ESTIMATES_KEY"
	mealPrepTimeEstimatesS3BucketEnvVar = "MEAL_PREP_TIME_ESTIMATES_BUCKET_KEY"
)

func main() {
	//ctx, signalCancel := signal.NotifyContext(context.Background(), os.Interrupt)
	//defer signalCancel()
	ctx := context.TODO()

	sess := session.Must(session.NewSession())
	s3Downloader := s3manager.NewDownloader(sess)
	mealPrepTimeEstimatesS3Key := os.Getenv(mealPrepTimeEstimatesS3KeyEnvVar)
	if mealPrepTimeEstimatesS3Key == "" {
		log.Fatalf("no key")
	}
	mealPrepTimeEstimatesS3Bucket := os.Getenv(mealPrepTimeEstimatesS3BucketEnvVar)
	if mealPrepTimeEstimatesS3Bucket == "" {
		log.Fatalf("no bucket")
	}
	mealPrepEstimateLookuper := NewBoltMealPrepTimeLookuper("meal_prep_estimates.bolt")
	go func() {
		f, err := os.Open("random_meal_prep_estimates.csv")
		if err != nil {
			log.Fatalf(err.Error())
		}
		if err := mealPrepEstimateLookuper.UpdateMealPrepEstimatesWithContext(ctx, f); err != nil {
			log.Println(err.Error())
		}
	}()
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		for {
			select {
			case <-ctx.Done():
			case <-ticker.C:
				updatedMealPrepEsimates, err := downloadMealPrepTimeEstimates(ctx, mealPrepTimeEstimatesS3Key, mealPrepTimeEstimatesS3Bucket, s3Downloader)
				if err != nil {
					log.Println(err.Error())
					continue
				}
				if err := mealPrepEstimateLookuper.UpdateMealPrepEstimatesWithContext(ctx, updatedMealPrepEsimates); err != nil {
					log.Println(err.Error())
				}
			}
		}
	}()
	simpleLookupEstimator := NewSimpleLookupEstimator(mealPrepEstimateLookuper)

	http.HandleFunc("/compute_meal_prep_estimates", func(w http.ResponseWriter, r *http.Request) {
		ComputeMealPrepTimeEstimatesWithContext(ctx, simpleLookupEstimator, w, r)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
