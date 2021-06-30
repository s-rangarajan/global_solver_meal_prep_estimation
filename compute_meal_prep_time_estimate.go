package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const computeTimeout = 3 * time.Second

type ComputeMealPrepTimeEstimatesRequest struct {
	Orders       map[string]map[string]string `json:"orders"`
	DispatchTime time.Time                    `json:"dispatch_time"`
}

func (c ComputeMealPrepTimeEstimatesRequest) Validate() error {
	if c.DispatchTime.IsZero() {
		return fmt.Errorf("missing dispatch time")
	}
	if c.Orders == nil {
		return fmt.Errorf("missing orders")
	}

	return nil
}

type ComputedMealPrepTimeEstimates struct {
	MealPrepTimeEstimates map[string]map[string]string `json:"meal_prep_time_estimates"`
}

func ComputeMealPrepTimeEstimatesWithContext(ctx context.Context, estimator MealPrepTimeEstimator, w http.ResponseWriter, r *http.Request) {
	ctx, cancelFunc := context.WithTimeout(ctx, computeTimeout)
	defer cancelFunc()

	w.Header().Set("Content-Type", "application/json")
	var computeRequest ComputeMealPrepTimeEstimatesRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&computeRequest); err != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(fmt.Sprintf(`{"error": "%s"}`, fmt.Errorf("error unmarshaling request: %w", err).Error())))
		return
	}

	if err := computeRequest.Validate(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"error": "%s"}`, fmt.Errorf("invalid request: %w", err).Error())))
		return
	}

	computedValues, err := estimator.EstimateMealPrepTimeWithContext(ctx, computeRequest)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"error": "%s"}`, fmt.Errorf("error computing estimates: %w", err).Error())))
		return
	}

	if err := json.NewEncoder(w).Encode(computedValues); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
