package main

import (
	"context"
	"runtime"
	"sync"
	"time"
)

type MealPrepTimeEstimator interface {
	EstimateMealPrepTimeWithContext(context.Context, ComputeMealPrepTimeEstimatesRequest) (ComputedMealPrepTimeEstimates, error)
}

type SimpleLookupEstimator struct {
	MealPrepTimeLookuper
}

func NewSimpleLookupEstimator(mealPrepTimeLookuper MealPrepTimeLookuper) *SimpleLookupEstimator {
	return &SimpleLookupEstimator{mealPrepTimeLookuper}
}

type mealPrepTimeEstimateInput struct {
	OrderID      string
	RestaurantID string
	ItemCount    string
	DispatchTime time.Time
}

type mealPrepTimeEstimateResult struct {
	OrderID      string
	RestaurantID string
	MealPrepTime string
}

func (s *SimpleLookupEstimator) EstimateMealPrepTimeWithContext(ctx context.Context, mealPrepTimeEstimatesRequest ComputeMealPrepTimeEstimatesRequest) (ComputedMealPrepTimeEstimates, error) {
	responseChan := make(chan (ComputedMealPrepTimeEstimates))
	go func() {
		var wg sync.WaitGroup
		computeChan := make(chan (mealPrepTimeEstimateInput), 100000)
		resultsChan := make(chan (mealPrepTimeEstimateResult), 100000)
		for i := 0; i < runtime.NumCPU(); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for input := range computeChan {
					mealPrepEstimate, err := s.LookupMealPrepTimeWithContext(ctx, input.RestaurantID, input.ItemCount, input.DispatchTime)
					if err != nil {
						mealPrepEstimate = "15.00"
					}

					resultsChan <- mealPrepTimeEstimateResult{
						OrderID:      input.OrderID,
						RestaurantID: input.RestaurantID,
						MealPrepTime: mealPrepEstimate,
					}
				}
			}()
		}

		computedMealPrepTimeEstimates := ComputedMealPrepTimeEstimates{MealPrepTimeEstimates: make(map[string]map[string]string, len(mealPrepTimeEstimatesRequest.Orders))}
		for orderID := range mealPrepTimeEstimatesRequest.Orders {
			computedMealPrepTimeEstimates.MealPrepTimeEstimates[orderID] = make(map[string]string, len(mealPrepTimeEstimatesRequest.Orders[orderID]))
		}
		go func() {
			for orderID := range mealPrepTimeEstimatesRequest.Orders {
				for restaurantID := range mealPrepTimeEstimatesRequest.Orders[orderID] {
					computeChan <- mealPrepTimeEstimateInput{
						OrderID:      orderID,
						RestaurantID: restaurantID,
						ItemCount:    mealPrepTimeEstimatesRequest.Orders[orderID][restaurantID],
						DispatchTime: mealPrepTimeEstimatesRequest.DispatchTime,
					}
				}
			}

			close(computeChan)
		}()

		go func() {
			wg.Wait()
			close(resultsChan)
		}()
		for result := range resultsChan {
			computedMealPrepTimeEstimates.MealPrepTimeEstimates[result.OrderID][result.RestaurantID] = result.MealPrepTime
		}

		responseChan <- computedMealPrepTimeEstimates
	}()

	select {
	case <-ctx.Done():
		return ComputedMealPrepTimeEstimates{}, ctx.Err()
	case response := <-responseChan:
		return response, nil
	}
}
