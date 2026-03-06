package kernel

import (
	"math/rand"
	"time"
)

type SimParameters struct {
	HorizonDays int
	AssetValue  float64
}

type Result struct {
	RiskScore float64
}

func ExecuteStochasticModel(p SimParameters) Result {

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	total := 0.0

	for i := 0; i < 10000; i++ {
		total += rng.Float64()
	}

	return Result{
		RiskScore: total / 10000,
	}
}
