package main

import (
    "math"
)

type BasicStats struct {
    Count float64
    Sum float64
    Sumsq float64
}

func (s *BasicStats) Add(data float64) {
    s.Count++
    s.Sum += data
    s.Sumsq += data*data
}

func (s *BasicStats) Mean() float64 {
    return s.Sum / s.Count
}

func (s *BasicStats) Variance() (variance float64) {
    if s.Count < 2 {
        return 0
    }
    mean := s.Mean()
    variance = s.Sumsq - s.Count * mean * mean
    if variance < 0 {
        variance = 0
    } else {
        variance /= (s.Count - 1)
    }
    return
}

func (s *BasicStats) StdDev() float64 {
    return math.Sqrt(s.Variance())
}


