package net

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlidingWindow_Quantiles(t *testing.T) {
	cases := []struct {
		size                      int
		data, quantiles, expected []float64
	}{
		{
			size:      10,
			data:      []float64{5, 1, 6, 2, 4, 9, 8, 7, 3},
			quantiles: []float64{1},
			expected:  []float64{9},
		},
		{
			size:      11,
			data:      []float64{5, 1, 6, 2, 4, 9, 8, 7, 3, 10},
			quantiles: []float64{0.5, 0.3, 0.8, 0.9},
			expected:  []float64{6, 4, 9, 10},
		},
		{
			size:      3,
			data:      []float64{5, 1, 6, 2, 4, 9, 8, 7, 3, 10},
			quantiles: []float64{0.49, 0.9},
			expected:  []float64{7, 10},
		},
		{
			size:      50,
			data:      []float64{5, 1, 2, 6, 4, 3},
			quantiles: []float64{0.49, 0.3, 0.7, 0.9},
			expected:  []float64{4, 3, 5, 6},
		},
		{
			size:      1,
			data:      []float64{5, 1, 6, 2, 4, 9, 8, 7, 3},
			quantiles: []float64{0.5, 0.3, 0.8, 0.9},
			expected:  []float64{3, 3, 3, 3}, // yeah, it's fine
		},
		{
			size:      100,
			data:      []float64{},
			quantiles: []float64{0.5, 0.3, 0.8, 0.9},
			expected:  nil,
		},
	}

	for _, tt := range cases {
		w := NewSlidingWindow(tt.size)
		for _, x := range tt.data {
			w.Push(x)
		}

		result := w.Quantiles(tt.quantiles)
		assert.Equal(t, tt.expected, result)
	}
}
