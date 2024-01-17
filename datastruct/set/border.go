package set

import (
	"errors"
	"strconv"
)

const (
	negativeInfinity int8 = -1
	positiveInfinity int8 = 1
)

type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

var (
	negInfBorder = &ScoreBorder{Inf: negativeInfinity}
	posInfBorder = &ScoreBorder{Inf: positiveInfinity}
)

func (b *ScoreBorder) greaterThan(val float64) bool {
	if b.Inf != 0 {
		return b.Inf > 0
	}
	if b.Value == val {
		return !b.Exclude
	}
	return b.Value > val
}

func (b *ScoreBorder) lessThan(val float64) bool {
	if b.Inf != 0 {
		return b.Inf < 0
	}
	if b.Value == val {
		return !b.Exclude
	}
	return b.Value < val
}

func BorderFromString(s string) (*ScoreBorder, error) {
	if s == "inf" || s == "+inf" {
		return posInfBorder, nil
	} else if s == "-inf" {
		return negInfBorder, nil
	}
	exclude := s[0] == '('
	if exclude {
		s = s[1:]
	}
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("ERR min or max is not a float")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   val,
		Exclude: exclude,
	}, nil
}
