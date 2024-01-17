package utils

import "math/rand"

func Xor(a, b bool) bool {
	return a && !b || !a && b
}

func BytesEqual(a, b []byte) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func AlnumString(l int) string {
	a := make([]byte, l)
	for i := 0; i < l; i++ {
		index := rand.Intn(62)
		if index < 10 {
			a[i] = byte(48 + index)
		} else if index < 36 {
			a[i] = byte(55 + index)
		} else {
			a[i] = byte(61 + index)
		}
	}
	return string(a)
}

func PotentialBytesEqual(a, b any) bool {
	sa, oa := a.([]byte)
	sb, ob := b.([]byte)
	if oa && ob {
		return BytesEqual(sa, sb)
	}
	return a == b
}

func StringsToLine(strs ...string) [][]byte {
	res := make([][]byte, len(strs))
	for i, str := range strs {
		res[i] = []byte(str)
	}
	return res
}

func StringsWithNameToLine(name string, strs [][]byte) [][]byte {
	res := make([][]byte, 1+len(strs))
	res[0] = []byte(name)
	for i, str := range strs {
		res[i+1] = str
	}
	return res
}
