package list

type EqualsFunc func(any) bool

type Consumer func(int, any) bool

type List interface {
	Size() int
	Add(val any)
	Insert(index int, val any)
	Set(index int, val any)
	Get(index int) any
	Count(equals EqualsFunc) int
	Remove(index int) any
	RemoveFirst(equals EqualsFunc) (found bool)
	RemoveN(equals EqualsFunc, n int) int
	RemoveAll(equals EqualsFunc) int
	RemoveLastN(equals EqualsFunc, n int) int
	ForEach(c Consumer)
	GetSlice(begin, end int) []any
}
