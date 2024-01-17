package dict

type Processor func(string, any) bool

type HashMap interface {
	Size() int
	Put(key string, value any)
	PutIfAbsent(key string, value any) (ok bool)
	PutIfExists(key string, value any) (ok bool)
	Get(key string) (value any, ok bool)
	Delete(key string) (ok bool)
	ForEach(p Processor)
	Keys() []string
	RandomKeys(nKeys int) []string
	RandomDistinctKeys(nKeys int) []string
	Clear()
}
