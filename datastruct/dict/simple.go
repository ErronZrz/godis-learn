package dict

type SimpleHashMap struct {
	data map[string]any
}

func NewSimpleHashMap() *SimpleHashMap {
	return &SimpleHashMap{data: make(map[string]any)}
}

func (m *SimpleHashMap) Size() int {
	if m.data == nil {
		panic("Nil map")
	}
	return len(m.data)
}

func (m *SimpleHashMap) Put(key string, value any) {
	if m.data == nil {
		panic("Nil map")
	}
	m.data[key] = value
}

func (m *SimpleHashMap) PutIfAbsent(key string, value any) (ok bool) {
	if m.data == nil {
		panic("Nil map")
	}
	_, exists := m.data[key]
	if !exists {
		m.data[key] = value
	}
	return !exists
}

func (m *SimpleHashMap) PutIfExists(key string, value any) (ok bool) {
	if m.data == nil {
		panic("Nil map")
	}
	_, exists := m.data[key]
	if exists {
		m.data[key] = value
	}
	return exists
}

func (m *SimpleHashMap) Get(key string) (value any, ok bool) {
	if m.data == nil {
		panic("Nil map")
	}
	value, ok = m.data[key]
	return
}

func (m *SimpleHashMap) Delete(key string) (ok bool) {
	if m.data == nil {
		panic("Nil map")
	}
	_, exists := m.data[key]
	if exists {
		delete(m.data, key)
	}
	return exists
}

func (m *SimpleHashMap) ForEach(p Processor) {
	if m.data == nil {
		panic("Nil map")
	}
	for k, v := range m.data {
		if !p(k, v) {
			break
		}
	}
}

func (m *SimpleHashMap) Keys() []string {
	if m.data == nil {
		panic("Nil map")
	}
	res := make([]string, m.Size())
	i := 0
	for key := range m.data {
		res[i] = key
		i++
	}
	return res
}

func (m *SimpleHashMap) RandomKeys(nKeys int) []string {
	if m.data == nil {
		panic("Nil map")
	}
	res := make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		for key := range m.data {
			res[i] = key
			break
		}
	}
	return res
}

func (m *SimpleHashMap) RandomDistinctKeys(nKeys int) []string {
	if m.data == nil {
		panic("Nil map")
	}
	if nKeys >= m.Size() {
		return m.Keys()
	}
	res := make([]string, nKeys)
	i := 0
	for key := range m.data {
		res[i] = key
		i++
		if i == nKeys {
			break
		}
	}
	return res
}

func (m *SimpleHashMap) Clear() {
	*m = *NewSimpleHashMap()
}
