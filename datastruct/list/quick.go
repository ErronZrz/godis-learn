package list

import "container/list"

const (
	pageSize = 1 << 10
)

type iterator struct {
	cur     *list.Element
	offset  int
	listPtr *QuickList
}

type QuickList struct {
	data *list.List
	size int
}

func NewQuickList() *QuickList {
	return &QuickList{
		data: list.New(),
		size: 0,
	}
}

func (l *QuickList) Size() int {
	return l.size
}

func (l *QuickList) Add(val any) {
	l.size++
	if l.data.Len() == 0 {
		l.addNewPage(val)
		return
	}
	lastNode := l.data.Back()
	lastPage := lastNode.Value.([]any)
	if len(lastPage) == pageSize {
		l.addNewPage(val)
		return
	}
	lastNode.Value = append(lastPage, val)
}

func (l *QuickList) Insert(index int, val any) {
	if index == l.size {
		l.Add(val)
		return
	}
	iter := l.iteratorAt(index)
	l.size++
	page := iter.cur.Value.([]any)
	i := iter.offset
	if len(page) < pageSize {
		page = append(page[:i+1], page[i:]...)
		page[i] = val
		iter.cur.Value = page
		return
	}
	halfSize := pageSize >> 1
	nextPage := page[halfSize:]
	page = page[:halfSize]
	if iter.offset < len(page) {
		page = append(page[:i+1], page[i:]...)
		page[i] = val
	} else {
		i -= halfSize
		nextPage = append(nextPage[:i+1], nextPage[i:]...)
		nextPage[i] = val
	}
	iter.cur.Value = page
	l.data.InsertAfter(nextPage, iter.cur)
}

func (l *QuickList) Set(index int, val any) {
	l.iteratorAt(index).set(val)
}

func (l *QuickList) Get(index int) any {
	return l.iteratorAt(index).get()
}

func (l *QuickList) Count(equals EqualsFunc) int {
	res := 0
	l.ForEach(func(i int, val any) bool {
		if equals(val) {
			res++
		}
		return true
	})
	return res
}

func (l *QuickList) Remove(index int) any {
	return l.iteratorAt(index).remove()
}

func (l *QuickList) RemoveFirst(equals EqualsFunc) (found bool) {
	if l.size == 0 {
		return false
	}
	iter := l.iteratorAt(0)
	for {
		if equals(iter.get()) {
			iter.remove()
			return true
		}
		if !iter.next() {
			return false
		}
	}
}

func (l *QuickList) RemoveN(equals EqualsFunc, n int) int {
	if l.size == 0 || n < 1 {
		return 0
	}
	iter := l.iteratorAt(0)
	res := 0
	for {
		if equals(iter.get()) {
			iter.remove()
			res++
			if res == n || iter.cur == nil || iter.offset == len(iter.page()) {
				return res
			}
		} else if !iter.next() {
			return res
		}
	}
}

func (l *QuickList) RemoveAll(equals EqualsFunc) int {
	return l.RemoveN(equals, l.size)
}

func (l *QuickList) RemoveLastN(equals EqualsFunc, n int) int {
	if l.size == 0 || n < 1 {
		return 0
	}
	iter := l.iteratorAt(l.size - 1)
	res := 0
	for {
		if equals(iter.get()) {
			iter.remove()
			res++
			if res == n || iter.cur == nil {
				return res
			}
		}
		if !iter.prev() {
			return res
		}
	}
}

func (l *QuickList) ForEach(c Consumer) {
	if l == nil {
		panic("QuickList is nil")
	}
	if l.size == 0 {
		return
	}
	iter := l.iteratorAt(0)
	i := 0
	for {
		if !c(i, iter.get()) {
			break
		}
		i++
		if !iter.next() {
			break
		}
	}
}

func (l *QuickList) GetSlice(begin, end int) []any {
	if l == nil {
		panic("QuickList is nil")
	}
	if begin >= end {
		panic("Begin not less than end")
	}
	if begin < 0 || end > l.size {
		panic("Argument out of bounds")
	}
	len_ := end - begin
	res := make([]any, 0, len_)
	iter := l.iteratorAt(begin)
	i := 0
	for i < len_ {
		res = append(res, iter.get())
		iter.next()
		i++
	}
	return res
}

func (l *QuickList) addNewPage(val any) {
	page := make([]any, 0, pageSize)
	page[0] = val
	l.data.PushBack(page)
}

func (l *QuickList) iteratorAt(index int) *iterator {
	if l == nil {
		panic("QuickList is nil")
	}
	if index < 0 || index >= l.size {
		panic("Index out of bounds")
	}
	var (
		e       *list.Element
		page    []any
		pageBeg int
	)
	if index < l.size>>1 {
		e = l.data.Front()
		pageBeg = 0
		for {
			page = e.Value.([]any)
			if pageBeg+len(page) > index {
				break
			}
			pageBeg += len(page)
			e = e.Next()
		}
	} else {
		e = l.data.Back()
		pageBeg = l.size
		for {
			page = e.Value.([]any)
			pageBeg -= len(page)
			if pageBeg <= index {
				break
			}
			e = e.Prev()
		}
	}
	pageOffset := index - pageBeg
	return &iterator{
		cur:     e,
		offset:  pageOffset,
		listPtr: l,
	}
}

func (it *iterator) remove() any {
	page := it.page()
	val := page[it.offset]
	page = append(page[:it.offset], page[it.offset+1:]...)
	it.listPtr.size--
	if len(page) > 0 {
		it.cur.Value = page
		if it.offset == len(page) && it.cur != it.listPtr.data.Back() {
			it.cur = it.cur.Next()
			it.offset = 0
		}
		return val
	}
	// 如果移除的元素恰好是所在页唯一一个元素，需要删掉这一页
	if it.cur == it.listPtr.data.Back() {
		it.listPtr.data.Remove(it.cur)
		it.cur = nil
	} else {
		nextNode := it.cur.Next()
		it.listPtr.data.Remove(it.cur)
		it.cur = nextNode
	}
	it.offset = 0
	return val
}

func (it *iterator) next() bool {
	pageLen := len(it.page())
	if it.offset < pageLen-1 {
		it.offset++
		return true
	}
	if it.cur == it.listPtr.data.Back() {
		it.offset = pageLen
		return false
	}
	it.cur = it.cur.Next()
	it.offset = 0
	return true
}

func (it *iterator) prev() bool {
	if it.offset > 0 {
		it.offset--
		return true
	}
	if it.cur == it.listPtr.data.Front() {
		it.offset = -1
		return false
	}
	it.cur = it.cur.Prev()
	it.offset = len(it.page()) - 1
	return true
}

func (it *iterator) get() any {
	return it.page()[it.offset]
}

func (it *iterator) set(val any) {
	it.page()[it.offset] = val
}

func (it *iterator) page() []any {
	return it.cur.Value.([]any)
}
