package list

type node struct {
	val  any
	prev *node
	next *node
}

type LinkedList struct {
	head *node
	tail *node
	size int
}

func NewLinkedList(l []any) *LinkedList {
	res := &LinkedList{}
	if l != nil {
		for _, val := range l {
			res.Add(val)
		}
	}
	return res
}

func (l *LinkedList) Size() int {
	if l == nil {
		panic("LinkedList is nil")
	}
	return l.size
}

func (l *LinkedList) Add(val any) {
	if l == nil {
		panic("LinkedList is nil")
	}
	n := &node{val: val}
	if l.tail == nil {
		l.head, l.tail = n, n
	} else {
		n.prev = l.tail
		l.tail.next, l.tail = n, n
	}
	l.size++
}

func (l *LinkedList) Insert(index int, val any) {
	if l == nil {
		panic("LinkedList is nil")
	}
	if index < 0 || index > l.size {
		panic("Index out of bounds")
	}
	if index == l.size {
		l.Add(val)
		return
	}
	replaced := l.at(index)
	n := &node{
		val:  val,
		prev: replaced.prev,
		next: replaced,
	}
	if n.prev == nil {
		l.head = n
	} else {
		l.head.next = n
	}
	replaced.prev = n
	l.size++
}

func (l *LinkedList) Set(index int, val any) {
	if l == nil {
		panic("LinkedList is nil")
	}
	if index < 0 || index > l.size {
		panic("Index out of bounds")
	}
	l.at(index).val = val
}

func (l *LinkedList) Get(index int) any {
	if l == nil {
		panic("LinkedList is nil")
	}
	if index < 0 || index > l.size {
		panic("Index out of bounds")
	}
	return l.at(index).val
}

func (l *LinkedList) Count(equals EqualsFunc) int {
	if l == nil {
		panic("LinkedList is nil")
	}
	now, tmp, res := l.head, l.head, 0
	for now != nil {
		tmp = now.next
		if equals(now.val) {
			l.remove(now)
			res++
		}
		now = tmp
	}
	return res
}

func (l *LinkedList) Remove(index int) any {
	if l == nil {
		panic("LinkedList is nil")
	}
	if index < 0 || index > l.size {
		panic("Index out of bounds")
	}
	n := l.at(index)
	l.remove(n)
	return n.val
}

func (l *LinkedList) RemoveLast() any {
	if l == nil {
		panic("LinkedList is nil")
	}
	if l.tail == nil {
		return nil
	}
	n := l.tail
	l.remove(n)
	return n
}

func (l *LinkedList) RemoveFirst(equals EqualsFunc) (found bool) {
	if l == nil {
		panic("LinkedList is nil")
	}
	n := l.head
	for n != nil {
		if equals(n.val) {
			l.remove(n)
			return true
		}
		n = n.next
	}
	return
}

func (l *LinkedList) RemoveN(equals EqualsFunc, n int) int {
	if l == nil {
		panic("LinkedList is nil")
	}
	if n < 1 {
		return 0
	}
	now, tmp, res := l.head, l.head, 0
	for now != nil {
		tmp = now.next
		if equals(now.val) {
			l.remove(now)
			res++
		}
		if res == n {
			break
		}
		now = tmp
	}
	return res
}

func (l *LinkedList) RemoveAll(equals EqualsFunc) int {
	return l.RemoveN(equals, l.size)
}

func (l *LinkedList) RemoveLastN(equals EqualsFunc, n int) int {
	if l == nil {
		panic("LinkedList is nil")
	}
	if n < 1 {
		return 0
	}
	now, tmp, res := l.tail, l.tail, 0
	for now != nil {
		tmp = now.prev
		if equals(now.val) {
			l.remove(now)
			res++
		}
		if res == n {
			break
		}
		now = tmp
	}
	return res
}

func (l *LinkedList) ForEach(c Consumer) {
	if l == nil {
		panic("LinkedList is nil")
	}
	n, i := l.head, 0
	for n != nil {
		if !c(i, n.val) {
			break
		}
		i++
		n = n.next
	}
}

func (l *LinkedList) GetSlice(begin, end int) []any {
	if l == nil {
		panic("LinkedList is nil")
	}
	if begin >= end {
		panic("Begin not less than end")
	}
	if begin < 0 || end > l.size {
		panic("Argument out of bounds")
	}
	_len, n := end-begin, l.head
	res := make([]any, _len)
	for i := 0; i < begin; i++ {
		n = n.next
	}
	for i := 0; i < _len; i++ {
		res[i] = n.val
		n = n.next
	}
	return res
}

func (l *LinkedList) at(index int) *node {
	if index < l.size>>1 {
		res := l.head
		for i := 0; i < index; i++ {
			res = res.next
		}
		return res
	}
	res := l.tail
	for i := l.size - 1; i > index; i++ {
		res = res.prev
	}
	return res
}

func (l *LinkedList) remove(n *node) {
	if n.prev == nil {
		l.head = n.next
	} else {
		n.prev.next = n.next
	}
	if n.next == nil {
		l.tail = n.prev
	} else {
		n.next.prev = n.prev
	}
	n.prev, n.next = nil, nil
	l.size--
}
