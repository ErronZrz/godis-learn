package timewheel

import (
	"container/list"
	"godis-instruction/lib/logger"
	"time"
)

type location struct {
	slot        int
	taskElement *list.Element
}

type task struct {
	delay   time.Duration
	circles int
	key     string
	job     func()
}

type TimeWheel struct {
	interval       time.Duration
	ticker         *time.Ticker
	slots          []*list.List
	posMap         map[string]*location
	currentPos     int
	nSlots         int
	insertTaskChan chan *task
	removeTaskChan chan string
	abortChan      chan bool
}

func NewTimeWheel(interval time.Duration, nSlots int) *TimeWheel {
	if interval <= 0 || nSlots <= 0 {
		return nil
	}
	w := &TimeWheel{
		interval:       interval,
		slots:          make([]*list.List, nSlots),
		posMap:         make(map[string]*location),
		currentPos:     0,
		nSlots:         nSlots,
		insertTaskChan: make(chan *task),
		removeTaskChan: make(chan string),
		abortChan:      make(chan bool),
	}
	for i := 0; i < nSlots; i++ {
		w.slots[i] = list.New()
	}
	return w
}

func (w *TimeWheel) Start() {
	w.ticker = time.NewTicker(w.interval)
	go w.start()
}

func (w *TimeWheel) Abort() {
	w.abortChan <- true
}

func (w *TimeWheel) AddJob(job func(), key string, delay time.Duration) {
	if delay < 0 {
		return
	}
	w.insertTaskChan <- &task{
		delay: delay,
		key:   key,
		job:   job,
	}
}

func (w *TimeWheel) RemoveJob(key string) {
	if key != "" {
		w.removeTaskChan <- key
	}
}

func (w *TimeWheel) start() {
	for {
		select {
		case <-w.ticker.C:
			w.tickHandler()
		case t := <-w.insertTaskChan:
			w.insertTask(t)
		case key := <-w.removeTaskChan:
			w.removeTask(key)
		case <-w.abortChan:
			w.ticker.Stop()
			break
		}
	}
}

func (w *TimeWheel) tickHandler() {
	l := w.slots[w.currentPos]
	w.currentPos++
	if w.currentPos == w.nSlots {
		w.currentPos = 0
	}
	go w.traverseTaskList(l)
}

func (w *TimeWheel) insertTask(t *task) {
	pos, circles := w.getPosAndCircles(t.delay)
	t.circles = circles
	e := w.slots[pos].PushBack(t)
	loc := &location{
		slot:        pos,
		taskElement: e,
	}
	if t.key == "" {
		_, ok := w.posMap[t.key]
		if ok {
			w.removeTask(t.key)
		}
	}
	w.posMap[t.key] = loc
}

func (w *TimeWheel) traverseTaskList(l *list.List) {
	for e := l.Front(); e != nil; {
		t := e.Value.(*task)
		if t.circles > 0 {
			t.circles--
			e = e.Next()
			continue
		}
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			t.job()
		}()
		next := e.Next()
		l.Remove(e)
		if t.key != "" {
			delete(w.posMap, t.key)
		}
		e = next
	}
}

func (w *TimeWheel) getPosAndCircles(d time.Duration) (pos, circles int) {
	steps := int(d.Seconds()) / int(w.interval.Seconds())
	pos = (w.currentPos + steps) % w.nSlots
	circles = steps / w.nSlots
	return
}

func (w *TimeWheel) removeTask(key string) {
	pos, ok := w.posMap[key]
	if !ok {
		return
	}
	l := w.slots[pos.slot]
	l.Remove(pos.taskElement)
	delete(w.posMap, key)
}
