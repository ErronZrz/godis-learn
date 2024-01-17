package timewheel

import "time"

var (
	w = NewTimeWheel(time.Second, 3600)
)

func init() {
	w.Start()
}

func JobWithDelay(job func(), key string, duration time.Duration) {
	w.AddJob(job, key, duration)
}

func JobAtTime(job func(), key string, t time.Time) {
	w.AddJob(job, key, t.Sub(time.Now()))
}

func Cancel(key string) {
	w.RemoveJob(key)
}
