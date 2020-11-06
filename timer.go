package pbft

import "time"

type timer struct {
	bft    *pbft
	c      chan Event
	timers map[string]*time.Timer
}

func newTimer(bft *pbft, c chan Event) *timer {
	return &timer{bft: bft, c: c, timers: make(map[string]*time.Timer)}
}

func (t *timer) ScheduleViewChange(digest string, newView uint64, duration time.Duration) {
	timer := t.timers[digest]
	if timer != nil {
		timer.Stop()
	}

	t.timers[digest] = time.AfterFunc(duration, func() {
		delete(t.timers, digest)
		t.c <- &ViewChangeEvent{NewView: newView}
	})
}

func (t *timer) ClearViewChange(digest string) {
	timer := t.timers[digest]
	if timer == nil {
		return
	}

	timer.Stop()
	delete(t.timers, digest)
}
