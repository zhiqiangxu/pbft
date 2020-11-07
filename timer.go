package pbft

import "time"

type timer struct {
	bft      *pbft
	c        chan Event
	vcTimers map[string]*time.Timer
	hbTimer  *time.Timer
}

func newTimer(bft *pbft, c chan Event) *timer {
	return &timer{bft: bft, c: c, vcTimers: make(map[string]*time.Timer)}
}

func (t *timer) ScheduleHeartBeat(duration time.Duration) {
	if t.hbTimer != nil {
		t.hbTimer.Stop()
	}

	t.hbTimer = time.AfterFunc(duration, func() {
		t.c <- &HeartBeatEvent{}
		t.hbTimer.Reset(duration)
	})
}

func (t *timer) CancelHeartBeat() {
	if t.hbTimer != nil {
		t.hbTimer.Stop()
		t.hbTimer = nil
	}
}

func (t *timer) ScheduleViewChange(digest string, newView uint64, duration time.Duration) {
	timer := t.vcTimers[digest]
	if timer != nil {
		timer.Stop()
	}

	t.vcTimers[digest] = time.AfterFunc(duration, func() {
		delete(t.vcTimers, digest)
		t.c <- &ViewChangeEvent{NewView: newView}
	})
}

func (t *timer) CancelViewChange(digest string) {
	timer := t.vcTimers[digest]
	if timer == nil {
		return
	}

	timer.Stop()
	delete(t.vcTimers, digest)
}
