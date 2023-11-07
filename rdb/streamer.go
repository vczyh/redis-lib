package rdb

type EventStreamer struct {
	c   chan *eventWrapper
	o   *RedisRdbEvent
	err error
}

type eventWrapper struct {
	e   *RedisRdbEvent
	err error
}

func newEventStreamer(c chan *eventWrapper) *EventStreamer {
	return &EventStreamer{c: c}
}

func (s *EventStreamer) HasNext() bool {
	if s.err != nil {
		return false
	}

	w, ok := <-s.c
	if !ok {
		return false
	}
	if err := w.err; err != nil {
		s.err = err
		return false
	}
	s.o = w.e
	return true
}

func (s *EventStreamer) Next() *RedisRdbEvent {
	return s.o
}

func (s *EventStreamer) Err() error {
	return s.err
}
