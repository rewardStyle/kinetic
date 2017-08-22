package kinetic

import (
	"container/list"
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// checkpointElement is the struct used to store checkpointing information.
type checkpointElement struct {
	seqNum int
	done   bool
}

// checkpointList is a doubly linked list used to track sequence numbers.  Sequence numbers are linked in ascending
// order.
type checkpointList struct {
	*list.List
}

// insert is a method used to insert sequence numbers (ordered) into the doubly linked list.  insert is optimized to
// insert from the back, so inserting in sequential or ascending order is faster than reverse / descending order.
func (c *checkpointList) insert(seqNum int) error {
	value := &checkpointElement{seqNum: seqNum}
	for e := c.Back(); e != nil; e = e.Prev() {
		element := e.Value.(*checkpointElement)
		switch {
		case element.seqNum < seqNum:
			c.InsertAfter(value, e)
			return nil
		case element.seqNum == seqNum:
			return nil
		case element.seqNum > seqNum:
			if e.Prev() != nil && e.Prev().Value.(*checkpointElement).seqNum < seqNum {
				c.InsertBefore(value, e)
				return nil
			}
		}
	}

	c.PushFront(value)
	return nil
}

// find is a method used to retreive the element in the doubly linked list for a given sequence number.  find is
// optimized for searching oldest numbers first as it traverse the linked list starting from the beginning.
func (c *checkpointList) find(seqNum int) (*list.Element, bool) {
	for e := c.Front(); e != nil; e = e.Next() {
		element := e.Value.(*checkpointElement)
		switch {
		case element.seqNum == seqNum:
			return e, true
		case element.seqNum < seqNum:
			break
		default:
			return nil, false
		}
	}
	return nil, false
}

// checkpointOptions is a struct containing all of the configurable options for a checkpoint object.
type checkpointOptions struct {
	autoCheckpointCount int                           // count of newly inserted messages before triggering an automatic checkpoint call
	autoCheckpointFreq  time.Duration                 // frequency with which to automatically call checkpoint
	checkpointFn        func(checkpoint string) error // callback function to call on a sequence number when a checkpoint is discovered with the checkpoint call
	countCheckFreq      time.Duration                 // frequency with which to check the insert count
}

// defaultCheckpointOptions is a function that returns a pointer to a checkpointOptions object with default values.
func defaultCheckpointOptions() *checkpointOptions {
	return &checkpointOptions{
		autoCheckpointCount: 10000,
		autoCheckpointFreq:  time.Minute,
		checkpointFn:        func(string) error { return nil },
		countCheckFreq:      time.Second,
	}
}

// checkpointOptionsFn is a function signature used to define function options for configuring all of the
// configurable options of a checkpoint object.
type checkpointOptionsFn func(*checkpointOptions) error

// checkpointAutoCheckpointCount is a functional option method for configuring the checkpoint's auto checkpoint count.
func checkpointAutoCheckpointCount(count int) checkpointOptionsFn {
	return func(o *checkpointOptions) error {
		o.autoCheckpointCount = count
		return nil
	}
}

// checkpointAutoCheckpointFreq is a functional option method for configuring the checkpoint's auto checkpoint
// frequency.
func checkpointAutoCheckpointFreq(freq time.Duration) checkpointOptionsFn {
	return func(o *checkpointOptions) error {
		o.autoCheckpointFreq = freq
		return nil
	}
}

// checkpointCheckpointFn is a functional option method for configuring the checkpoint's checkpoint callback function.
func checkpointCheckpointFn(fn func(string) error) checkpointOptionsFn {
	return func(o *checkpointOptions) error {
		o.checkpointFn = fn
		return nil
	}
}

// checkpointCountCheckFreq is a functional option method for configuring the checkpoint's count check frequency.
func checkpointCountCheckFreq(freq time.Duration) checkpointOptionsFn {
	return func(o *checkpointOptions) error {
		o.countCheckFreq = freq
		return nil
	}
}

// checkpoint is a data structure that is used as a bookkeeping system to track the state of data processing
// for records pulled off of the consumer's message channel.  The check pointing system uses a binary search
// tree keyed off of the Kinesis sequence number of the record.  The sequence numbers should be inserted into the
// binary search tree using the insert() function after messages are pulled off of the consumer's message
// channel and should be marked done using the markDone() function after data processing is completed.  The
// checkpoint() can be called periodically which may trigger a checkpoint call to KCL if the oldest sequence
// numbers have been marked complete.  Call startup() to enable automatic checkpointing and expiration.
type checkpointer struct {
	*checkpointOptions // contains all of the configuration settings for the checkpoint object
	list               *checkpointList
	listMu             sync.Mutex
	counter            uint64     // counter to track the number of messages inserted since the last checkpoint
	checkpointCh       chan empty // channel with which to communicate / coordinate checkpointing
	startupOnce        sync.Once  // used to ensure that the startup function is called once
	shutdownOnce       sync.Once  // used to ensure that the shutdown function is called once
}

// newCheckpoint instantiates a new checkpoint object with default configuration settings unless the function option
// methods are provided to change the default values.
func newCheckpointer(optionFns ...checkpointOptionsFn) *checkpointer {
	checkpointOptions := defaultCheckpointOptions()
	for _, optionFn := range optionFns {
		optionFn(checkpointOptions)
	}
	return &checkpointer{
		checkpointOptions: checkpointOptions,
		list:              &checkpointList{list.New()},
	}
}

// startup is a method used to enable automatic checkpointing.
func (c *checkpointer) startup(ctx context.Context) {
	c.startupOnce.Do(func() {
		defer func() {
			c.shutdownOnce = sync.Once{}
		}()

		c.checkpointCh = make(chan empty)
		go func() {
			defer c.shutdown()

			autoCheckpointTimer := time.NewTimer(c.autoCheckpointFreq)
			counterCheckTicker := time.NewTicker(c.countCheckFreq)

			for {
			wait:
				for atomic.LoadUint64(&c.counter) < uint64(c.autoCheckpointCount) {
					select {
					case <-ctx.Done():
						autoCheckpointTimer.Stop()
						counterCheckTicker.Stop()
						return
					case <-c.checkpointCh:
						break wait
					case <-autoCheckpointTimer.C:
						break wait
					case <-counterCheckTicker.C:
						break
					}
				}

				// call check to obtain the checkpoint
				if cp, found := c.check(); found {
					c.checkpointFn(cp)
				}

				// reset counter and timer
				atomic.StoreUint64(&c.counter, 0)
				autoCheckpointTimer.Reset(c.autoCheckpointFreq)
			}
		}()
	})
}

// shutdown is a method used to clean up the checkpoint object
func (c *checkpointer) shutdown() {
	c.shutdownOnce.Do(func() {
		defer func() {
			c.startupOnce = sync.Once{}
		}()

		if c.checkpointCh != nil {
			close(c.checkpointCh)
		}
	})
}

// size safely determines the number of nodes in the checkpointer.
func (c *checkpointer) size() int {
	c.listMu.Lock()
	defer c.listMu.Unlock()

	return c.list.Len()
}

// insert safely inserts a sequence number into the binary search tree.
func (c *checkpointer) insert(seqNumStr string) error {
	c.listMu.Lock()
	defer c.listMu.Unlock()

	seqNum, err := strconv.Atoi(seqNumStr)
	if err != nil {
		return err
	}

	if err := c.list.insert(seqNum); err != nil {
		return err
	}

	atomic.AddUint64(&c.counter, 1)
	return nil
}

// markDone safely marks the given sequence number as done.
func (c *checkpointer) markDone(seqNumStr string) error {
	c.listMu.Lock()
	defer c.listMu.Unlock()

	seqNum, err := strconv.Atoi(seqNumStr)
	if err != nil {
		return err
	}

	e, ok := c.list.find(seqNum)
	if !ok {
		return errors.New("Sequence number not found")
	}

	e.Value.(*checkpointElement).done = true
	if e.Prev() != nil && e.Prev().Value.(*checkpointElement).done {
		c.list.Remove(e.Prev())
	}
	if e.Next() != nil && e.Next().Value.(*checkpointElement).done {
		c.list.Remove(e)
	}

	return nil
}

// check returns the largest sequence number marked as done where all smaller sequence numbers have
// also been marked as done.
func (c *checkpointer) check() (checkpoint string, found bool) {
	c.listMu.Lock()
	defer c.listMu.Unlock()

	if c.list != nil {
		for e := c.list.Front(); e != nil; e = e.Next() {
			checkpointElement := e.Value.(*checkpointElement)
			if !checkpointElement.done {
				break
			} else {
				checkpoint = strconv.Itoa(checkpointElement.seqNum)
				found = true
			}
		}
	}

	return checkpoint, found
}

// checkpoint sends a signal to the checkpointCh channel to trigger a call to checkpoint (potentially).
func (c *checkpointer) checkpoint() error {
	if c.checkpointCh == nil {
		return errors.New("Nil checkpoint channel")
	}
	c.checkpointCh <- empty{}
	return nil
}
