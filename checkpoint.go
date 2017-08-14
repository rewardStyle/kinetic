package kinetic

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// btreeNode is a binary tree node used to encapsulate checkpointing information in a binary search tree.
type btreeNode struct {
	seqNum int        // key to store the sequence number in the checkpointing system
	done   bool       // flag to indicate whether or not processing of a record is complete
	age    time.Time  // time stamp used to expire btreeNodes from the checkpointing system
	left   *btreeNode // pointer to a btreeNode whose sequence number is less than own
	right  *btreeNode // pointer to a btreeNode whose sequence number is greater than own
}

// insert is a (recursive) method used to insert sequence numbers into the binary search tree.
func (n *btreeNode) insert(seqNum int) error {
	if n == nil {
		return errors.New("Cannot call insert on a nil node")
	}

	switch {
	case seqNum == n.seqNum:
		return nil
	case seqNum < n.seqNum:
		if n.left == nil {
			n.left = &btreeNode{
				seqNum: seqNum,
				age:    time.Now(),
			}
			return nil
		}
		return n.left.insert(seqNum)
	case seqNum > n.seqNum:
		if n.right == nil {
			n.right = &btreeNode{
				seqNum: seqNum,
				age:    time.Now(),
			}
			return nil
		}
		return n.right.insert(seqNum)
	}
	return nil
}

// find is a (recursive) method used to retrieve a pointer to the btreeNode containing the sequence number or 'false'
// if the sequence number provided is not found.
func (n *btreeNode) find(seqNum int) (*btreeNode, bool) {
	if n == nil {
		return nil, false
	}

	switch {
	case seqNum == n.seqNum:
		return n, true
	case seqNum < n.seqNum:
		return n.left.find(seqNum)
	default:
		return n.right.find(seqNum)
	}
}

// findMin is a helper function used to find the minimum element in the subtree of the given parent node.
func (n *btreeNode) findMin(parent *btreeNode) (*btreeNode, *btreeNode) {
	if n.left == nil {
		return n, parent
	}
	return n.left.findMin(n)
}

// findMax is a helper function used to find the maximum element in the subtree of the given parent node.
func (n *btreeNode) findMax(parent *btreeNode) (*btreeNode, *btreeNode) {
	if n.right == nil {
		return n, parent
	}
	return n.right.findMax(n)
}

// replaceNode is a helper method used to replace btreeNodes with a binary search tree.
func (n *btreeNode) replaceNode(parent, replacement *btreeNode) error {
	if n == nil {
		return errors.New("Cannot call replaceNode on a nil node")
	}

	if n == parent.left {
		parent.left = replacement
		return nil
	}
	parent.right = replacement
	return nil
}

// delete is a (recursive) method used remove (and re-organize if necessary) a btreeNode from the binary search tree.
func (n *btreeNode) delete(seqNum int, parent *btreeNode) error {
	if n == nil {
		return errors.New("Cannot call delete on a nil node")
	}

	switch {
	case seqNum < n.seqNum:
		return n.left.delete(seqNum, n)
	case seqNum > n.seqNum:
		return n.right.delete(seqNum, n)
	default:
		if n.left == nil && n.right == nil {
			n.replaceNode(parent, nil)
			return nil
		}
		if n.left == nil {
			n.replaceNode(parent, n.right)
			return nil
		}
		if n.right == nil {
			n.replaceNode(parent, n.left)
			return nil
		}

		replacement, replacementParent := n.left.findMax(n)
		n.seqNum = replacement.seqNum
		n.done = replacement.done
		n.age = replacement.age

		return replacement.delete(replacement.seqNum, replacementParent)
	}
}

// markDone is a method used to change the 'done' flag of a btreeNode to 'true'.
func (n *btreeNode) markDone() {
	n.done = true
}

// btree is a binary search tree.
type btree struct {
	root *btreeNode // the root node for the binary search tree
}

// insert is a method used to insert sequence numbers into the binary search tree of the checkpointing system.
func (t *btree) insert(seqNum int) error {
	if t.root == nil {
		t.root = &btreeNode{
			seqNum: seqNum,
			age:    time.Now(),
		}
		return nil
	}
	return t.root.insert(seqNum)
}

// find is a method used retrieve a pointer to a btreeNode of the given sequence number or 'false' if the sequence
// number was not found.
func (t *btree) find(seqNum int) (*btreeNode, bool) {
	if t.root == nil {
		return nil, false
	}
	return t.root.find(seqNum)
}

// delete is a method used to remove a btreeNode with the given sequence number from the binary search tree.
func (t *btree) delete(seqNum int) error {
	if t.root == nil {
		return errors.New("Cannot call delete on a nil tree")
	}

	if t.root.seqNum == seqNum {
		fakeParent := &btreeNode{right: t.root}
		if err := t.root.delete(seqNum, fakeParent); err != nil {
			return err
		}
		t.root = fakeParent.right
		return nil
	}
	return t.root.delete(seqNum, nil)
}

// traverseOrdered is a (recursive) method used to apply a callback function on all of the nodes in the binary
// search tree ordered by sequence number from smallest to largest.
func (t *btree) traverseOrdered(n *btreeNode, fn func(*btreeNode)) {
	if n == nil {
		return
	}

	t.traverseOrdered(n.left, fn)
	fn(n)
	t.traverseOrdered(n.right, fn)
}

// traverseChildren is a (recursive) method used to apply a callback function on all of children nodes in the
// binary search tree.
func (t *btree) traverseChildren(n *btreeNode, fn func(*btreeNode)) {
	if n == nil {
		return
	}

	t.traverseChildren(n.left, fn)
	t.traverseChildren(n.right, fn)
	fn(n)
}

// size is a method used to determine the number of btreeNodes in the binary search tree.
func (t *btree) size() int {
	var size int
	if t.root != nil {
		t.traverseOrdered(t.root, func(n *btreeNode) {
			size++
		})
	}
	return size
}

// minNode is a method used to retrieve a pointer to the btreeNode containing the smallest sequence number in the
// binary search tree.
func (t *btree) minNode() *btreeNode {
	if t.root == nil {
		return nil
	}
	node, _ := t.root.findMin(t.root)
	return node
}

// maxNode is a method used to retrieve a pointer to the btreeNode containing the largest sequence number in the
// binary search tree.
func (t *btree) maxNode() *btreeNode {
	if t.root == nil {
		return nil
	}
	node, _ := t.root.findMax(t.root)
	return node
}

// trim is a method used to trim the binary search tree so that the sequence number of all of the remaining nodes
// fall within (non-inclusive) the minSeqNum and maxSeqNum values.
func (t *btree) trim(minSeqNum, maxSeqNum *int) error {
	var min, max int
	if minSeqNum == nil {
		min = math.MinInt64
	} else {
		min = *minSeqNum
	}
	if maxSeqNum == nil {
		max = math.MaxInt64
	} else {
		max = *maxSeqNum
	}

	fakeParent := &btreeNode{right: t.root}
	t.traverseChildren(fakeParent, func(n *btreeNode) {
		if n.left != nil && (n.left.seqNum <= min || n.left.seqNum >= max) {
			n.delete(n.left.seqNum, n)
		}
		if n.right != nil && (n.right.seqNum <= min || n.right.seqNum >= max) {
			n.delete(n.right.seqNum, n)
		}
	})
	t.root = fakeParent.right

	return nil
}

// expire is a method used to delete btreeNodes from the binary search tree whose age is older than the given age.
func (t *btree) expire(age time.Duration) error {
	fakeParent := &btreeNode{right: t.root}
	t.traverseChildren(fakeParent, func(n *btreeNode) {
		if n.left != nil && time.Since(n.left.age) > age {
			n.delete(n.left.seqNum, n)
		}
		if n.right != nil && time.Since(n.right.age) > age {
			n.delete(n.right.seqNum, n)
		}
	})
	t.root = fakeParent.right

	return nil
}

// checkpointOptions is a struct containing all of the configurable options for a checkpoint object.
type checkpointOptions struct {
	autoCheckpointCount int                           // count of newly inserted messages before triggering an automatic checkpoint call
	autoCheckpointFreq  time.Duration                 // frequency with which to automatically call checkpoint
	maxAge              time.Duration                 // maximum duration for an inserted sequence number to live before being expired
	maxSize             int                           // maximum capacity (number of btreeNodes) of the checkpoint system
	checkpointFn        func(checkpoint string) error // callback function to call on a sequence number when a checkpoint is discovered with the checkpoint call
	expireFn            func(checkpoint string) error // callback function to call when a sequence number is aged out
	capacityFn          func(checkpoint string) error // callback function to call when the checkpoint system has reached max capacity
	countCheckFreq      time.Duration                 // frequency with which to check the insert count
}

// defaultCheckpointOptions is a function that returns a pointer to a checkpointOptions object with default values.
func defaultCheckpointOptions() *checkpointOptions {
	return &checkpointOptions{
		autoCheckpointCount: 10000,
		autoCheckpointFreq:  time.Minute,
		maxAge:              time.Hour,
		maxSize:             1000000,
		checkpointFn:        func(string) error { return nil },
		expireFn:            func(string) error { return nil },
		capacityFn:          func(string) error { return nil },
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

// checkpointMaxAge is a functional option method for configuring the checkpoint's maximum age.
func checkpointMaxAge(age time.Duration) checkpointOptionsFn {
	return func(o *checkpointOptions) error {
		o.maxAge = age
		return nil
	}
}

// checkpointMaxSize is a functional option method for configuring the checkpoint's maximum size.
func checkpointMaxSize(size int) checkpointOptionsFn {
	return func(o *checkpointOptions) error {
		o.maxSize = size
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

// checkpointExpireFn is a functional option method for configuring the checkpoint's expire callback function.
func checkpointExpireFn(fn func(string) error) checkpointOptionsFn {
	return func(o *checkpointOptions) error {
		o.expireFn = fn
		return nil
	}
}

// checkpointCapacityFn is a functional option method for configuring the checkpoint's capacity callback function.
func checkpointCapacityFn(fn func(string) error) checkpointOptionsFn {
	return func(o *checkpointOptions) error {
		o.capacityFn = fn
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
type checkpoint struct {
	*checkpointOptions               // contains all of the configuration settings for the checkpoint object
	keys               *btree        // binary search tree used to store Kinesis record sequence numbers
	keysMu             sync.Mutex    // mutex to make keys thread safe
	counter            uint64        // counter to track the number of messages inserted since the last checkpoint
	checkpointCh       chan struct{} // channel with which to communicate / coordinate checkpointing
	startupOnce        sync.Once     // used to ensure that the startup function is called once
	shutdownOnce       sync.Once     // used to ensure that the shutdown function is called once
}

// newCheckpoint instantiates a new checkpoint object with default configuration settings unless the function option
// methods are provided to change the default values.
func newCheckpoint(optionFns ...checkpointOptionsFn) *checkpoint {
	checkpointOptions := defaultCheckpointOptions()
	for _, optionFn := range optionFns {
		optionFn(checkpointOptions)
	}
	return &checkpoint{
		checkpointOptions: checkpointOptions,
		keys:              &btree{},
	}
}

// startup is a method used to enable automatic checkpointing and expiration of btreeNodes from the
// checkpointing system.
func (c *checkpoint) startup(ctx context.Context) {
	c.startupOnce.Do(func() {
		defer func() {
			c.shutdownOnce = sync.Once{}
		}()

		c.checkpointCh = make(chan struct{})
		go func() {
			defer c.shutdown()

			autoCheckpointTimer := time.NewTimer(c.autoCheckpointFreq)
			expirationTicker := time.NewTicker(c.maxAge)
			counterCheckTicker := time.NewTicker(c.countCheckFreq)

			for {
			wait:
				for atomic.LoadUint64(&c.counter) < uint64(c.autoCheckpointCount) {
					select {
					case <-ctx.Done():
						autoCheckpointTimer.Stop()
						expirationTicker.Stop()
						counterCheckTicker.Stop()
						return
					case <-c.checkpointCh:
						break wait
					case <-autoCheckpointTimer.C:
						break wait
					case <-expirationTicker.C:
						c.expire(c.maxAge)
						break
					case <-counterCheckTicker.C:
						break
					}
				}

				// call check to obtain the checkpoint
				cp, found := c.check()
				if found {
					c.checkpointFn(cp)
					c.trim(cp)
				}

				// reset counter and timer
				atomic.StoreUint64(&c.counter, 0)
				autoCheckpointTimer.Reset(c.autoCheckpointFreq)
			}
		}()
	})
}

// shutdown is a method used to clean up the checkpoint object
func (c *checkpoint) shutdown() {
	c.shutdownOnce.Do(func() {
		defer func() {
			c.startupOnce = sync.Once{}
		}()

		if c.checkpointCh != nil {
			close(c.checkpointCh)
		}
	})
}

// insert safely inserts a sequence number into the binary search tree.
func (c *checkpoint) insert(seqNumStr string) error {
	c.keysMu.Lock()
	defer c.keysMu.Unlock()

	if c.keys.size() >= c.maxSize {
		c.capacityFn(seqNumStr)
		return errors.New("Unable to insert due to capacity")
	}

	if seqNum, err := strconv.Atoi(seqNumStr); err == nil {
		err := c.keys.insert(seqNum)
		if err != nil {
			return err
		}
	}

	atomic.AddUint64(&c.counter, 1)
	return nil
}

// markDone safely marks the given sequence number as done.
func (c *checkpoint) markDone(seqNumStr string) error {
	c.keysMu.Lock()
	defer c.keysMu.Unlock()

	seqNum, err := strconv.Atoi(seqNumStr)
	if err != nil {
		return err
	}

	node, ok := c.keys.find(seqNum)
	if !ok {
		return errors.New("Sequence number not found")
	}

	node.markDone()
	return nil
}

// check returns the largest sequence number marked as done where all smaller sequence numbers have
// also been marked as done.
func (c *checkpoint) check() (checkpoint string, found bool) {
	c.keysMu.Lock()
	defer c.keysMu.Unlock()

	if c.keys == nil {
		return "", found
	}

	var stop bool // flag to stop applying function in traverse
	c.keys.traverseOrdered(c.keys.root, func(n *btreeNode) {
		if stop {
			return
		}

		if n.left == nil && !n.done {
			stop = true
			return
		} else if !n.done {
			stop = true
			return
		}
		checkpoint = strconv.Itoa(n.seqNum)
		found = true
	})
	return checkpoint, found
}

// trim safely simplifies the binary tree housing the sequence numbers so that only the smallest element is
// marked as done.
func (c *checkpoint) trim(checkpointStr string) error {
	c.keysMu.Lock()
	defer c.keysMu.Unlock()

	checkpoint, err := strconv.Atoi(checkpointStr)
	if err != nil {
		return err
	}

	if _, ok := c.keys.find(checkpoint); !ok {
		return errors.New("Sequence number not found")
	}

	cp := int(checkpoint)
	return c.keys.trim(&cp, nil)
}

// expire safely removes sequence numbers from the checkpointing system that are older than the given age.
func (c *checkpoint) expire(age time.Duration) error {
	c.keysMu.Lock()
	defer c.keysMu.Unlock()

	return c.keys.expire(age)
}

// checkpoint sends a signal to the checkpointCh channel to enable a call to checkpoint.
func (c *checkpoint) checkpoint() error {
	if c.checkpointCh == nil {
		return errors.New("Nil checkpoint channel")
	}
	c.checkpointCh <- struct{}{}
	return nil
}
