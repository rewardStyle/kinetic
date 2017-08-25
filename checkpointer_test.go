package kinetic

import (
	. "github.com/smartystreets/goconvey/convey"

	"container/list"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// initialize the randomization seed for the random number generator.
func init() {
	rand.Seed(time.Now().UnixNano())
}

// sortInt is an alias to a slice array of int that implements the sort interface for sorting.
type sortInt []int

// Len implements the Len function for hte sort interface.
func (k sortInt) Len() int {
	return len(k)
}

// Less implements the Less function for the sort interface.
func (k sortInt) Less(i, j int) bool {
	return k[i] < k[j]
}

// Swap implements the Swap function for the sort interface.
func (k sortInt) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

// sortUnique is a helper method used to return a unique and sorted slice of keys.
func sortUnique(keys sortInt) sortInt {
	keyMap := make(map[int]bool)
	for _, key := range keys {
		if _, ok := keyMap[key]; !ok {
			keyMap[key] = true
		}
	}

	var sortedKeys sortInt
	for key := range keyMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Sort(sortedKeys)

	return sortedKeys
}

// randomInts is helper method used to generate a random set of keys for testing.
func randomInts(size, max int) (ints []int) {
	for i := 0; i < size; i++ {
		ints = append(ints, int(rand.Intn(max)))
	}
	return ints
}

// intPointer is a helper method used to return the address of an int
func intPointer(x int) *int {
	return &x
}

func TestCheckpointListInsert(t *testing.T) {
	var tests = []struct {
		scenario string
		numbers  []int
	}{
		{"insert into an empty list", []int{1}},
		{"insert duplicate element", []int{1, 1}},
		{"insert in ascending order", []int{1, 2, 3, 4, 5}},
		{"insert in descending order", []int{5, 4, 3, 2, 1}},
		{"insert in random order", randomInts(100, 1000)},
	}

	for _, test := range tests {
		c := &checkpointList{list.New()}
		Convey(fmt.Sprintf("running checkpointList test suite on scenario: [%s]", test.scenario), t, func() {
			Convey("inserting numbers into list", func() {
				var errCount int
				for _, number := range test.numbers {
					if err := c.insert(number); err != nil {
						errCount++
					}
				}
				So(errCount, ShouldEqual, 0)

				Convey("confirm the length and order of the list", func() {
					sortedUniqueNumbers := sortUnique(test.numbers)
					var actualNumbers sortInt
					for e := c.Front(); e != nil; e = e.Next() {
						actualNumbers = append(actualNumbers, e.Value.(*checkpointElement).seqNum)
					}
					So(len(actualNumbers), ShouldEqual, len(sortedUniqueNumbers))
					So(sort.IsSorted(actualNumbers), ShouldBeTrue)
				})
			})
		})
	}
}

func TestCheckpointListFind(t *testing.T) {
	testNumbers := []int{3, 2, 4, 5, 1}
	var tests = []struct {
		scenario   string
		numbers    []int
		find       int
		shouldFind bool
	}{
		{"find on an empty list", []int{}, 0, false},
		{"find on an element that does not exist", testNumbers, 0, false},
		{"find the biggeset element", testNumbers, 5, true},
		{"find the smallest element", testNumbers, 1, true},
		{"find a middle element", testNumbers, 3, true},
	}

	for _, test := range tests {
		c := &checkpointList{list.New()}
		Convey(fmt.Sprintf("running find test suite on scenario: [%s]", test.scenario), t, func() {
			Convey("inserting numbers into list", func() {
				var errCount int
				for _, number := range test.numbers {
					if err := c.insert(number); err != nil {
						errCount++
					}
				}
				So(errCount, ShouldEqual, 0)

				Convey("find should behave as expected", func() {
					el, found := c.find(test.find)
					So(found, ShouldEqual, test.shouldFind)
					if found {
						So(el.Value.(*checkpointElement).seqNum, ShouldEqual, test.find)
					}
				})
			})
		})
	}
}

func TestCheckpointerInsertAndSize(t *testing.T) {
	var tests = []struct {
		scenario string
		numbers  []int
	}{
		{"insert into an empty list", []int{1}},
		{"insert duplicate elements", []int{1, 1}},
		{"insert into an empty list", []int{1}},
		{"insert in ascending order", []int{11, 22, 33, 44, 55}},
		{"insert in descending order", []int{55, 44, 33, 22, 11}},
		{"insert in random order", randomInts(100, 1000)},
	}

	for _, test := range tests {
		c := newCheckpointer()
		Convey(fmt.Sprintf("running insert and size test suite on scenario: [%s]", test.scenario), t, func() {
			Convey("inserting numbers into list", func() {
				var errCount int
				for _, number := range test.numbers {
					if err := c.insert(strconv.Itoa(number)); err != nil {
						errCount++
					}
				}
				So(errCount, ShouldEqual, 0)

				Convey("confirm the size and order of the list", func() {
					sortedUniqueNumbers := sortUnique(test.numbers)
					var actualNumbers sortInt
					for e := c.list.Front(); e != nil; e = e.Next() {
						actualNumbers = append(actualNumbers, e.Value.(*checkpointElement).seqNum)
					}
					So(c.size(), ShouldEqual, len(sortedUniqueNumbers))
					So(len(actualNumbers), ShouldEqual, len(sortedUniqueNumbers))
					So(sort.IsSorted(actualNumbers), ShouldBeTrue)
				})
			})
		})
	}
}

func TestCheckpointerMarkDoneAndCheck(t *testing.T) {
	testEmpty := []int{}
	testNumbers := []int{5, 2, 6, 4, 0, 1, 3}
	var tests = []struct {
		scenario              string
		numbers               []int
		doneNumbers           []int
		shouldErrorOnMarkDone bool
		expectedSize          *int
		shouldFindCheckpoint  bool
		expectedCheckpoint    *int
	}{
		{
			scenario:              "markDone on an empty list",
			numbers:               testEmpty,
			doneNumbers:           []int{1},
			shouldErrorOnMarkDone: true,
			expectedSize:          nil,
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "markDone on non existent number",
			numbers:               testNumbers,
			doneNumbers:           []int{-1},
			shouldErrorOnMarkDone: true,
			expectedSize:          nil,
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "check when no numbers are inserted",
			numbers:               testEmpty,
			doneNumbers:           testEmpty,
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(len(testEmpty)),
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "check when no numbers are marked done",
			numbers:               testNumbers,
			doneNumbers:           testEmpty,
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(len(testNumbers)),
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "markDone on an existing number (not smallest number) and check",
			numbers:               testNumbers,
			doneNumbers:           []int{1},
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(len(testNumbers)),
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "markDone twice on an existing number (not smallest number) and check ",
			numbers:               testNumbers,
			doneNumbers:           []int{1, 1},
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(len(testNumbers)),
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "markDone on two sequential numbers (not smallest number, ascending) and check",
			numbers:               testNumbers,
			doneNumbers:           []int{1, 2},
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(len(testNumbers) - 1),
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "markDone on two sequential numbers (not smallest number, descending) and check",
			numbers:               testNumbers,
			doneNumbers:           []int{2, 1},
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(len(testNumbers) - 1),
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "markDone on three sequential numbers (not smallest number) and check",
			numbers:               testNumbers,
			doneNumbers:           []int{3, 1, 2},
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(len(testNumbers) - 2),
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "check when the smallest number is not marked done",
			numbers:               testNumbers,
			doneNumbers:           []int{1, 2, 3, 4, 5, 6},
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(2),
			shouldFindCheckpoint:  false,
			expectedCheckpoint:    nil,
		},
		{
			scenario:              "check when a chain (including the smallest number) is marked done",
			numbers:               testNumbers,
			doneNumbers:           []int{2, 1, 3, 0},
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(4),
			shouldFindCheckpoint:  true,
			expectedCheckpoint:    intPointer(3),
		},
		{
			scenario:              "markDone on all numbers and check",
			numbers:               testNumbers,
			doneNumbers:           testNumbers,
			shouldErrorOnMarkDone: false,
			expectedSize:          intPointer(1),
			shouldFindCheckpoint:  true,
			expectedCheckpoint:    intPointer(6),
		},
	}

	for _, test := range tests {
		c := newCheckpointer()
		Convey(fmt.Sprintf("running markDone and check test suite on scenario: [%s]", test.scenario), t, func() {
			Convey("inserting numbers into list", func() {
				var errCount int
				for _, number := range test.numbers {
					if err := c.insert(strconv.Itoa(number)); err != nil {
						errCount++
					}
				}
				So(errCount, ShouldEqual, 0)

				Convey("mark numbers done should behave as expected", func() {
					var doneErrCount int
					for _, doneNumber := range test.doneNumbers {
						if err := c.markDone(strconv.Itoa(doneNumber)); err != nil {
							doneErrCount++
						}
					}
					if test.shouldErrorOnMarkDone {
						So(doneErrCount, ShouldBeGreaterThan, 0)
						return
					}

					So(doneErrCount, ShouldEqual, 0)
					So(c.size(), ShouldEqual, *test.expectedSize)
					Convey("call check should behave as expected", func() {
						cp, found := c.check()
						if test.shouldFindCheckpoint {
							So(found, ShouldBeTrue)
							So(cp, ShouldEqual, strconv.Itoa(*test.expectedCheckpoint))
						} else {
							So(found, ShouldBeFalse)

						}
					})
				})
			})
		})
	}
}

func TestCheckpointerAutoCheckpointing(t *testing.T) {
	testNumbers := []int{1, -2, 3, -4, 5}
	Convey("test auto checkpoint using autoCheckpointCount", t, func() {
		autoCheckpointCount := 5
		Convey("instantiating a new checkpoint with options", func() {
			var checkpointFnCalled uint64
			cp := newCheckpointer(
				checkpointCountCheckFreq(time.Millisecond),
				checkpointAutoCheckpointFreq(time.Minute),
				checkpointAutoCheckpointCount(autoCheckpointCount),
				checkpointCheckpointFn(func(seqNum string) error {
					atomic.AddUint64(&checkpointFnCalled, 1)
					return nil
				}),
			)
			So(cp, ShouldNotBeNil)
			So(cp.autoCheckpointCount, ShouldEqual, autoCheckpointCount)

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()
			cp.startup(ctx)

			Convey("inserting and marking all keys as done", func() {
				var failedCount int
				for _, testKey := range testNumbers {
					key := strconv.Itoa(testKey)
					if err := cp.insert(key); err != nil {
						failedCount++
					}
					if err := cp.markDone(key); err != nil {
						failedCount++
					}
				}
				So(failedCount, ShouldEqual, 0)

				<-time.After(2*time.Millisecond)
				Convey("confirming that checkpoint was called", func() {
					So(atomic.LoadUint64(&checkpointFnCalled), ShouldBeGreaterThan, 0)
				})
			})
		})
	})

	Convey("test auto checkpoint using autoCheckpointFreq", t, func() {
		autoCheckpointFreq := 100 * time.Millisecond
		Convey("instantiating a new checkpoint with options", func() {
			var checkpointFnCalled uint64
			cp := newCheckpointer(
				checkpointAutoCheckpointFreq(autoCheckpointFreq),
				checkpointAutoCheckpointCount(1000),
				checkpointCheckpointFn(func(seqNum string) error {
					atomic.AddUint64(&checkpointFnCalled, 1)
					return nil
				}),
			)
			So(cp, ShouldNotBeNil)
			So(cp.autoCheckpointFreq, ShouldEqual, autoCheckpointFreq)

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()
			cp.startup(ctx)

			Convey("inserting and marking all keys as done", func() {
				var failedCount int
				for _, testKey := range testNumbers {
					key := strconv.Itoa(testKey)
					if err := cp.insert(key); err != nil {
						failedCount++
					}
					if err := cp.markDone(key); err != nil {
						failedCount++
					}
				}
				So(failedCount, ShouldEqual, 0)

				<-time.After(time.Duration(2) * autoCheckpointFreq)
				Convey("confirming that checkpoint was called", func() {
					So(atomic.LoadUint64(&checkpointFnCalled), ShouldBeGreaterThan, 0)
				})
			})
		})

	})

	Convey("test calling checkpoint function", t, func() {
		testKeys := []int{1, -2, 3, -4, 5}
		var checkpointFnCalled uint64
		cp := newCheckpointer(
			checkpointCheckpointFn(func(seqNum string) error {
				atomic.AddUint64(&checkpointFnCalled, 1)
				return nil
			}),
		)
		So(cp, ShouldNotBeNil)

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		cp.startup(ctx)

		Convey("inserting and marking all keys as done", func() {
			var failedCount int
			for _, testKey := range testKeys {
				key := strconv.Itoa(testKey)
				if err := cp.insert(key); err != nil {
					failedCount++
				}
				if err := cp.markDone(key); err != nil {
					failedCount++
				}
			}
			So(failedCount, ShouldEqual, 0)

			Convey("calling checkpoint", func() {
				err := cp.checkpoint()
				So(err, ShouldBeNil)

				<-time.After(500 * time.Millisecond)
				Convey("confirming that checkpointFn was called", func() {
					So(atomic.LoadUint64(&checkpointFnCalled), ShouldBeGreaterThan, 0)
				})
			})
		})
	})
}

func TestCheckpointerOffNominal(t *testing.T) {
	Convey("insert a bogus number into the list", t, func() {
		c := newCheckpointer()
		err := c.insert("bogus data")
		So(err, ShouldNotBeNil)
	})
}
