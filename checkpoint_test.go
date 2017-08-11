package kinetic

import (
	. "github.com/smartystreets/goconvey/convey"

	"context"
	"fmt"
	"math"
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

// sortByKeys is an alias to a slice array of int that implements the sort interface for sorting.
type sortByKeys []int

// Len implements the Len function for hte sort interface.
func (k sortByKeys) Len() int {
	return len(k)
}

// Less implements the Less function for the sort interface.
func (k sortByKeys) Less(i, j int) bool {
	return k[i] < k[j]
}

// Swap implements the Swap function for the sort interface.
func (k sortByKeys) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

// insertKeys is a helper function for inserting multiple keys into the binary search tree.
func insertKeys(t *btree, keys []int) bool {
	for _, key := range keys {
		err := t.insert(key)
		if err != nil {
			return false
		}
	}
	return true
}

// findKeys is a helper function for confirm that all of the given keys were found.
func findKeys(t *btree, keys []int) bool {
	for _, key := range keys {
		node, ok := t.find(key)
		if !ok {
			return false
		}
		if node == nil || key != node.seqNum {
			return false
		}
	}
	return true
}

// sortUnique is a helper method used to return a unique and sorted slice of keys.
func sortUnique(keys []int) sortByKeys {
	keyMap := make(map[int]bool)
	for _, key := range keys {
		if _, ok := keyMap[key]; !ok {
			keyMap[key] = true
		}
	}

	var sortedKeys sortByKeys
	for key := range keyMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Sort(sortedKeys)

	return sortedKeys
}

// randomKeys is helper method used to generate a random set of keys for testing.
func randomKeys(size, max int) (keys []int) {
	for i := 0; i < size; i++ {
		keys = append(keys, int(rand.Intn(max)))
	}
	return keys
}

// intPointer is a helper method used to return the address of an int
func intPointer(x int) *int {
	return &x
}

func TestCheckpointBTreeInsert(t *testing.T) {
	var tests = []struct {
		scenario string
		keys     []int
	}{
		{
			scenario: "empty tree",
			keys:     []int{},
		},
		{
			scenario: "single key",
			keys:     []int{1},
		},
		{
			scenario: "duplicate key",
			keys:     []int{1, 1},
		},
		{
			scenario: "keys in ascending order",
			keys:     []int{1, 2, 3},
		},
		{
			scenario: "keys in descendeing order",
			keys:     []int{4, 3, 2, 1},
		},
		{
			scenario: "keys in random order",
			keys:     randomKeys(100, 1000),
		},
	}

	for _, test := range tests {
		var b btree
		sortedKeys := sortUnique(test.keys)
		Convey(fmt.Sprintf("running binary tree insert test suit on scenario: [%s]", test.scenario), t, func() {
			Convey("should be able to insert keys without errors", func() {
				ok := insertKeys(&b, test.keys)
				So(ok, ShouldBeTrue)

				if len(sortedKeys) > 0 {
					Convey("should be able to find all keys without errors", func() {
						ok := findKeys(&b, sortedKeys)
						So(ok, ShouldBeTrue)
					})
				}

				Convey("should be able to determine size correctly", func() {
					So(b.size(), ShouldEqual, len(sortedKeys))
				})

				Convey("should be able to confirm sorting order", func() {
					var keys sortByKeys
					b.traverseOrdered(b.root, func(n *btreeNode) {
						keys = append(keys, n.seqNum)
					})
					So(b.size(), ShouldEqual, len(keys))
					So(sort.IsSorted(keys), ShouldBeTrue)
				})

				Convey("should be able to find the min node correctly", func() {
					minNode := b.minNode()
					if minNode == nil {
						So(b.root, ShouldBeNil)
					} else {
						So(b.minNode().seqNum, ShouldEqual, sortedKeys[0])
					}
				})

				Convey("should be able to find the max node correctly", func() {
					maxNode := b.maxNode()
					if maxNode == nil {
						So(b.root, ShouldBeNil)
					} else {
						So(b.maxNode().seqNum, ShouldEqual, sortedKeys[len(sortedKeys)-1])
					}
				})
			})
		})
	}
}

func TestCheckpointBTreeFind(t *testing.T) {
	var tests = []struct {
		scenario   string
		keys       []int
		findKey    int
		shouldFind bool
	}{
		{
			scenario:   "key is on a root node",
			keys:       []int{2, 3, 1},
			findKey:    2,
			shouldFind: true,
		},
		{
			scenario:   "key is on a right child node",
			keys:       []int{2, 3, 1},
			findKey:    3,
			shouldFind: true,
		},
		{
			scenario:   "key is on a left child node",
			keys:       []int{2, 3, 1},
			findKey:    1,
			shouldFind: true,
		},
		{
			scenario:   "key does not exist",
			keys:       []int{2, 3, 1},
			findKey:    0,
			shouldFind: false,
		},
		{
			scenario:   "empty tree / key does not exist",
			keys:       []int{},
			findKey:    0,
			shouldFind: false,
		},
	}

	var b btree
	for _, test := range tests {
		Convey(fmt.Sprintf("running binary tree find test suit on scenario: [%s]", test.scenario), t, func() {
			ok := insertKeys(&b, test.keys)
			So(ok, ShouldBeTrue)

			Convey("calling find should behave accordingly", func() {
				node, ok := b.find(test.findKey)
				So(ok, ShouldEqual, test.shouldFind)
				if test.shouldFind {
					Convey("the node return should match the key", func() {
						So(node.seqNum, ShouldEqual, test.findKey)
					})
				}
			})
		})
	}
}

func TestCheckpointBTreeDelete(t *testing.T) {
	var tests = []struct {
		scenario    string
		keys        []int
		deleteKey   int
		shouldError bool
	}{
		{
			scenario:    "delete on empty tree",
			keys:        []int{},
			deleteKey:   1,
			shouldError: true,
		},
		{
			scenario:    "delete a key that doesn't exist",
			keys:        []int{2, 3, 1},
			deleteKey:   0,
			shouldError: true,
		},
		{
			scenario:    "delete a node with no children",
			keys:        []int{5, 2, 18, -4, 3},
			deleteKey:   -4,
			shouldError: false,
		},
		{
			scenario:    "delete a node with one child on the left",
			keys:        []int{5, 2, 18, -4, 3, 16, 15, 17},
			deleteKey:   18,
			shouldError: false,
		},
		{
			scenario:    "delete a node with one child on the right",
			keys:        []int{5, 2, 18, -4, 3, 21, 19, 25},
			deleteKey:   18,
			shouldError: false,
		},
		{
			scenario:    "delete a node with two children",
			keys:        []int{5, 2, 19, 16, 25},
			deleteKey:   19,
			shouldError: false,
		},
		{
			scenario:    "delete a root node with no children",
			keys:        []int{1},
			deleteKey:   1,
			shouldError: false,
		},
		{
			scenario:    "delete a root node with one child on the left",
			keys:        []int{2, 1},
			deleteKey:   2,
			shouldError: false,
		},
		{
			scenario:    "delete a root node with one child on the right",
			keys:        []int{2, 3},
			deleteKey:   2,
			shouldError: false,
		},
		{
			scenario:    "delete a root node with two children",
			keys:        []int{2, 1, 3},
			deleteKey:   3,
			shouldError: false,
		},
	}

	for _, test := range tests {
		var b btree
		sortedKeys := sortUnique(test.keys)
		Convey(fmt.Sprintf("running binary tree delete test suit on scenario: [%s]", test.scenario), t, func() {
			ok := insertKeys(&b, test.keys)
			So(ok, ShouldBeTrue)

			Convey("should be able to call delete and respond correctly", func() {
				err := b.delete(test.deleteKey)
				if test.shouldError {
					So(err, ShouldNotBeNil)
				} else {
					So(err, ShouldBeNil)
				}

				if test.shouldError {
					if len(sortedKeys) > 0 {
						Convey("should be able to find all keys without errors", func() {
							ok := findKeys(&b, sortedKeys)
							So(ok, ShouldBeTrue)
						})
					}

					Convey("should not be able to find delete key", func() {
						_, ok := b.find(test.deleteKey)
						So(ok, ShouldBeFalse)
					})

					Convey("should be able to determine size correctly", func() {
						So(b.size(), ShouldEqual, len(sortedKeys))
					})

					Convey("should be able to confirm sorting order", func() {
						var keys sortByKeys
						b.traverseOrdered(b.root, func(n *btreeNode) {
							keys = append(keys, n.seqNum)
						})
						So(b.size(), ShouldEqual, len(keys))
						So(sort.IsSorted(keys), ShouldBeTrue)
					})

					Convey("should be able to find the min node correctly", func() {
						minNode := b.minNode()
						if minNode == nil {
							So(b.root, ShouldBeNil)
						} else {
							So(b.minNode().seqNum, ShouldEqual, sortedKeys[0])
						}
					})

					Convey("should be able to find the max node correctly", func() {
						maxNode := b.maxNode()
						if maxNode == nil {
							So(b.root, ShouldBeNil)
						} else {
							So(b.maxNode().seqNum, ShouldEqual, sortedKeys[len(sortedKeys)-1])
						}
					})

				} else {
					// determine the new key set (sorted and unique) after removing the first element
					var newKeys sortByKeys
					for _, k := range test.keys {
						if k != test.deleteKey {
							newKeys = append(newKeys, k)
						}
					}
					sort.Sort(newKeys)

					if len(newKeys) > 0 {
						Convey("should be able to find remaining keys after a delete", func() {
							ok := findKeys(&b, newKeys)
							So(ok, ShouldBeTrue)
						})
					}

					Convey("should be able to determine size correctly after a delete", func() {
						So(b.size(), ShouldEqual, len(newKeys))
					})

					Convey("should be able to confirm sorting order after a delete", func() {
						var keys sortByKeys
						b.traverseOrdered(b.root, func(n *btreeNode) {
							keys = append(keys, n.seqNum)
						})
						So(b.size(), ShouldEqual, len(keys))
						So(sort.IsSorted(keys), ShouldBeTrue)
					})

					Convey("should be able to find the min node correctly after a delete", func() {
						minNode := b.minNode()
						if minNode == nil {
							So(b.root, ShouldBeNil)
						} else {
							So(b.minNode().seqNum, ShouldEqual, newKeys[0])
						}
					})

					Convey("should be able to find the max node correctly after a delete", func() {
						maxNode := b.maxNode()
						if maxNode == nil {
							So(b.root, ShouldBeNil)
						} else {
							So(b.maxNode().seqNum, ShouldEqual, newKeys[len(newKeys)-1])
						}
					})
				}
			})
		})
	}
}

func TestCheckpointBTreeTrim(t *testing.T) {
	testKeys := []int{50, 20, 60, 40, 0, 10, 30}
	var tests = []struct {
		scenario string
		keys     []int
		trimMin  *int
		trimMax  *int
	}{
		{
			scenario: "trim on empty tree",
			keys:     []int{},
			trimMin:  intPointer(0),
			trimMax:  intPointer(10),
		},
		{
			scenario: "trim left - none",
			keys:     testKeys,
			trimMin:  intPointer(-1),
			trimMax:  nil,
		},
		{
			scenario: "trim left - some",
			keys:     testKeys,
			trimMin:  intPointer(20),
			trimMax:  nil,
		},
		{
			scenario: "trim left - all",
			keys:     testKeys,
			trimMin:  intPointer(60),
			trimMax:  nil,
		},
		{
			scenario: "trim right - none",
			keys:     testKeys,
			trimMin:  nil,
			trimMax:  intPointer(70),
		},
		{
			scenario: "trim right - some",
			keys:     testKeys,
			trimMin:  nil,
			trimMax:  intPointer(40),
		},
		{
			scenario: "trim right - all",
			keys:     testKeys,
			trimMin:  nil,
			trimMax:  intPointer(0),
		},
		{
			scenario: "trim - none",
			keys:     testKeys,
			trimMin:  intPointer(0),
			trimMax:  intPointer(60),
		},
		{
			scenario: "trim - some",
			keys:     testKeys,
			trimMin:  intPointer(10),
			trimMax:  intPointer(50),
		},
		{
			scenario: "trim - all",
			keys:     testKeys,
			trimMin:  intPointer(1),
			trimMax:  intPointer(9),
		},
	}

	for _, test := range tests {
		var b btree
		sortedKeys := sortUnique(test.keys)
		Convey(fmt.Sprintf("running binary tree trim test suite on [%s]", test.scenario), t, func() {
			Convey("should be able to insert keys without errors", func() {
				ok := insertKeys(&b, test.keys)
				So(ok, ShouldBeTrue)

				if len(sortedKeys) > 0 {
					Convey("should be able to find all keys without errors", func() {
						ok := findKeys(&b, sortedKeys)
						So(ok, ShouldBeTrue)
					})
				}

				uniqueKeys := sortUnique(test.keys)
				var remainingKeys sortByKeys
				var trimmedKeys sortByKeys
				var min, max int
				if test.trimMin == nil {
					min = math.MinInt64
				} else {
					min = *test.trimMin
				}
				if test.trimMax == nil {
					max = math.MaxInt64
				} else {
					max = *test.trimMax
				}

				for _, key := range uniqueKeys {
					if key > min && key < max {
						remainingKeys = append(remainingKeys, key)
					} else {
						trimmedKeys = append(trimmedKeys, key)
					}
				}

				Convey("calling trim on tree", func() {
					err := b.trim(test.trimMin, test.trimMax)
					So(err, ShouldBeNil)

					if len(remainingKeys) > 0 {
						Convey("should be able to find all remaining keys", func() {
							So(findKeys(&b, remainingKeys), ShouldBeTrue)
						})
					}

					if len(trimmedKeys) > 0 {
						Convey("should not be able to find any trimmed keys", func() {
							for _, key := range trimmedKeys {
								_, ok := b.find(key)
								So(ok, ShouldBeFalse)
							}
						})
					}
				})
			})
		})
	}
}

func TestCheckpointBTreeExpire(t *testing.T) {
	testKeys1 := []int{20, 30, 10}
	testKeys2 := []int{15, 25, -5}
	var tests = []struct {
		scenario     string
		keys1        []int
		keys2        []int
		pause1       time.Duration
		pause2       time.Duration
		expiration   time.Duration
		expectedSize int
	}{
		{
			scenario:     "expire on an empty tree",
			keys1:        []int{},
			keys2:        []int{},
			pause1:       0 * time.Second,
			pause2:       0 * time.Second,
			expiration:   time.Second,
			expectedSize: 0,
		},
		{
			scenario:     "expire none",
			keys1:        testKeys1,
			keys2:        testKeys2,
			pause1:       0 * time.Second,
			pause2:       0 * time.Second,
			expiration:   time.Second,
			expectedSize: len(testKeys1) + len(testKeys2),
		},
		{
			scenario:     "expire some",
			keys1:        testKeys1,
			keys2:        testKeys2,
			pause1:       time.Second,
			pause2:       0 * time.Second,
			expiration:   time.Second,
			expectedSize: len(testKeys2),
		},
		{
			scenario:     "expire all",
			keys1:        testKeys1,
			keys2:        testKeys2,
			pause1:       0 * time.Second,
			pause2:       time.Second,
			expiration:   time.Second,
			expectedSize: 0,
		},
	}

	for _, test := range tests {
		var b btree
		sortedKeys := sortUnique(append(test.keys1, test.keys2...))
		Convey(fmt.Sprintf("running binary tree expire test suit on scenario: [%s]", test.scenario), t, func() {
			ok1 := insertKeys(&b, test.keys1)
			So(ok1, ShouldBeTrue)
			<-time.After(test.pause1)

			ok2 := insertKeys(&b, test.keys2)
			So(ok2, ShouldBeTrue)
			<-time.After(test.pause2)

			Convey("should be able to find all inserted keys", func() {
				ok := findKeys(&b, sortedKeys)
				So(ok, ShouldBeTrue)

				Convey("calling trim with expiration", func() {
					err := b.expire(test.expiration)
					So(err, ShouldBeNil)

					Convey("size should match expected size", func() {
						So(b.size(), ShouldEqual, test.expectedSize)

						if test.expectedSize > 0 {
							Convey("remaining nodes should be under the age limit", func() {
								var failed bool
								b.traverseOrdered(b.root, func(n *btreeNode) {
									var stop bool
									if stop {
										return
									}
									if time.Since(n.age) > test.expiration {
										failed = false
										stop = true
									}
								})
								So(failed, ShouldBeFalse)
							})
						}
					})
				})
			})
		})
	}
}

func TestCheckpointOffNominal(t *testing.T) {
	Convey("off nominal operations on an empty tree", t, func() {
		cp := newCheckpoint()
		Convey("markDone on an empty tree should error", func() {
			err := cp.markDone("1")
			So(err, ShouldNotBeNil)
		})

		Convey("trim on an empty tree should error", func() {
			err := cp.trim("1")
			So(err, ShouldNotBeNil)

		})
	})

	Convey("off nominal operations on a non-empty tree", t, func() {
		cp := newCheckpoint()
		cp.insert("2")
		cp.insert("3")
		cp.insert("1")
		Convey("markDone on a key that does not exist should error", func() {
			err := cp.markDone("0")
			So(err, ShouldNotBeNil)
		})

		Convey("trim on a key that does not exist should error", func() {
			err := cp.trim("0")
			So(err, ShouldNotBeNil)

		})
	})
}

func TestCheckpointConfigs(t *testing.T) {
	testKeys := []int{1, -2, 3, -4, 5}
	Convey("test auto checkpoint using autoCheckpointCount", t, func() {
		autoCheckpointCount := 5
		Convey("instantiating a new checkpoint with options", func() {
			var checkpointFnCalled uint64
			cp := newCheckpoint(
				checkpointCountCheckFreq(time.Millisecond),
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

				<-time.After(time.Millisecond)
				Convey("confirming that checkpoint and trim was called", func() {
					So(atomic.LoadUint64(&checkpointFnCalled), ShouldBeGreaterThan, 0)
					cp.keysMu.Lock()
					So(cp.keys.size(), ShouldEqual, 0)
					cp.keysMu.Unlock()
				})
			})
		})
	})

	Convey("test auto checkpoint using autoCheckpointFreq", t, func() {
		autoCheckpointFreq := 100 * time.Millisecond
		Convey("instantiating a new checkpoint with options", func() {
			var checkpointFnCalled uint64
			cp := newCheckpoint(
				checkpointAutoCheckpointFreq(autoCheckpointFreq),
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

				<-time.After(time.Duration(2) * autoCheckpointFreq)
				Convey("confirming that checkpoint and trim was called", func() {
					So(atomic.LoadUint64(&checkpointFnCalled), ShouldBeGreaterThan, 0)
					cp.keysMu.Lock()
					So(cp.keys.size(), ShouldEqual, 0)
					cp.keysMu.Unlock()
				})
			})
		})

	})

	Convey("test auto expiration using maxAge", t, func() {
		maxAge := 500 * time.Millisecond
		Convey("instantiating a new checkpoint with options", func() {
			cp := newCheckpoint(
				checkpointMaxAge(maxAge),
			)
			So(cp, ShouldNotBeNil)
			So(cp.maxAge, ShouldEqual, maxAge)

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

				<-time.After(time.Duration(2) * maxAge)
				Convey("confirming that expire was called", func() {
					cp.keysMu.Lock()
					So(cp.keys.size(), ShouldEqual, 0)
					cp.keysMu.Unlock()
				})
			})
		})
	})

	Convey("test preventing inserts after maxSize capacity is reached", t, func() {
		Convey("instantiating a new checkpoint with max size set", func() {
			testKeys := []int{1, -23, 45, -67, 89}
			maxSize := 4
			var capacityFnCalled bool
			cp := newCheckpoint(
				checkpointMaxSize(maxSize),
				checkpointCapacityFn(func(seqNum string) error {
					capacityFnCalled = true
					return nil
				}),
			)
			So(cp, ShouldNotBeNil)
			So(cp.maxSize, ShouldEqual, maxSize)

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()
			cp.startup(ctx)

			Convey("inserting one too many keys than allowed", func() {
				var failCount int
				for _, testKey := range testKeys {
					if err := cp.insert(strconv.Itoa(testKey)); err != nil {
						failCount++
					}
				}
				So(failCount, ShouldBeGreaterThan, 0)
				So(capacityFnCalled, ShouldBeTrue)
			})
		})
	})

	Convey("test calling checkpoint function", t, func() {
		testKeys := []int{1, -2, 3, -4, 5}
		var checkpointFnCalled uint64
		cp := newCheckpoint(
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
					cp.keysMu.Lock()
					So(cp.keys.size(), ShouldEqual, 0)
					cp.keysMu.Unlock()
				})
			})
		})
	})
}

func TestCheckpointNominal(t *testing.T) {
	testKeys := []int{5, 2, 6, 4, 0, 1, 3}
	var tests = []struct {
		scenario   string
		keys       []int
		doneKeys   []int
		shouldBeOK bool
	}{
		{
			scenario:   "checkpoint/trim when there are no keys inserted",
			keys:       []int{},
			doneKeys:   []int{},
			shouldBeOK: false,
		},
		{
			scenario:   "checkpoint/trim when no keys are marked done",
			keys:       testKeys,
			doneKeys:   []int{},
			shouldBeOK: false,
		},
		{
			scenario:   "checkpoint/trim when minimum node is not marked done",
			keys:       testKeys,
			doneKeys:   []int{1, 2, 3},
			shouldBeOK: false,
		},
		{
			scenario:   "checkpoint/trim when minimum node is marked done",
			keys:       testKeys,
			doneKeys:   []int{2, 3, 1, 0},
			shouldBeOK: true,
		},
		{
			scenario:   "checkpoint/trim when all keys are marked done",
			keys:       testKeys,
			doneKeys:   testKeys,
			shouldBeOK: true,
		},
		{
			scenario:   "checkpoint/trim only one key exists",
			keys:       []int{1},
			doneKeys:   []int{1},
			shouldBeOK: true,
		},
	}

	for _, test := range tests {
		cp := newCheckpoint()
		Convey(fmt.Sprintf("running checkpoint test suit on scenario: [%s]", test.scenario), t, func() {
			Convey("inserting the test keys", func() {
				var errCount int
				for _, key := range test.keys {
					if err := cp.insert(strconv.Itoa(key)); err != nil {
						errCount++
					}
				}
				So(errCount, ShouldEqual, 0)

				Convey("confirming keys were inserted correctly", func() {
					So(findKeys(cp.keys, test.keys), ShouldBeTrue)
				})

				uniqueDoneKeys := sortUnique(test.doneKeys)
				Convey("marking test keys as done", func() {
					if len(test.doneKeys) > 0 {
						var errCount int
						for _, doneKey := range test.doneKeys {
							if err := cp.markDone(strconv.Itoa(doneKey)); err != nil {
								errCount++
							}
						}
						So(errCount, ShouldEqual, 0)

						Convey("confirm that only the test keys were marked done", func() {
							var markedDone sortByKeys
							cp.keys.traverseOrdered(cp.keys.root, func(n *btreeNode) {
								if n.done {
									markedDone = append(markedDone, n.seqNum)
								}
							})
							So(len(markedDone), ShouldEqual, len(uniqueDoneKeys))

							for _, doneKey := range test.doneKeys {
								node, ok := cp.keys.find(doneKey)
								So(node.done, ShouldBeTrue)
								So(ok, ShouldBeTrue)
							}
						})
					}

					Convey("calling checkpoint should yeild the expected behavior", func() {
						checkpoint, ok := cp.check()
						So(checkpoint, ShouldNotBeNil)
						So(ok, ShouldEqual, test.shouldBeOK)
						//So(cp.lastCheckpoint, ShouldEqual, checkpoint)

						if test.shouldBeOK {
							uniqueKeys := sortUnique(test.keys)
							Convey("and should also result in the correct checkpoint key", func() {
								var expectedCheckpoint int
								for i := 0; i < len(uniqueDoneKeys); i++ {
									if uniqueKeys[i] == uniqueDoneKeys[i] {
										expectedCheckpoint = uniqueDoneKeys[i]
									} else {
										break
									}
								}
								So(checkpoint, ShouldEqual, strconv.Itoa(expectedCheckpoint))
							})

							Convey("calling trim using the checkpoint key", func() {
								err := cp.trim(checkpoint)
								So(err, ShouldBeNil)

								Convey("should not be able to find any keys marked done", func() {
									var foundDone bool
									var stop bool
									cp.keys.traverseOrdered(cp.keys.root, func(n *btreeNode) {
										if stop {
											return
										}
										if n.done {
											foundDone = true
											stop = true
										}
									})
									So(foundDone, ShouldBeFalse)
								})

								if len(uniqueKeys)-len(uniqueDoneKeys) > 0 {
									Convey("should still be able to find remaining keys", func() {
										remainingKeys := uniqueKeys[len(uniqueDoneKeys):]
										for _, key := range remainingKeys {
											node, ok := cp.keys.find(key)
											So(ok, ShouldBeTrue)
											So(node, ShouldNotBeNil)
											So(node.done, ShouldBeFalse)
										}
									})
								}
							})
						}
					})
				})
			})
		})
	}
}
