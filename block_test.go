// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"encoding/binary"

	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
	"github.com/prometheus/tsdb/tsdbutil"
)

// In Prometheus 2.1.0 we had a bug where the meta.json version was falsely bumped
// to 2. We had a migration in place resetting it to 1 but we should move immediately to
// version 3 next time to avoid confusion and issues.
func TestBlockMetaMustNeverBeVersion2(t *testing.T) {
	dir, err := ioutil.TempDir("", "metaversion")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	_, err = writeMetaFile(log.NewNopLogger(), dir, &BlockMeta{})
	testutil.Ok(t, err)

	meta, _, err := readMetaFile(dir)
	testutil.Ok(t, err)
	testutil.Assert(t, meta.Version != 2, "meta.json version must never be 2")
}

func TestSetCompactionFailed(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()

	blockDir := createBlock(t, tmpdir, genSeries(1, 1, 0, 1))
	b, err := OpenBlock(nil, blockDir, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, false, b.meta.Compaction.Failed)
	testutil.Ok(t, b.setCompactionFailed())
	testutil.Equals(t, true, b.meta.Compaction.Failed)
	testutil.Ok(t, b.Close())

	b, err = OpenBlock(nil, blockDir, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, true, b.meta.Compaction.Failed)
	testutil.Ok(t, b.Close())
}

func TestCreateBlock(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()
	b, err := OpenBlock(nil, createBlock(t, tmpdir, genSeries(1, 1, 0, 10)), nil)
	if err == nil {
		testutil.Ok(t, b.Close())
	}
	testutil.Ok(t, err)
}

func TestCorruptedChunk(t *testing.T) {
	for name, test := range map[string]struct {
		corrFunc func(f *os.File) // Func that applies the corruption.
		expErr   error
	}{
		"invalid header size": {
			func(f *os.File) {
				err := f.Truncate(1)
				testutil.Ok(t, err)
			},
			errors.New("invalid chunk header in segment 0: invalid size"),
		},
		"invalid magic number": {
			func(f *os.File) {
				magicChunksOffset := int64(0)
				_, err := f.Seek(magicChunksOffset, 0)
				testutil.Ok(t, err)

				// Set invalid magic number.
				b := make([]byte, chunks.MagicChunksSize)
				binary.BigEndian.PutUint32(b[:chunks.MagicChunksSize], 0x00000000)
				n, err := f.Write(b)
				testutil.Ok(t, err)
				testutil.Equals(t, chunks.MagicChunksSize, n)
			},
			errors.New("invalid magic number 0"),
		},
		"invalid chunk format version": {
			func(f *os.File) {
				chunksFormatVersionOffset := int64(4)
				_, err := f.Seek(chunksFormatVersionOffset, 0)
				testutil.Ok(t, err)

				// Set invalid chunk format version.
				b := make([]byte, chunks.ChunksFormatVersionSize)
				b[0] = 0
				n, err := f.Write(b)
				testutil.Ok(t, err)
				testutil.Equals(t, chunks.ChunksFormatVersionSize, n)
			},
			errors.New("invalid chunk format version 0"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			tmpdir, err := ioutil.TempDir("", "test_open_block_chunk_corrupted")
			testutil.Ok(t, err)
			defer func() {
				testutil.Ok(t, os.RemoveAll(tmpdir))
			}()

			blockDir := createBlock(t, tmpdir, genSeries(1, 1, 0, 1))
			files, err := sequenceFiles(chunkDir(blockDir))
			testutil.Ok(t, err)
			testutil.Assert(t, len(files) > 0, "No chunk created.")

			f, err := os.OpenFile(files[0], os.O_RDWR, 0666)
			testutil.Ok(t, err)

			// Apply corruption function.
			test.corrFunc(f)
			testutil.Ok(t, f.Close())

			_, err = OpenBlock(nil, blockDir, nil)
			testutil.Equals(t, test.expErr.Error(), err.Error())
		})
	}
}

// TestBlockSize ensures that the block size is calculated correctly.
func TestBlockSize(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test_blockSize")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()

	var (
		blockInit    *Block
		expSizeInit  int64
		blockDirInit string
	)

	// Create a block and compare the reported size vs actual disk size.
	{
		blockDirInit = createBlock(t, tmpdir, genSeries(10, 1, 1, 100))
		blockInit, err = OpenBlock(nil, blockDirInit, nil)
		testutil.Ok(t, err)
		defer func() {
			testutil.Ok(t, blockInit.Close())
		}()
		expSizeInit = blockInit.Size()
		actSizeInit := testutil.DirSize(t, blockInit.Dir())
		testutil.Equals(t, expSizeInit, actSizeInit)
	}

	// Delete some series and check the sizes again.
	{
		testutil.Ok(t, blockInit.Delete(1, 10, labels.NewMustRegexpMatcher("", ".*")))
		expAfterDelete := blockInit.Size()
		testutil.Assert(t, expAfterDelete > expSizeInit, "after a delete the block size should be bigger as the tombstone file should grow %v > %v", expAfterDelete, expSizeInit)
		actAfterDelete := testutil.DirSize(t, blockDirInit)
		testutil.Ok(t, err)
		testutil.Equals(t, expAfterDelete, actAfterDelete, "after a delete reported block size doesn't match actual disk size")

		c, err := NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{0}, nil)
		testutil.Ok(t, err)
		blockDirAfterCompact, err := c.Compact(tmpdir, []string{blockInit.Dir()}, nil)
		testutil.Ok(t, err)
		blockAfterCompact, err := OpenBlock(nil, filepath.Join(tmpdir, blockDirAfterCompact.String()), nil)
		testutil.Ok(t, err)
		defer func() {
			testutil.Ok(t, blockAfterCompact.Close())
		}()
		expAfterCompact := blockAfterCompact.Size()
		actAfterCompact := testutil.DirSize(t, blockAfterCompact.Dir())
		testutil.Assert(t, actAfterDelete > actAfterCompact, "after a delete and compaction the block size should be smaller %v,%v", actAfterDelete, actAfterCompact)
		testutil.Equals(t, expAfterCompact, actAfterCompact, "after a delete and compaction reported block size doesn't match actual disk size")
	}
}

// createBlock creates a block with given set of series and returns its dir.
func createBlock(tb testing.TB, dir string, series []Series) string {
	head := createHead(tb, series)
	compactor, err := NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil)
	testutil.Ok(tb, err)

	testutil.Ok(tb, os.MkdirAll(dir, 0777))

	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	ulid, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime()+1, nil)
	testutil.Ok(tb, err)
	return filepath.Join(dir, ulid.String())
}

func createHead(tb testing.TB, series []Series) *Head {
	head, err := NewHead(nil, nil, nil, 2*60*60*1000)
	testutil.Ok(tb, err)
	defer head.Close()

	app := head.Appender()
	for _, s := range series {
		ref := uint64(0)
		it := s.Iterator()
		for it.Next() {
			t, v := it.At()
			if ref != 0 {
				err := app.AddFast(ref, t, v)
				if err == nil {
					continue
				}
			}
			ref, err = app.Add(s.Labels(), t, v)
			testutil.Ok(tb, err)
		}
		testutil.Ok(tb, it.Err())
	}
	err = app.Commit()
	testutil.Ok(tb, err)
	return head
}

const (
	defaultLabelName  = "labelName"
	defaultLabelValue = "labelValue"
)

// genSeries generates series with a given number of labels and values.
func genSeries(totalSeries, labelCount int, mint, maxt int64) []Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t < maxt; t++ {
			samples = append(samples, sample{t: t, v: rand.Float64()})
		}
		series[i] = newSeries(lbls, samples)
	}
	return series
}

// populateSeries generates series from given labels, mint and maxt.
func populateSeries(lbls []map[string]string, mint, maxt int64) []Series {
	if len(lbls) == 0 {
		return nil
	}

	series := make([]Series, 0, len(lbls))
	for _, lbl := range lbls {
		if len(lbl) == 0 {
			continue
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t <= maxt; t++ {
			samples = append(samples, sample{t: t, v: rand.Float64()})
		}
		series = append(series, newSeries(lbl, samples))
	}
	return series
}

// queryBlockSamples runs a query against a block for all series matching the
// given matchers and returns every returned sample grouped by label set.
func queryBlockSamples(t *testing.T, b BlockReader, mint, maxt int64, ms ...labels.Matcher) map[string][]tsdbutil.Sample {
	t.Helper()

	q, err := NewBlockQuerier(b, mint, maxt)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, q.Close()) }()

	ss, err := q.Select(ms...)
	testutil.Ok(t, err)

	res := make(map[string][]tsdbutil.Sample)
	for ss.Next() {
		s := ss.At()
		samples, err := expandSeriesIterator(s.Iterator())
		testutil.Ok(t, err)
		res[s.Labels().String()] = samples
	}
	testutil.Ok(t, ss.Err())
	return res
}

// TestBlockDeleteBatchedFiltersAtQuery verifies requirement 1 and 4: deletes
// recorded by time interval uniformly filter query results, even when the
// deletion arrives in adjacent batches and the query range is narrower than
// the block range. Out-of-range queries must not return deleted data.
func TestBlockDeleteBatchedFiltersAtQuery(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	const (
		mint int64 = 0
		maxt int64 = 20
	)
	blockDir := createBlock(t, tmpdir, genSeries(2, 1, mint, maxt))
	b, err := OpenBlock(nil, blockDir, nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, b.Close()) }()

	matchAll := labels.NewMustRegexpMatcher("", ".*")

	// Sanity check: all samples are visible before deletion.
	got := queryBlockSamples(t, b, mint, maxt, matchAll)
	testutil.Equals(t, 2, len(got))
	for _, smpls := range got {
		testutil.Equals(t, int(maxt-mint), len(smpls))
	}

	// Two adjacent, separately recorded deletions together cover the whole
	// range of series 0. They must merge into one tombstone range and filter
	// every single sample.
	testutil.Ok(t, b.Delete(0, 9, labels.NewEqualMatcher(defaultLabelName, "0")))
	testutil.Ok(t, b.Delete(10, 19, labels.NewEqualMatcher(defaultLabelName, "0")))

	// Series 0 must now be entirely hidden, while series 1 is untouched.
	got = queryBlockSamples(t, b, mint, maxt, matchAll)
	testutil.Equals(t, 1, len(got))
	for lset, smpls := range got {
		testutil.Equals(t, labels.Labels{{Name: defaultLabelName, Value: "1"}}.String(), lset)
		testutil.Equals(t, int(maxt-mint), len(smpls))
	}

	// A query scoped to the deleted range must return no series at all, and a
	// narrow sub-range inside the tombstone must be empty as well.
	testutil.Equals(t, 0, len(queryBlockSamples(t, b, 0, 19, labels.NewEqualMatcher(defaultLabelName, "0"))))
	testutil.Equals(t, 0, len(queryBlockSamples(t, b, 3, 7, labels.NewEqualMatcher(defaultLabelName, "0"))))
}

// TestBlockCleanTombstonesDropsFullyDeletedBlock verifies requirement 3: when
// every sample in a block is deleted, rewriting it yields an empty block and
// the source block is marked deletable, so the deleted data cannot reappear by
// a later reload reading the source block's tombstoned samples.
func TestBlockCleanTombstonesDropsFullyDeletedBlock(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	compactor, err := NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil)
	testutil.Ok(t, err)

	const (
		mint int64 = 0
		maxt int64 = 20
	)
	blockDir := createBlock(t, tmpdir, genSeries(1, 1, mint, maxt))
	b, err := OpenBlock(nil, blockDir, nil)
	testutil.Ok(t, err)

	matchAll := labels.NewMustRegexpMatcher("", ".*")

	// Delete the whole block in two adjacent batches.
	testutil.Ok(t, b.Delete(0, 9, matchAll))
	testutil.Ok(t, b.Delete(10, 19, matchAll))

	// Querying before cleanup already returns nothing.
	testutil.Equals(t, 0, len(queryBlockSamples(t, b, mint, maxt, matchAll)))

	uid, err := b.CleanTombstones(tmpdir, compactor)
	testutil.Ok(t, err)
	testutil.Assert(t, uid == nil, "rewrite of a fully deleted block must not create a block, got %v", uid)
	testutil.Assert(t, b.Meta().Compaction.Deletable,
		"source block of an empty rewrite must be marked deletable")
	testutil.Ok(t, b.Close())

	// No replacement block directory may exist next to the (now deletable)
	// source block.
	dirs, err := blockDirs(tmpdir)
	testutil.Ok(t, err)
	testutil.Equals(t, []string{blockDir}, dirs)
}

// TestBlockDeletePartialSurvivesCompaction makes sure partial deletions are
// preserved through a block rewrite: deleted samples never reappear and the
// remaining samples keep their exact timestamps and values.
func TestBlockDeletePartialSurvivesCompaction(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	compactor, err := NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil)
	testutil.Ok(t, err)

	const (
		mint int64 = 0
		maxt int64 = 20
	)
	blockDir := createBlock(t, tmpdir, genSeries(1, 1, mint, maxt))
	b, err := OpenBlock(nil, blockDir, nil)
	testutil.Ok(t, err)

	matchAll := labels.NewMustRegexpMatcher("", ".*")

	before := queryBlockSamples(t, b, mint, maxt, matchAll)
	var lset string
	var original []tsdbutil.Sample
	for k, v := range before {
		lset, original = k, v
	}

	// Delete the middle, in two non-adjacent batches.
	testutil.Ok(t, b.Delete(5, 9, matchAll))
	testutil.Ok(t, b.Delete(12, 15, matchAll))

	uid, err := b.CleanTombstones(tmpdir, compactor)
	testutil.Ok(t, err)
	testutil.Assert(t, uid != nil, "expected a replacement block for partial deletion")
	testutil.Ok(t, b.Close())

	rewritten, err := OpenBlock(nil, filepath.Join(tmpdir, uid.String()), nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rewritten.Close()) }()

	after := queryBlockSamples(t, rewritten, mint, maxt, matchAll)
	smpls, ok := after[lset]
	testutil.Assert(t, ok, "series disappeared after partial compaction")

	want := make([]tsdbutil.Sample, 0, len(original))
	for _, s := range original {
		ts := s.T()
		if (ts >= 5 && ts <= 9) || (ts >= 12 && ts <= 15) {
			continue
		}
		want = append(want, s)
	}
	testutil.Equals(t, want, smpls)
}

// TestCompactMergesTombstonesAcrossBlocks verifies requirement 2 and 3 on the
// multi-block compaction path: when the same series carries tombstones in
// several source blocks (including a new interval that fully contains an older
// one), the merged block physically drops every deleted sample and keeps the
// rest. If all samples disappear, no block is written and the sources are
// marked deletable.
func TestCompactMergesTombstonesAcrossBlocks(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	compactor, err := NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil)
	testutil.Ok(t, err)

	matchAll := labels.NewMustRegexpMatcher("", ".*")

	// Two sequential blocks, each holding one series with samples [0,20) and
	// [20,40). They share the same label set so compaction merges them into
	// one series.
	dir1 := createBlock(t, tmpdir, genSeries(1, 1, 0, 20))
	block1, err := OpenBlock(nil, dir1, nil)
	testutil.Ok(t, err)

	secondDir := filepath.Join(tmpdir, "second")
	testutil.Ok(t, os.MkdirAll(secondDir, 0777))
	dir2 := createBlock(t, secondDir, genSeries(1, 1, 20, 40))
	block2, err := OpenBlock(nil, dir2, nil)
	testutil.Ok(t, err)

	testutil.Ok(t, block1.Close())
	testutil.Ok(t, block2.Close())

	// Re-open and record deletes in separate batches. Block 1 gets a narrow
	// delete first, then a containing delete; block 2 deletes an adjacent range.
	block1, err = OpenBlock(nil, dir1, nil)
	testutil.Ok(t, err)
	block2, err = OpenBlock(nil, dir2, nil)
	testutil.Ok(t, err)

	testutil.Ok(t, block1.Delete(5, 8, matchAll))
	testutil.Ok(t, block1.Delete(3, 12, matchAll)) // contains [5,8]
	testutil.Ok(t, block2.Delete(25, 30, matchAll))

	sourceUIDs := []ulid.ULID{block1.Meta().ULID, block2.Meta().ULID}
	testutil.Ok(t, block1.Close())
	testutil.Ok(t, block2.Close())

	uid, err := compactor.Compact(tmpdir, []string{dir1, dir2}, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, uid != (ulid.ULID{}), "expected a merged block since not all samples were deleted")

	merged, err := OpenBlock(nil, filepath.Join(tmpdir, uid.String()), nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, merged.Close()) }()

	got := queryBlockSamples(t, merged, 0, 40, matchAll)
	testutil.Equals(t, 1, len(got))
	var smpls []tsdbutil.Sample
	for _, v := range got {
		smpls = v
	}

	deleted := func(ts int64) bool {
		return (ts >= 3 && ts <= 12) || (ts >= 25 && ts <= 30)
	}
	var expectedTs []int64
	for ts := int64(0); ts < 40; ts++ {
		if !deleted(ts) {
			expectedTs = append(expectedTs, ts)
		}
	}
	testutil.Equals(t, len(expectedTs), len(smpls))
	for i, s := range smpls {
		testutil.Equals(t, expectedTs[i], s.T())
	}

	// The merged block must not carry tombstones anymore: the deletions were
	// materialised into the rewritten chunks.
	tr, _, err := readTombstones(merged.Dir())
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(0), tr.Total())

	// For a non-empty merge the source blocks are reclaimed through the new
	// block's parent references on the next DB.reload() (DB.deletableBlocks),
	// not through the Deletable flag, which is reserved for empty rewrites.
	parentSet := map[ulid.ULID]bool{}
	for _, p := range merged.Meta().Compaction.Parents {
		parentSet[p.ULID] = true
	}
	for _, id := range sourceUIDs {
		testutil.Assert(t, parentSet[id], "merged block does not reference source block %s", id)
	}
}

// TestCompactFullyDeletedBlocksDropsThem covers requirement 3 for the
// multi-block path: compacting blocks whose union is entirely deleted writes
// nothing and marks every source block deletable.
func TestCompactFullyDeletedBlocksDropsThem(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	compactor, err := NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil)
	testutil.Ok(t, err)

	matchAll := labels.NewMustRegexpMatcher("", ".*")

	dir1 := createBlock(t, tmpdir, genSeries(1, 1, 0, 20))
	secondDir := filepath.Join(tmpdir, "second")
	testutil.Ok(t, os.MkdirAll(secondDir, 0777))
	dir2 := createBlock(t, secondDir, genSeries(1, 1, 20, 40))

	for _, d := range []string{dir1, dir2} {
		b, err := OpenBlock(nil, d, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, b.Delete(0, 40, matchAll))
		testutil.Ok(t, b.Close())
	}

	uid, err := compactor.Compact(tmpdir, []string{dir1, dir2}, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, ulid.ULID{}, uid)

	for _, d := range []string{dir1, dir2} {
		meta, _, err := readMetaFile(d)
		testutil.Ok(t, err)
		testutil.Assert(t, meta.Compaction.Deletable, "fully deleted source block not marked deletable: %s", d)
	}
}
