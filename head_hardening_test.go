package tsdb

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

func TestHeadConcurrentAddSameLabelSetDeduplicates(t *testing.T) {
	h, err := NewHead(nil, nil, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	lset := labels.FromStrings("job", "api", "zone", "a")
	refs := make(chan uint64, 64)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app := h.Appender()
			ref, err := app.Add(lset, 1, 1)
			if err != nil {
				t.Error(err)
				_ = app.Rollback()
				return
			}
			if err := app.Commit(); err != nil {
				t.Error(err)
				return
			}
			refs <- ref
		}()
	}
	wg.Wait()
	close(refs)

	seen := map[uint64]struct{}{}
	for ref := range refs {
		seen[ref] = struct{}{}
	}
	if len(seen) != 1 {
		t.Fatalf("expected one series ref, got %v", seen)
	}

	p, err := h.Index()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	postings, err := p.Postings("job", "api")
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for postings.Next() {
		ids = append(ids, postings.At())
	}
	if err := postings.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected one posting ID, got %v", ids)
	}
}

func TestHeadConcurrentAddAndTruncateHasNoDuplicatePostings(t *testing.T) {
	h, err := NewHead(nil, nil, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	lset := labels.FromStrings("job", "api")
	var nextT int64
	stop := make(chan struct{})
	var truncator sync.WaitGroup
	truncator.Add(1)
	go func() {
		defer truncator.Done()
		for {
			select {
			case <-stop:
				return
			default:
				cur := atomic.LoadInt64(&nextT)
				if cur > 20 {
					_ = h.Truncate(cur - 10)
				}
			}
		}
	}()

	var writers sync.WaitGroup
	for g := 0; g < 8; g++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; i < 100; i++ {
				ts := atomic.AddInt64(&nextT, 1)
				app := h.Appender()
				ref, err := app.Add(lset, ts, float64(ts))
				if err != nil {
					_ = app.Rollback()
					continue
				}
				if err := app.Commit(); err != nil {
					t.Error(err)
					return
				}
				_ = ref
			}
		}()
	}
	writers.Wait()
	close(stop)
	truncator.Wait()

	h.postings.EnsureOrder()
	postings := h.postings.Get("job", "api")
	var ids []uint64
	for postings.Next() {
		ids = append(ids, postings.At())
	}
	if err := postings.Err(); err != nil {
		t.Fatal(err)
	}
	seen := map[uint64]struct{}{}
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate posting ID %d in %v", id, ids)
		}
		seen[id] = struct{}{}
	}
}

func TestHeadCloseIsIdempotentAndRejectsWrites(t *testing.T) {
	h, err := NewHead(nil, nil, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second close returned error: %v", err)
	}
	if !h.Closed() {
		t.Fatal("Closed returned false after Close")
	}

	app := h.Appender()
	if _, err := app.Add(labels.FromStrings("a", "b"), 1, 1); err == nil {
		t.Fatal("Add after Close unexpectedly succeeded")
	}
	if err := app.AddFast(1, 1, 1); err == nil {
		t.Fatal("AddFast after Close unexpectedly succeeded")
	}
	if err := app.Commit(); err == nil {
		t.Fatal("Commit after Close unexpectedly succeeded")
	}
	if err := app.Rollback(); err == nil {
		t.Fatal("Rollback after Close unexpectedly succeeded")
	}
}

func TestHeadCanonicalizesUnorderedLabelSets(t *testing.T) {
	h, err := NewHead(nil, nil, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	a := labels.Labels{{Name: "b", Value: "2"}, {Name: "a", Value: "1"}}
	b := labels.Labels{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}
	s1, created1 := h.getOrCreate(a.Hash(), a)
	s2, created2 := h.getOrCreate(b.Hash(), b)
	if !created1 || created2 {
		t.Fatalf("creation flags = (%v,%v), want (true,false)", created1, created2)
	}
	if s1.ref != s2.ref {
		t.Fatalf("refs differ for same label set: %d != %d", s1.ref, s2.ref)
	}
}

func TestHeadStartedQueryChunkSurvivesTruncate(t *testing.T) {
	h, err := NewHead(nil, nil, nil, 10000)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	lset := labels.FromStrings("job", "api")
	app := h.Appender()
	ref, err := app.Add(lset, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for ts := int64(2); ts <= 20; ts++ {
		if err := app.AddFast(ref, ts, float64(ts)); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}

	idx := h.indexRange(0, 100)
	var gotLabels labels.Labels
	var chks []chunks.Meta
	if err := idx.Series(ref, &gotLabels, &chks); err != nil {
		t.Fatal(err)
	}
	if len(chks) != 1 || chks[0].Chunk == nil {
		t.Fatalf("expected one preloaded chunk, got %+v", chks)
	}

	if err := h.Truncate(1000); err != nil {
		t.Fatal(err)
	}

	it := chks[0].Chunk.Iterator(nil)
	var count int
	for it.Next() {
		count++
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 20 {
		t.Fatalf("started query saw %d samples after cut/truncate, want 20", count)
	}
}

func TestDBCutHeadIfMemoryExceeded(t *testing.T) {
	db, err := Open(t.TempDir(), nil, nil, &Options{
		RetentionDuration: 2 * 60 * 60 * 1000,
		BlockRanges:       []int64{1000000},
		WALSegmentSize:    -1,
		NoLockfile:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	app := db.Appender()
	for i := 0; i < 100; i++ {
		lset := labels.FromStrings("job", "api", "i", string(rune('a'+i%26)))
		if _, err := app.Add(lset, int64(i+1), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}

	if db.head.compactable() {
		t.Fatal("test setup should not be time-compactable")
	}
	cut, err := db.CutHeadIfMemoryExceeded(1)
	if err != nil {
		t.Fatal(err)
	}
	if !cut {
		t.Fatal("expected memory limit to trigger a cut")
	}
	if blocks := db.Blocks(); len(blocks) == 0 {
		t.Fatal("expected a persisted block after memory cut")
	}
}

func TestDBCloseIsIdempotentAndRejectsWrites(t *testing.T) {
	db, err := Open(t.TempDir(), nil, nil, &Options{
		RetentionDuration: 2 * 60 * 60 * 1000,
		BlockRanges:       []int64{1000},
		WALSegmentSize:    -1,
		NoLockfile:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second DB close: %v", err)
	}

	app := db.Appender()
	if _, err := app.Add(labels.FromStrings("a", "b"), 1, 1); err == nil {
		t.Fatal("DB Appender Add after Close unexpectedly succeeded")
	}
}
