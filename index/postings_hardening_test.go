package index

import (
	"sync"
	"testing"

	"github.com/prometheus/tsdb/labels"
)

func TestMemPostingsGetReturnsStableSnapshot(t *testing.T) {
	p := NewMemPostings()
	lset := labels.FromStrings("job", "api")
	p.Add(1, lset)

	q := p.Get("job", "api")
	p.Add(2, lset)
	p.Delete(map[uint64]struct{}{1: {}})

	var got []uint64
	for q.Next() {
		got = append(got, q.At())
	}
	if err := q.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("snapshot changed after concurrent-style mutation: %v", got)
	}

	got = got[:0]
	q = p.Get("job", "api")
	for q.Next() {
		got = append(got, q.At())
	}
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("current view = %v, want [2]", got)
	}
}

func TestMemPostingsConcurrentAddDeleteQuery(t *testing.T) {
	p := NewMemPostings()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := uint64(g*200 + i)
				v := string(rune('a' + i%4))
				p.Add(id, labels.FromStrings("job", "x", "v", v))
				q := p.Get("v", v)
				for q.Next() {
					_ = q.At()
				}
				if i%25 == 0 {
					p.Delete(map[uint64]struct{}{id: {}})
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestMemPostingsCloseIdempotent(t *testing.T) {
	p := NewMemPostings()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	q := p.Get("a", "b")
	if q.Next() {
		t.Fatal("closed postings returned a series")
	}
	if err := q.Err(); err != ErrClosed {
		t.Fatalf("query error = %v, want %v", err, ErrClosed)
	}
}
