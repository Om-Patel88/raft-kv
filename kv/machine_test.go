package kv

import (
	"errors"
	"reflect"
	"testing"
)

func TestMachineOps(t *testing.T) {
	m := newMachine()
	seq := uint64(0)
	do := func(r Request) Response {
		seq++
		r.ClientID, r.Seq = "c1", seq
		return m.apply(r)
	}

	if r := do(Request{Op: OpGet, Key: "a"}); r.Found {
		t.Fatalf("get on empty store found %q", r.Value)
	}
	do(Request{Op: OpPut, Key: "a", Value: "1"})
	if r := do(Request{Op: OpGet, Key: "a"}); !r.Found || r.Value != "1" {
		t.Fatalf("get a = %+v, want 1", r)
	}
	do(Request{Op: OpAppend, Key: "a", Value: "23"})
	do(Request{Op: OpAppend, Key: "new", Value: "x"})
	if v := m.data["a"]; v != "123" {
		t.Fatalf("after append a = %q", v)
	}
	if v := m.data["new"]; v != "x" {
		t.Fatalf("append to missing key gave %q", v)
	}

	if r := do(Request{Op: OpCAS, Key: "a", Expected: "nope", Value: "z"}); r.Swapped {
		t.Fatal("cas with wrong expected value swapped")
	}
	if r := do(Request{Op: OpCAS, Key: "a", Expected: "123", Value: "z"}); !r.Swapped {
		t.Fatal("cas with right expected value did not swap")
	}
	if r := do(Request{Op: OpCAS, Key: "fresh", Expected: "", Value: "v"}); !r.Swapped || m.data["fresh"] != "v" {
		t.Fatal("cas on a missing key with empty expected should create it")
	}

	if r := do(Request{Op: OpDelete, Key: "a"}); !r.Found {
		t.Fatal("delete of existing key reported not found")
	}
	if r := do(Request{Op: OpDelete, Key: "a"}); r.Found {
		t.Fatal("second delete reported found")
	}
	if _, ok := m.data["a"]; ok {
		t.Fatal("a still present after delete")
	}
}

func TestMachineDedup(t *testing.T) {
	m := newMachine()
	app := Request{Op: OpAppend, Key: "k", Value: "x", ClientID: "c1", Seq: 1}
	m.apply(app)
	m.apply(app) // retry that got committed twice
	if v := m.data["k"]; v != "x" {
		t.Fatalf("duplicate append applied twice: %q", v)
	}

	cas := Request{Op: OpCAS, Key: "k", Expected: "x", Value: "y", ClientID: "c1", Seq: 2}
	if r := m.apply(cas); !r.Swapped {
		t.Fatal("first cas should swap")
	}
	// The retry must get the original answer, not a fresh evaluation
	// (which would say false now that the value is y).
	if r := m.apply(cas); !r.Swapped {
		t.Fatal("retried cas lost its original result")
	}

	// a stale seq from before the last one is ignored
	m.apply(Request{Op: OpPut, Key: "k", Value: "old", ClientID: "c1", Seq: 1})
	if v := m.data["k"]; v != "y" {
		t.Fatalf("stale request was applied: %q", v)
	}

	// other clients have their own sequence numbers
	m.apply(Request{Op: OpAppend, Key: "k", Value: "!", ClientID: "c2", Seq: 1})
	if v := m.data["k"]; v != "y!" {
		t.Fatalf("other client's seq 1 was treated as a duplicate: %q", v)
	}

	// no client id means no dedup
	anon := Request{Op: OpAppend, Key: "anon", Value: "a"}
	m.apply(anon)
	m.apply(anon)
	if v := m.data["anon"]; v != "aa" {
		t.Fatalf("anonymous appends: %q", v)
	}
	if len(m.sessions) != 2 {
		t.Fatalf("expected 2 sessions, have %v", m.sessions)
	}
}

func TestMachineSnapshotRoundTrip(t *testing.T) {
	m := newMachine()
	m.apply(Request{Op: OpPut, Key: "a", Value: "1", ClientID: "c1", Seq: 1})
	m.apply(Request{Op: OpAppend, Key: "b", Value: "2", ClientID: "c2", Seq: 7})
	m.apply(Request{Op: OpCAS, Key: "a", Expected: "1", Value: "3", ClientID: "c1", Seq: 2})

	b, err := m.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := newMachine()
	r.apply(Request{Op: OpPut, Key: "junk", Value: "gone"})
	if err := r.restore(b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.data, r.data) {
		t.Fatalf("data: got %v, want %v", r.data, m.data)
	}
	if !reflect.DeepEqual(m.sessions, r.sessions) {
		t.Fatalf("sessions: got %v, want %v", r.sessions, m.sessions)
	}
	// the restored dedup table still recognises the old requests
	if resp := r.apply(Request{Op: OpCAS, Key: "a", Expected: "1", Value: "3", ClientID: "c1", Seq: 2}); !resp.Swapped {
		t.Fatal("restored session lost the cached cas result")
	}
	r.apply(Request{Op: OpAppend, Key: "b", Value: "2", ClientID: "c2", Seq: 7})
	if r.data["b"] != "2" {
		t.Fatalf("duplicate applied after restore: b=%q", r.data["b"])
	}

	empty := newMachine()
	b, _ = empty.snapshot()
	if err := r.restore(b); err != nil || len(r.data) != 0 || r.data == nil {
		t.Fatalf("restoring an empty snapshot: err=%v data=%v", err, r.data)
	}
}

func TestRequestValidate(t *testing.T) {
	for _, r := range []Request{{Op: "nope", Key: "k"}, {Op: OpPut}} {
		if err := r.validate(); !errors.Is(err, ErrBadRequest) {
			t.Errorf("%+v: got %v, want ErrBadRequest", r, err)
		}
	}
	b, _ := encodeRequest(Request{Op: OpCAS, Key: "k", Value: "n", Expected: "o", ClientID: "c", Seq: 3})
	got, err := decodeRequest(b)
	if err != nil || got != (Request{Op: OpCAS, Key: "k", Value: "n", Expected: "o", ClientID: "c", Seq: 3}) {
		t.Fatalf("round trip: %+v %v (%s)", got, err, b)
	}
}
