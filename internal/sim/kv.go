package sim

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
)

const (
	maxKeyLen   = 32
	maxValueLen = 64
)

// machine is the tiny key-value store each node applies committed
// commands to. It survives a node being restarted for a speed change, but
// not a kill, so a killed node really does have to rebuild from its log.
type machine struct {
	mu      sync.Mutex
	data    map[string]string
	applied uint64 // index of the last command or snapshot applied
	snapAt  uint64 // index of the last snapshot we asked raft to take
}

func newMachine() *machine {
	return &machine{data: map[string]string{}}
}

// parseCommand accepts "set key value" and "del key". The value may
// contain spaces.
func parseCommand(s string) (op, key, val string, err error) {
	f := strings.Fields(s)
	if len(f) == 0 {
		return "", "", "", errors.New("empty command")
	}
	op = strings.ToLower(f[0])
	switch op {
	case "set":
		if len(f) < 3 {
			return "", "", "", errors.New("usage: set key value")
		}
		key, val = f[1], strings.Join(f[2:], " ")
	case "del":
		if len(f) != 2 {
			return "", "", "", errors.New("usage: del key")
		}
		key = f[1]
	default:
		return "", "", "", errors.New("unknown command " + f[0] + " (use set or del)")
	}
	if len(key) > maxKeyLen {
		return "", "", "", errors.New("key too long")
	}
	if len(val) > maxValueLen {
		return "", "", "", errors.New("value too long")
	}
	return op, key, val, nil
}

// apply runs the command at index unless it has already been applied.
func (m *machine) apply(index uint64, cmd []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index <= m.applied {
		return false
	}
	m.applied = index
	op, key, val, err := parseCommand(string(cmd))
	if err != nil {
		return true
	}
	if op == "set" {
		m.data[key] = val
	} else {
		delete(m.data, key)
	}
	return true
}

func (m *machine) restore(index uint64, snap []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index <= m.applied {
		return
	}
	data := map[string]string{}
	if len(snap) > 0 {
		if err := json.Unmarshal(snap, &data); err != nil {
			data = map[string]string{}
		}
	}
	m.data = data
	m.applied = index
	m.snapAt = index
}

// snapshotDue returns an encoded copy of the data if at least every
// commands have been applied since the last snapshot.
func (m *machine) snapshotDue(every uint64) (uint64, []byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if every == 0 || m.applied < m.snapAt+every {
		return 0, nil, false
	}
	b, err := json.Marshal(m.data)
	if err != nil {
		return 0, nil, false
	}
	m.snapAt = m.applied
	return m.applied, b, true
}

func (m *machine) copyData() (map[string]string, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out, m.applied
}
