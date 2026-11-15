//go:build js && wasm

// Command raftsim is the browser demo: a five node cluster from
// internal/sim compiled to WebAssembly and driven from web/app.js through
// a global raftsim object.
package main

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/Om-Patel88/raft-kv/internal/sim"
)

func main() {
	s := sim.New(sim.Options{})

	api := map[string]any{
		// state and messages return JSON strings; JSON.parse is much
		// cheaper than building nested objects through syscall/js
		"state":    fn(func(args []js.Value) any { return toJSON(s.State()) }),
		"messages": fn(func(args []js.Value) any { return toJSON(s.Messages()) }),

		"kill":       fn(func(args []js.Value) any { return errOrNull(s.Kill(str(args, 0))) }),
		"restart":    fn(func(args []js.Value) any { return errOrNull(s.Restart(str(args, 0))) }),
		"disconnect": fn(func(args []js.Value) any { return errOrNull(s.Disconnect(str(args, 0))) }),
		"connect":    fn(func(args []js.Value) any { return errOrNull(s.Connect(str(args, 0))) }),
		"heal":       fn(func(args []js.Value) any { s.Heal(); return nil }),
		"reset":      fn(func(args []js.Value) any { s.Reset(); return nil }),

		"partition": fn(func(args []js.Value) any {
			var groups [][]string
			if len(args) > 0 {
				g := args[0]
				for i := 0; i < g.Length(); i++ {
					var ids []string
					for j := 0; j < g.Index(i).Length(); j++ {
						ids = append(ids, g.Index(i).Index(j).String())
					}
					groups = append(groups, ids)
				}
			}
			return errOrNull(s.Partition(groups))
		}),

		"propose": fn(func(args []js.Value) any {
			r := s.Propose(str(args, 0), str(args, 1))
			return map[string]any{
				"ok": r.OK, "seq": r.Seq, "node": r.Node,
				"index": int(r.Index), "term": int(r.Term), "error": r.Error,
			}
		}),

		"setDropRate": fn(func(args []js.Value) any { s.SetDropRate(num(args, 0)); return nil }),
		"setSpeed":    fn(func(args []js.Value) any { s.SetSpeed(num(args, 0)); return nil }),
	}
	js.Global().Set("raftsim", js.ValueOf(api))

	if ready := js.Global().Get("raftsimReady"); ready.Type() == js.TypeFunction {
		ready.Invoke()
	}
	select {}
}

// fn wraps f so a panic comes back to JavaScript as an exception-like
// error string instead of killing the Go runtime for good.
func fn(f func(args []js.Value) any) js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) (out any) {
		defer func() {
			if r := recover(); r != nil {
				out = fmt.Sprint("raftsim: ", r)
			}
		}()
		return f(args)
	})
}

func toJSON(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

func errOrNull(err error) any {
	if err != nil {
		return err.Error()
	}
	return nil
}

func str(args []js.Value, i int) string {
	if i >= len(args) || args[i].Type() != js.TypeString {
		return ""
	}
	return args[i].String()
}

func num(args []js.Value, i int) float64 {
	if i >= len(args) || args[i].Type() != js.TypeNumber {
		return 0
	}
	return args[i].Float()
}
