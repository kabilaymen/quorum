//go:build js && wasm

// Command wasm exposes the demo simulation to the page as window.quorum.
// It only converts between JavaScript values and demo/sim: no logic here.
package main

import (
	"encoding/json"
	"syscall/js"

	"github.com/kabilaymen/quorum/demo/sim"
	"github.com/kabilaymen/quorum/raft"
)

func main() {
	s := sim.New(sim.DefaultConfig(1))
	id := func(a []js.Value) raft.NodeID { return raft.NodeID(a[0].Int()) }
	fn := func(f func(a []js.Value) any) js.Func {
		return js.FuncOf(func(_ js.Value, a []js.Value) any { return f(a) })
	}
	js.Global().Set("quorum", js.ValueOf(map[string]any{
		"step": fn(func(a []js.Value) any { return s.Step(a[0].Int()) }),
		"state": fn(func([]js.Value) any {
			b, _ := json.Marshal(s.State())
			return string(b)
		}),
		"kill":        fn(func(a []js.Value) any { s.Kill(id(a)); return nil }),
		"restart":     fn(func(a []js.Value) any { s.Restart(id(a)); return nil }),
		"isolate":     fn(func(a []js.Value) any { s.Isolate(id(a), a[1].Bool()); return nil }),
		"heal":        fn(func([]js.Value) any { s.Heal(); return nil }),
		"put":         fn(func(a []js.Value) any { return s.Put(a[0].String(), a[1].String()) }),
		"setDropRate": fn(func(a []js.Value) any { s.SetDropRate(a[0].Float()); return nil }),
		"reset": fn(func(a []js.Value) any {
			s = sim.New(sim.DefaultConfig(int64(a[0].Int())))
			return nil
		}),
	}))
	js.Global().Call("dispatchEvent", js.Global().Get("Event").New("quorum-ready"))
	select {} // keep the Go runtime alive for the callbacks
}
