# raft-kv

[![ci](https://github.com/Om-Patel88/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/Om-Patel88/raft-kv/actions/workflows/ci.yml)

A distributed key-value store built on my own implementation of the [Raft](https://raft.github.io/raft.pdf) consensus algorithm, written in Go with only the standard library.

A cluster of nodes keeps a replicated log. As long as a majority of them are up and can talk to each other, the store keeps accepting reads and writes, and it never returns stale or conflicting data, even while nodes crash, restart or get cut off from each other.

**Live demo:** https://om-patel88.github.io/raft-kv/

The demo runs five nodes in your browser. It is the same Raft code as the real server, compiled to WebAssembly and connected through a simulated network, so you can kill the leader, split the network or drop messages and watch the cluster sort itself out.

## What's in here

- Leader election with randomized timeouts
- Log replication, with fast backtracking when a follower's log has diverged
- Persistence: a write-ahead log with CRC-checked records and fsync, so a node can crash at any point and recover
- Snapshots and log compaction, including InstallSnapshot for followers that fall too far behind
- A key-value state machine with `get`, `put`, `delete`, `append` and compare-and-swap
- Linearizable reads and writes, and exactly-once writes when a client retries
- HTTP API, a Go client that finds and follows the leader, and a small CLI
- A simulated network for tests that can delay, drop and partition messages
- The browser visualizer

Cluster membership is fixed at startup. Adding and removing nodes at runtime is not implemented.

## Try it locally

You need Go 1.24 or newer.

Start three nodes on ports 8001 to 8003:

```
# Linux / macOS
./scripts/cluster.sh

# Windows (PowerShell)
.\scripts\cluster.ps1
```

Then from another terminal:

```
$ export RAFTKV_SERVERS=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003

$ ./bin/kvctl put greeting hello
$ ./bin/kvctl append greeting ", world"
$ ./bin/kvctl get greeting
hello, world

$ ./bin/kvctl put counter 1
$ ./bin/kvctl cas counter 1 2
swapped

$ ./bin/kvctl status
NODE  ADDR            ROLE      TERM  COMMIT  LAST  LEADER  KEYS
n1    127.0.0.1:8001  follower  1     6       6     n3      2
n2    127.0.0.1:8002  follower  1     6       6     n3      2
n3    127.0.0.1:8003  leader    1     6       6     n3      2
```

Now kill the leader's process and run `kvctl get greeting` again. One of the other two takes over within about a second and the data is still there. Start the dead node again with the same command line and it catches up from the new leader.

To run a node by hand:

```
./bin/raftkv -id n1 -peers n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003 -data ./data/n1
```

`raftkv -h` lists the timing and snapshot flags.

### HTTP API

Any node accepts requests. A follower answers `421` with the leader's address, which the client library and `kvctl` follow automatically.

| Request | Result |
|---|---|
| `GET /kv/{key}` | `200 {"value": "..."}` or `404` |
| `PUT /kv/{key}` with the value as the body | `204` |
| `DELETE /kv/{key}` | `204` |
| `POST /kv/{key}/append` with the text as the body | `204` |
| `POST /kv/{key}/cas` with `{"expected": "a", "new": "b"}` | `200 {"swapped": true}` |
| `GET /status` | this node's role, term, commit index, leader |

```
curl -X PUT -d hello http://127.0.0.1:8001/kv/greeting
curl http://127.0.0.1:8001/kv/greeting
```

### Running the visualizer locally

```
./scripts/build-web.sh        # or .\scripts\build-web.ps1
python -m http.server 8000 --directory web
```

and open http://localhost:8000.

## How it's put together

```
raft/        the consensus library: election, replication, snapshots
raft/wal/    file-backed storage (log, term/vote, snapshot)
simnet/      in-memory network with delays, drops and partitions
kv/          the key-value state machine and the store that drives it through raft
server/      HTTP API, and the HTTP transport nodes use to talk to each other
client/      Go client
cmd/raftkv   the server binary
cmd/kvctl    the CLI
cmd/raftsim  WebAssembly entry point for the demo
internal/sim cluster controller behind the demo
web/         the visualizer (plain HTML, CSS and JS)
```

The Raft library knows nothing about key-value stores, HTTP or disks. It talks to the outside world through three things:

- a `Transport` to send RPCs to peers,
- a `Storage` to persist its term, vote, log and snapshot,
- a channel it delivers committed entries on.

That separation is what makes the rest possible. The real server plugs in an HTTP transport and the write-ahead log. The tests plug in `simnet` and can then cut any link they want. The browser demo plugs in `simnet` and in-memory storage, and runs the unchanged library as WebAssembly.

A few decisions worth mentioning:

**One lock per node.** All Raft state sits behind a single mutex that is never held across a network call or a send on the apply channel. It is not the fastest design but it is easy to reason about, and CI runs every test under the race detector.

**Reads go through the log.** A `get` is committed like a write before it is answered, so a leader that has been partitioned away can never serve stale data. It costs a round trip to a majority per read. ReadIndex or leader leases would be cheaper and are the obvious next step.

**Retries are safe.** Every client request carries a client id and sequence number, and the state machine remembers the last one it applied for each client. If a reply gets lost and the client retries, an `append` is not applied twice. That table is saved in snapshots too.

**A new leader commits a no-op first.** Raft only lets a leader commit entries from its own term directly, so without this, entries left over from an earlier term would sit uncommitted until the next client write.

**Crashing on storage errors.** If an fsync fails the node exits rather than carry on with state it may not have saved.

## Testing

```
go test ./...
```

Most of the work in this project went into the tests. They run whole clusters in one process on the simulated network and break things on purpose:

- elections after the leader is disconnected, with and without a quorum
- a partitioned old leader accepting writes that must be thrown away once it rejoins
- followers with long conflicting logs being repaired
- crashing and restarting followers, leaders and the whole cluster, on both in-memory storage and the real write-ahead log
- the Figure 8 scenario from the paper, repeated a few hundred times on a lossy network
- followers catching up through snapshots
- torn and corrupted records in the write-ahead log

While these run, the harness checks continuously that no two nodes ever apply different commands at the same log index and that no term has two leaders.

On the key-value side there is a chaos test: several clients append unique tokens while another goroutine randomly partitions, crashes and restarts nodes. At the end every acknowledged append must be present exactly once and in order, and all nodes must hold identical data. There is also a small linearizability checker for a single register that verifies histories recorded from concurrent clients during a partition.

The full suite takes about a minute and a half.

## Limitations

- No membership changes.
- No PreVote, so a node that rejoins after being isolated forces one unnecessary election.
- Snapshots are sent in one piece and the state machine is serialized as JSON, which is fine for a demo-sized store but not for a large one.
- No TLS or authentication between nodes.

## References

- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) (extended version)
- Diego Ongaro, [Consensus: Bridging Theory and Practice](https://github.com/ongardie/dissertation)
