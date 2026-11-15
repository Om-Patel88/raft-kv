// Command kvctl is a command-line client for a raftkv cluster.
//
//	kvctl -servers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 put greeting hello
//	kvctl get greeting
//	kvctl status
//
// The server list can also come from RAFTKV_SERVERS.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Om-Patel88/raft-kv/client"
)

const (
	exitOK       = 0
	exitError    = 1
	exitUsage    = 2
	exitNotFound = 3
)

const usage = `usage: kvctl [-servers host:port,...] [-timeout d] <command> [args]

commands:
  get <key>                     print the value (exit 3 if missing)
  put <key> <value>             set a value
  del <key>                     delete a key
  append <key> <value>          append to a value
  cas <key> <expected> <new>    set to new if the value is expected
  status                        show every node's raft state

servers default to $RAFTKV_SERVERS, then 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	servers := fs.String("servers", "", "comma-separated server addresses")
	timeout := fs.Duration("timeout", 10*time.Second, "give up after this long")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	list := *servers
	if list == "" {
		list = os.Getenv("RAFTKV_SERVERS")
	}
	if list == "" {
		list = "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
	}
	c := client.New(strings.Split(list, ","))
	defer c.Close()

	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return exitUsage
	}
	cmd, rest := rest[0], rest[1:]
	want := map[string]int{"get": 1, "put": 2, "del": 1, "append": 2, "cas": 3, "status": 0}
	n, ok := want[cmd]
	if !ok {
		fmt.Fprintf(stderr, "kvctl: unknown command %q\n\n", cmd)
		fs.Usage()
		return exitUsage
	}
	if len(rest) != n {
		fmt.Fprintf(stderr, "kvctl: %s takes %d argument(s), got %d\n\n", cmd, n, len(rest))
		fs.Usage()
		return exitUsage
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var err error
	switch cmd {
	case "get":
		var v string
		var found bool
		v, found, err = c.Get(ctx, rest[0])
		if err == nil && !found {
			fmt.Fprintf(stderr, "kvctl: %s: not found\n", rest[0])
			return exitNotFound
		}
		if err == nil {
			fmt.Fprintln(stdout, v)
		}
	case "put":
		err = c.Put(ctx, rest[0], rest[1])
	case "del":
		err = c.Delete(ctx, rest[0])
	case "append":
		err = c.Append(ctx, rest[0], rest[1])
	case "cas":
		var swapped bool
		swapped, err = c.CAS(ctx, rest[0], rest[1], rest[2])
		if err == nil {
			if swapped {
				fmt.Fprintln(stdout, "swapped")
			} else {
				fmt.Fprintln(stdout, "not swapped")
			}
		}
	case "status":
		return status(ctx, c, stdout)
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprintf(stderr, "kvctl: no answer from the cluster within %v: %v\n", *timeout, err)
		} else {
			fmt.Fprintf(stderr, "kvctl: %v\n", err)
		}
		return exitError
	}
	return exitOK
}

// status asks every server at once and prints one row per server. It
// fails only if none of them answered.
func status(ctx context.Context, c *client.Client, out io.Writer) int {
	servers := c.Servers()
	rows := make([]string, len(servers))
	up := 0
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i, addr := range servers {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			st, err := c.Status(sctx, addr)
			if err != nil {
				rows[i] = fmt.Sprintf("%s\t%s\tdown\t-\t-\t-\t-\t-", "?", addr)
				return
			}
			leader := st.Leader
			if leader == "" {
				leader = "-"
			}
			rows[i] = fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%s\t%d",
				st.ID, addr, strings.ToLower(st.Role.String()), st.Term, st.CommitIndex, st.LastIndex, leader, st.Keys)
			mu.Lock()
			up++
			mu.Unlock()
		}(i, addr)
	}
	wg.Wait()

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tADDR\tROLE\tTERM\tCOMMIT\tLAST\tLEADER\tKEYS")
	for _, r := range rows {
		fmt.Fprintln(tw, r)
	}
	tw.Flush()
	if up == 0 {
		return exitError
	}
	return exitOK
}
