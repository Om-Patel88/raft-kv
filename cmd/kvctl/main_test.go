package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"frobnicate"},
		{"get"},
		{"put", "k"},
		{"cas", "k", "a"},
		{"-nosuchflag", "get", "k"},
	} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != exitUsage {
			t.Errorf("%v: exit %d, want %d (stderr %q)", args, code, exitUsage, errOut.String())
		}
	}
}

func TestStatusAllDown(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"-servers", "127.0.0.1:1", "-timeout", "500ms", "status"}, &out, &errOut)
	if code != exitError {
		t.Fatalf("exit %d, want %d", code, exitError)
	}
	if !strings.Contains(out.String(), "down") {
		t.Fatalf("status output:\n%s", out.String())
	}
}
