package contentlibrary

import (
	"context"
	"strings"
	"testing"
)

func TestGovcRunner_RunSuccess(t *testing.T) {
	r := GovcRunner{Command: "bash"}
	out, err := r.Run(context.Background(), "-lc", "printf 'ok'")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if string(out) != "ok" {
		t.Fatalf("unexpected output: %q", string(out))
	}
}

func TestGovcRunner_RunIncludesEnv(t *testing.T) {
	r := GovcRunner{
		Command: "bash",
		Env:     []string{"TEST_MYVAR=hello"},
	}
	out, err := r.Run(context.Background(), "-lc", "printf '%s' \"$TEST_MYVAR\"")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if string(out) != "hello" {
		t.Fatalf("unexpected output: %q", string(out))
	}
}

func TestGovcRunner_RunErrorUsesStderr(t *testing.T) {
	r := GovcRunner{Command: "bash"}
	_, err := r.Run(context.Background(), "-lc", "echo err-msg 1>&2; exit 2")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "err-msg") {
		t.Fatalf("expected stderr in error, got: %v", err)
	}
}

func TestGovcRunner_RunErrorFallsBackToStdout(t *testing.T) {
	r := GovcRunner{Command: "bash"}
	_, err := r.Run(context.Background(), "-lc", "echo out-msg; exit 3")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "out-msg") {
		t.Fatalf("expected stdout fallback in error, got: %v", err)
	}
}
