package contentlibrary

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type fakeRunner struct {
	mu       sync.Mutex
	calls    []string
	response map[string][]fakeResp
}

type fakeResp struct {
	out []byte
	err error
}

func (f *fakeRunner) Run(_ context.Context, args ...string) ([]byte, error) {
	cmd := strings.Join(args, " ")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, cmd)
	seq, ok := f.response[cmd]
	if !ok || len(seq) == 0 {
		return nil, fmt.Errorf("unexpected call: %s", cmd)
	}
	resp := seq[0]
	f.response[cmd] = seq[1:]
	return resp.out, resp.err
}

func (f *fakeRunner) countCalls(command string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == command {
			n++
		}
	}
	return n
}

func TestItemPath_Trims(t *testing.T) {
	got := ItemPath("  lib-id  ", "  item ")
	if got != "lib-id/item" {
		t.Fatalf("unexpected path: %q", got)
	}
}

func TestParseLibraryID_Object(t *testing.T) {
	id, err := parseLibraryID([]byte(`{"Library":{"ID":"abc-123","Name":"x"}}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "abc-123" {
		t.Fatalf("id = %q", id)
	}
}

func TestParseLibraryID_Array(t *testing.T) {
	id, err := parseLibraryID([]byte(`[{"id":"lib-id"}]`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "lib-id" {
		t.Fatalf("id = %q", id)
	}
}

func TestParseLibraryID_Ambiguous(t *testing.T) {
	_, err := parseLibraryID([]byte(`[{"id":"a"},{"id":"b"}]`))
	if err == nil || !strings.Contains(err.Error(), "matches 2 items") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestParseLibraryID_NotFound(t *testing.T) {
	_, err := parseLibraryID([]byte(`null`))
	if err == nil || !strings.Contains(err.Error(), "matches 0 items") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestParseInfoPresence(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "null", raw: "null", want: false},
		{name: "empty array", raw: "[]", want: false},
		{name: "empty object", raw: "{}", want: false},
		{name: "object", raw: `{"name":"x"}`, want: true},
		{name: "array", raw: `[{"name":"x"}]`, want: true},
		{name: "invalid", raw: "{invalid", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseInfoPresence([]byte(tc.raw)); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestResolveLibraryID_Empty(t *testing.T) {
	c := NewClient(&fakeRunner{})
	_, err := c.ResolveLibraryID(context.Background(), " ")
	if err == nil || !strings.Contains(err.Error(), "library name is required") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestResolveLibraryID_RunError(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json lib": {{err: errors.New("boom")}},
	}}
	c := NewClient(r)
	_, err := c.ResolveLibraryID(context.Background(), "lib")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestEnsureLibrary_Existing(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json my-lib": {{out: []byte(`{"Library":{"ID":"lib-1"}}`)}},
	}}
	c := NewClient(r)
	ref, err := c.EnsureLibrary(context.Background(), "my-lib")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ref.ID != "lib-1" || ref.Target != "lib-1" || ref.Name != "my-lib" {
		t.Fatalf("unexpected ref: %#v", ref)
	}
}

func TestEnsureLibrary_CreateWhenMissing(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json my-lib": {
			{out: []byte(`null`)},
			{out: []byte(`{"Library":{"ID":"lib-1"}}`)},
		},
		"library.create my-lib": {{}},
	}}
	c := NewClient(r)
	ref, err := c.EnsureLibrary(context.Background(), "my-lib")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ref.ID != "lib-1" || ref.Target != "lib-1" {
		t.Fatalf("unexpected ref: %#v", ref)
	}
}

func TestEnsureLibrary_CreateError(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json my-lib": {{out: []byte(`[]`)}},
		"library.create my-lib":     {{err: errors.New("no rights")}},
	}}
	c := NewClient(r)
	_, err := c.EnsureLibrary(context.Background(), "my-lib")
	if err == nil || !strings.Contains(err.Error(), "create library") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestEnsureLibrary_ResolveAfterCreateError(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json my-lib": {{out: []byte(`[]`)}, {err: errors.New("still missing")}},
		"library.create my-lib":     {{}},
	}}
	c := NewClient(r)
	_, err := c.EnsureLibrary(context.Background(), "my-lib")
	if err == nil || !strings.Contains(err.Error(), "after create") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestEnsureLibrary_NonNoMatchError(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json my-lib": {{err: errors.New("network failed")}},
	}}
	c := NewClient(r)
	_, err := c.EnsureLibrary(context.Background(), "my-lib")
	if err == nil || !strings.Contains(err.Error(), "resolve library") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestItemExists_Error(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json lib/item": {{err: errors.New("govc failed")}},
	}}
	c := NewClient(r)
	_, err := c.ItemExists(context.Background(), "lib", "item")
	if err == nil || !strings.Contains(err.Error(), "govc failed") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestEnsureItemFromURL_SkipsWhenExists(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json lib/item": {{out: []byte(`{"name":"item"}`)}},
	}}
	c := NewClient(r)
	if err := c.EnsureItemFromURL(context.Background(), "lib", "item", "https://example.invalid/item.ova"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := r.countCalls("library.import -pull -n item lib https://example.invalid/item.ova"); got != 0 {
		t.Fatalf("expected no import, got %d", got)
	}
}

func TestEnsureItemFromURL_ItemExistsCheckError(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json lib/item": {{err: errors.New("check failed")}},
	}}
	c := NewClient(r)
	err := c.EnsureItemFromURL(context.Background(), "lib", "item", "https://example.invalid/item.ova")
	if err == nil || !strings.Contains(err.Error(), "item existence check failed") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestEnsureItemFromURL_ImportsWhenMissing(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json lib/item":                                  {{out: []byte(`null`)}},
		"library.import -pull -n item lib https://example.invalid/ova": {{}},
	}}
	c := NewClient(r)
	if err := c.EnsureItemFromURL(context.Background(), "lib", "item", "https://example.invalid/ova"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureItemFromURL_ConcurrentSingleImport(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.info -json lib/item": {
			{out: []byte(`null`)},
			{out: []byte(`{"name":"item"}`)},
		},
		"library.import -pull -n item lib https://example.invalid/ova": {{}},
	}}
	c := NewClient(r)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- c.EnsureItemFromURL(context.Background(), "lib", "item", "https://example.invalid/ova")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	}
	if got := r.countCalls("library.import -pull -n item lib https://example.invalid/ova"); got != 1 {
		t.Fatalf("expected 1 import, got %d", got)
	}
}

func TestImportItemFromURL_PullSuccess(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.import -pull -n item lib https://example.invalid/ova": {{}},
	}}
	c := NewClient(r)
	if err := c.ImportItemFromURL(context.Background(), "lib", "item", "https://example.invalid/ova"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := r.countCalls("library.rm lib/item"); got != 0 {
		t.Fatalf("expected no remove, got %d", got)
	}
}

func TestImportItemFromURL_Fallback(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.import -pull -n item lib https://example.invalid/ova": {{err: errors.New("pull failed")}},
		"library.rm lib/item": {{}},
		"library.import -n item lib https://example.invalid/ova": {{}},
	}}
	c := NewClient(r)
	if err := c.ImportItemFromURL(context.Background(), "lib", "item", "https://example.invalid/ova"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestImportItemFromURL_FallbackAlreadyExists(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.import -pull -n item lib https://example.invalid/ova": {{err: errors.New("pull failed")}},
		"library.rm lib/item": {{}},
		"library.import -n item lib https://example.invalid/ova": {
			{err: errors.New("duplicate_item_name_unsupported_in_library")},
		},
	}}
	c := NewClient(r)
	if err := c.ImportItemFromURL(context.Background(), "lib", "item", "https://example.invalid/ova"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestImportItemFromURL_FallbackError(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.import -pull -n item lib https://example.invalid/ova": {{err: errors.New("pull failed")}},
		"library.rm lib/item": {{}},
		"library.import -n item lib https://example.invalid/ova": {{err: errors.New("still failed")}},
	}}
	c := NewClient(r)
	err := c.ImportItemFromURL(context.Background(), "lib", "item", "https://example.invalid/ova")
	if err == nil || !strings.Contains(err.Error(), "library.import failed") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestDeployItem_Validation(t *testing.T) {
	c := NewClient(&fakeRunner{})
	cases := []struct {
		name string
		opt  DeployOptions
		err  string
	}{
		{name: "missing dc", opt: DeployOptions{}, err: "datacenter is required"},
		{name: "missing ds", opt: DeployOptions{Datacenter: "dc"}, err: "datastore is required"},
		{name: "missing item", opt: DeployOptions{Datacenter: "dc", Datastore: "ds"}, err: "item path is required"},
		{name: "missing vm", opt: DeployOptions{Datacenter: "dc", Datastore: "ds", ItemPath: "lib/item"}, err: "vm name is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.DeployItem(context.Background(), tc.opt)
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("unexpected err: %v", err)
			}
		})
	}
}

func TestDeployItem_MinimalArgs(t *testing.T) {
	r := &fakeRunner{response: map[string][]fakeResp{
		"library.deploy -dc dc -ds ds lib/item vm01": {{}},
	}}
	c := NewClient(r)
	err := c.DeployItem(context.Background(), DeployOptions{
		Datacenter: "dc",
		Datastore:  "ds",
		ItemPath:   "lib/item",
		VMName:     "vm01",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestDeployItem_FullArgsAndRunnerError(t *testing.T) {
	cmd := "library.deploy -dc dc -ds ds -options /tmp/spec.json -folder /vm/folder -pool /rp pool/lib/item vm01"
	r := &fakeRunner{response: map[string][]fakeResp{
		cmd: {{err: errors.New("bad request")}},
	}}
	c := NewClient(r)
	err := c.DeployItem(context.Background(), DeployOptions{
		Datacenter:   "dc",
		Datastore:    "ds",
		OptionsPath:  "/tmp/spec.json",
		Folder:       "/vm/folder",
		ResourcePool: "/rp",
		ItemPath:     "pool/lib/item",
		VMName:       "vm01",
	})
	if err == nil || !strings.Contains(err.Error(), "library.deploy failed") {
		t.Fatalf("unexpected err: %v", err)
	}
}
