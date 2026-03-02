# vmware-content-library-core

[![CI](https://github.com/Bibi40k/vmware-content-library-core/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/Bibi40k/vmware-content-library-core/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Bibi40k/vmware-content-library-core)](https://github.com/Bibi40k/vmware-content-library-core/releases)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Bibi40k/vmware-content-library-core/master/docs/coverage/coverage.json)](https://github.com/Bibi40k/vmware-content-library-core/blob/master/docs/coverage/coverage.json)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Bibi40k/vmware-content-library-core)](https://github.com/Bibi40k/vmware-content-library-core/blob/master/go.mod)
[![License](https://img.shields.io/github/license/Bibi40k/vmware-content-library-core)](https://github.com/Bibi40k/vmware-content-library-core/blob/master/LICENSE)

Reusable Go primitives for VMware vSphere Content Library workflows via `govc`.

## What it provides

- Resolve/create content libraries (`ResolveLibraryID`, `EnsureLibrary`)
- Idempotent item management from remote artifacts (`EnsureItemFromURL`)
- Safe concurrent imports (per-item lock for `check + import`)
- OVF deployment wrapper (`DeployItem`)
- Pluggable runner (`Runner`) for tests and custom execution environments

The package focuses on reusable VMware content library operations for OVA/OVF workflows.

## Install

```bash
go get github.com/Bibi40k/vmware-content-library-core
```

## Minimal usage

```go
runner := contentlibrary.GovcRunner{Env: []string{
    "GOVC_URL=https://vcenter.example.local/sdk",
    "GOVC_USERNAME=svc@example.local",
    "GOVC_PASSWORD=***",
    "GOVC_DATACENTER=Datacenter",
    "GOVC_INSECURE=true",
}}

client := contentlibrary.NewClient(runner)
ctx := context.Background()

lib, _ := client.EnsureLibrary(ctx, "images")
_ = client.EnsureItemFromURL(ctx, lib.Target, "ubuntu-24.04", "https://example.invalid/ubuntu.ova")
_ = client.DeployItem(ctx, contentlibrary.DeployOptions{
    Datacenter: "Datacenter",
    Datastore:  "Datastore01",
    ItemPath:   contentlibrary.ItemPath(lib.Target, "ubuntu-24.04"),
    VMName:     "vm-test-01",
})
```
