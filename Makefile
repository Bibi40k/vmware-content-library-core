# Compatibility shim — all targets delegate to Taskfile.yml
# Migrate: use `task <command>` directly
# Install go-task: go install github.com/go-task/task/v3/cmd/task@latest

SHELL  := /bin/bash
_TASK  := $(or $(shell command -v task 2>/dev/null),$(shell go env GOPATH 2>/dev/null)/bin/task)

define _require_task
@test -x "$(_TASK)" || { printf "go-task not found.\nInstall: go install github.com/go-task/task/v3/cmd/task@latest\n"; exit 1; }
endef

.DEFAULT_GOAL := help
.PHONY: FORCE help install-task

help:
	@printf "\033[1;33mBootstrap:\033[0m make install-task\n"
	@printf "Use \033[1mtask\033[0m for available commands.\n"

install-task:
	@go install github.com/go-task/task/v3/cmd/task@latest
	@printf "Installed go-task to $$(go env GOPATH)/bin/task\n"
	@printf "Add to PATH: export PATH=\"\$$HOME/go/bin:\$$PATH\"\n"

# Prevent Make from trying to remake the Makefile itself via %: catch-all
Makefile GNUmakefile: ;

# FORCE as PHONY prerequisite ensures %: always runs (handles build/ dirs etc.)
FORCE:

%: FORCE
	$(_require_task)
	@$(_TASK) $@
