LUA_PATH := lib/?.lua;spec/?.lua;;
SRCS := lib/resty/retry_budget.lua $(wildcard lib/resty/retry_budget/*.lua)
LUAJIT ?= luajit

.PHONY: test lint gset prove t bench

test:
	LUA_PATH="$(LUA_PATH)" busted spec

prove:
	bash t/docker-prove.sh

# Local Test::Nginx. One shell: exit skips the rest of the recipe.
t:
	@set -e; \
	if ! command -v openresty >/dev/null 2>&1 || ! command -v prove >/dev/null 2>&1; then \
		echo "skip: openresty or prove is not on PATH. Integration tests run with: make prove"; \
		exit 0; \
	fi; \
	if ! perl -MTest::Nginx::Socket::Lua -e '1' >/dev/null 2>&1; then \
		echo "skip: Test::Nginx::Socket::Lua is not installed. Integration tests run with: make prove"; \
		exit 0; \
	fi; \
	PATH="$$(dirname $$(command -v openresty)):$$PATH" RETRY_BUDGET_LIB="$(CURDIR)/lib/?.lua" prove -r t

bench:
	bash bench/run.sh

lint: gset
	luacheck lib spec

gset:
	@fail=0; \
	for f in $(SRCS); do \
	  out=`$(LUAJIT) -bl $$f | grep GSET || true`; \
	  if [ -n "$$out" ]; then \
	    echo "global write in $$f"; \
	    echo "$$out"; \
	    fail=1; \
	  fi; \
	done; \
	exit $$fail
