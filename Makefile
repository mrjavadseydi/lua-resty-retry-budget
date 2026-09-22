LUA_PATH := lib/?.lua;spec/?.lua;;
SRCS := lib/resty/retry_budget.lua $(wildcard lib/resty/retry_budget/*.lua)
LUAJIT ?= luajit

.PHONY: test lint gset prove

test:
	LUA_PATH="$(LUA_PATH)" busted spec

prove:
	bash t/docker-prove.sh

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
