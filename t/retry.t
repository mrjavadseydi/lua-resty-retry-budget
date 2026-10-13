use Test::Nginx::Socket::Lua 'no_plan';
use Cwd qw(cwd);

repeat_each(1);
no_shuffle();
no_long_string();

my $lib = $ENV{RETRY_BUDGET_LIB} || (cwd() . "/lib/?.lua");

add_block_preprocessor(sub {
    my $block = shift;
    my $extra = $block->http_config;
    $extra = '' unless defined $extra;
    $block->set_value("http_config", <<"EOF");
lua_package_path "$lib;;";
lua_socket_log_errors off;
lua_shared_dict retry_budget 10m;
lua_shared_dict tiny 64k;
lua_shared_dict state 1m;

server {
    listen 127.0.0.1:1985;
    lua_check_client_abort on;

    location = /once {
        content_by_lua_block {
            local n = ngx.shared.state:incr("once", 1, 0)
            ngx.header["Connection"] = "close"
            if n == 1 then
                ngx.status = 503
                ngx.say("no")
                return
            end
            ngx.say("yes")
        }
    }

    location = /always {
        content_by_lua_block {
            ngx.shared.state:incr("always", 1, 0)
            ngx.status = 503
            ngx.header["Connection"] = "close"
            ngx.say("no")
        }
    }

    location = /post {
        content_by_lua_block {
            ngx.shared.state:incr("post", 1, 0)
            ngx.status = 503
            ngx.header["Connection"] = "close"
            ngx.say("no")
        }
    }

    location = /idem {
        content_by_lua_block {
            local n = ngx.shared.state:incr("idem", 1, 0)
            ngx.header["Connection"] = "close"
            if n == 1 then
                ngx.status = 503
                ngx.say("no")
                return
            end
            ngx.say("yes")
        }
    }

    location = /ra {
        content_by_lua_block {
            local n = ngx.shared.state:incr("ra", 1, 0)
            ngx.header["Connection"] = "close"
            if n == 1 then
                ngx.status = 503
                ngx.header["Retry-After"] = "1"
                ngx.say("wait")
                return
            end
            ngx.say("go")
        }
    }

    location = /ra-long {
        content_by_lua_block {
            local n = ngx.shared.state:incr("ralong", 1, 0)
            ngx.header["Connection"] = "close"
            if n == 1 then
                ngx.status = 503
                ngx.header["Retry-After"] = "30"
                ngx.say("wait")
                return
            end
            ngx.say("go")
        }
    }

    location = /slow {
        content_by_lua_block {
            ngx.sleep(1.5)
            ngx.say("late")
        }
    }

    location = /slow-headers {
        content_by_lua_block {
            local sock = assert(ngx.req.socket(true))
            sock:send("HTTP/1.1 200 OK\\r\\nConnection: close\\r\\nContent-Length: 0\\r\\n")
            ngx.sleep(0.35)
            sock:send("\\r")
            ngx.sleep(0.35)
            sock:send("\\n")
            return ngx.exit(444)
        }
    }

    location = /hedge-headers {
        content_by_lua_block {
            local n = ngx.shared.state:incr("hedge_headers", 1, 0)
            if n == 1 then
                ngx.sleep(0.1)
                ngx.status = 503
                ngx.header["Connection"] = "close"
                ngx.say("no")
                return
            end
            local sock = assert(ngx.req.socket(true))
            sock:send("HTTP/1.1 200 OK\\r\\nConnection: close\\r\\nContent-Length: 0\\r\\n")
            ngx.sleep(0.35)
            sock:send("\\r")
            ngx.sleep(0.35)
            sock:send("\\n")
            return ngx.exit(444)
        }
    }

    location = /continue {
        content_by_lua_block {
            ngx.req.read_body()
            ngx.header["Connection"] = "close"
            ngx.say(ngx.req.get_body_data() or "empty")
        }
    }

    location = /continue-reject {
        content_by_lua_block {
            ngx.req.clear_header("Expect")
            ngx.status = 417
            ngx.header["Connection"] = "close"
            ngx.say("rejected")
        }
    }

    location = /trickle {
        content_by_lua_block {
            ngx.status = 200
            ngx.header["Content-Length"] = "4"
            ngx.send_headers()
            for _ = 1, 4 do
                ngx.print("x")
                ngx.flush(true)
                ngx.sleep(0.45)
            end
        }
    }

    location = /hedge {
        content_by_lua_block {
            local n = ngx.shared.state:incr("hedge", 1, 0)
            if n == 1 then
                ngx.sleep(0.8)
                local ok = ngx.say("slow")
                if ok then
                    ok = ngx.flush(true)
                end
                if ok then
                    ngx.shared.state:incr("slow_done", 1, 0)
                end
                return
            end
            ngx.header["Connection"] = "close"
            ngx.say("fast")
        }
    }

    location = /ping {
        content_by_lua_block {
            ngx.header["Connection"] = "close"
            ngx.say("pong")
        }
    }

    location = /p95 {
        content_by_lua_block {
            ngx.shared.state:incr("p95", 1, 0)
            ngx.header["Connection"] = "close"
            ngx.say("one")
        }
    }

    location = /two-slow {
        content_by_lua_block {
            local n = ngx.shared.state:incr("twoslow", 1, 0)
            ngx.sleep(0.35)
            ngx.header["Connection"] = "close"
            if n == 1 then
                ngx.status = 503
                ngx.say("no")
                return
            end
            ngx.say("yes")
        }
    }

    location = /hedge-dead {
        content_by_lua_block {
            ngx.shared.state:incr("hedgedead", 1, 0)
            ngx.sleep(1)
            ngx.header["Connection"] = "close"
            ngx.say("slow")
        }
    }
}
EOF
    $block->set_value("http_config", $block->http_config . $extra);
});

run_tests();

__DATA__

=== TEST 1: retry a 503 then take the success
--- config
    location = /t {
        content_by_lua_block {
            local c = require("resty.retry_budget").new_client("once", {
                dict = "retry_budget",
                max_attempts = 3,
                min_retries_per_sec = 100,
                backoff = { base_ms = 1, max_ms = 5 },
            })
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/once")
            if not res then
                ngx.say("ERR ", err)
                return
            end
            ngx.say(res.status)
            local body = res.body or ""
            if body:byte(-1) == 10 then body = body:sub(1, -2) end
            if body:byte(-1) == 13 then body = body:sub(1, -2) end
            ngx.say(body)
            ngx.say(meta.attempts)
            ngx.say(meta.winner)
            ngx.say(meta.budget_exhausted and "true" or "false")
        }
    }
--- request
GET /t
--- response_body
200
yes
2
2
false



=== TEST 2: POST without an idempotency key is not retried
--- config
    location = /t {
        content_by_lua_block {
            local c = require("resty.retry_budget").new_client("post", {
                dict = "retry_budget",
                max_attempts = 3,
                backoff = { base_ms = 1, max_ms = 5 },
            })
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/post", {
                method = "POST",
                body = "abc",
            })
            if not res then
                ngx.say("ERR ", err)
                return
            end
            ngx.say(res.status)
            ngx.say(meta.attempts)
            ngx.say(meta.retry_skipped)
            ngx.say(ngx.shared.state:get("post"))
        }
    }
--- request
GET /t
--- response_body
503
1
non_idempotent
1



=== TEST 3: POST with an idempotency key can be retried
--- config
    location = /t {
        content_by_lua_block {
            local c = require("resty.retry_budget").new_client("idem", {
                dict = "retry_budget",
                max_attempts = 3,
                min_retries_per_sec = 100,
                backoff = { base_ms = 1, max_ms = 5 },
            })
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/idem", {
                method = "POST",
                body = "abc",
                headers = { ["Idempotency-Key"] = "k1" },
            })
            if not res then
                ngx.say("ERR ", err)
                return
            end
            ngx.say(res.status)
            local body = res.body or ""
            if body:byte(-1) == 10 then body = body:sub(1, -2) end
            if body:byte(-1) == 13 then body = body:sub(1, -2) end
            ngx.say(body)
            ngx.say(meta.attempts)
            ngx.say(meta.retry_skipped or "nil")
        }
    }
--- request
GET /t
--- response_body
200
yes
2
nil



=== TEST 4: sustained failure stays inside the ratio
--- config
    location = /t {
        content_by_lua_block {
            local c = require("resty.retry_budget").new_client("storm", {
                dict = "retry_budget",
                max_attempts = 5,
                retry_ratio = 0.2,
                min_retries_per_sec = 0,
                window_sec = 10,
                backoff = { base_ms = 0, max_ms = 0 },
            })
            local res, err, meta
            for _ = 1, 40 do
                res, err, meta = c:request_uri("http://127.0.0.1:1985/always")
            end
            local hits = ngx.shared.state:get("always") or 0
            ngx.say(res and res.status or err)
            ngx.say(meta.budget_exhausted and "true" or "false")
            ngx.say(hits > 40 and hits <= 52 and "bounded" or ("bad " .. hits))
        }
    }
--- request
GET /t
--- response_body
503
true
bounded



=== TEST 5: Retry-After is honored and capped by max_ms
--- config
    location = /t {
        content_by_lua_block {
            local rb = require "resty.retry_budget"
            local c = rb.new_client("ra", {
                dict = "retry_budget",
                max_attempts = 2,
                min_retries_per_sec = 100,
                backoff = { base_ms = 1, max_ms = 5000 },
            })
            local t0 = ngx.now()
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/ra")
            local dt = ngx.now() - t0
            if not res then
                ngx.say("ERR ", err)
                return
            end
            ngx.say(res.status)
            ngx.say(meta.attempts)
            ngx.say(dt >= 0.8 and "delayed" or "fast")

            local c2 = rb.new_client("ralong", {
                dict = "retry_budget",
                max_attempts = 2,
                min_retries_per_sec = 100,
                backoff = { base_ms = 1, max_ms = 80 },
            })
            t0 = ngx.now()
            res, err, meta = c2:request_uri("http://127.0.0.1:1985/ra-long")
            dt = ngx.now() - t0
            ngx.say(res and res.status or err)
            ngx.say(meta.attempts)
            ngx.say(dt < 0.4 and "capped" or "slow")
        }
    }
--- request
GET /t
--- response_body
200
2
delayed
200
2
capped
--- timeout: 8



=== TEST 6: deadline skips the next retry and aborts a slow body
--- config
    location = /t {
        content_by_lua_block {
            local rb = require "resty.retry_budget"
            local c = rb.new_client("dead", {
                dict = "retry_budget",
                max_attempts = 3,
                min_retries_per_sec = 100,
                backoff = { base_ms = 2000, max_ms = 2000 },
                deadline_ms = 200,
            })
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/always")
            ngx.say(res and res.status or err)
            ngx.say(meta.attempts)
            ngx.say(meta.deadline_exceeded and "yes" or "no")

            local c2 = rb.new_client("slow", {
                dict = "retry_budget",
                max_attempts = 1,
                connect_timeout = 1000,
                send_timeout = 1000,
                read_timeout = 5000,
            })
            local t0 = ngx.now()
            res, err, meta = c2:request_uri("http://127.0.0.1:1985/slow", nil, {
                deadline_ms = 400,
            })
            local dt = ngx.now() - t0
            ngx.say(meta.deadline_exceeded and "yes" or "no")
            ngx.say(dt < 1 and "short" or "long")

            local c3 = rb.new_client("trickle", {
                dict = "retry_budget",
                max_attempts = 1,
                read_timeout = 5000,
            })
            t0 = ngx.now()
            res, err, meta = c3:request_uri("http://127.0.0.1:1985/trickle", nil, {
                deadline_ms = 500,
            })
            dt = ngx.now() - t0
            local body = res and (res.body or "") or ""
            ngx.say(body == "xxxx" and "full" or "cut")
            ngx.say(dt < 1.3 and "short" or "long")
            ngx.say(meta.deadline_exceeded and "yes" or "no")
        }
    }
--- request
GET /t
--- response_body
503
1
yes
yes
short
cut
short
yes
--- timeout: 8



=== TEST 7: hedge uses the fast attempt and does not finish the slow one
--- config
    location = /t {
        content_by_lua_block {
            local c = require("resty.retry_budget").new_client("hedge", {
                dict = "retry_budget",
                max_attempts = 2,
                min_retries_per_sec = 100,
                connect_timeout = 1000,
                send_timeout = 1000,
                read_timeout = 2000,
            })
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/hedge", nil, {
                hedge = {
                    enabled = true,
                    delay = 40,
                    max_hedges = 1,
                    min_delay_ms = 5,
                },
            })
            if not res then
                ngx.say("ERR ", err)
                return
            end
            local body = res.body or ""
            if body:byte(-1) == 10 then body = body:sub(1, -2) end
            if body:byte(-1) == 13 then body = body:sub(1, -2) end
            ngx.say(body)
            ngx.say(meta.hedged and "true" or "false")
            ngx.say(meta.winner)
            ngx.say(meta.attempts)
            ngx.sleep(1)
            ngx.say(ngx.shared.state:get("slow_done") or 0)
            local ok_n = 0
            for _ = 1, 3 do
                local ping = c:request_uri("http://127.0.0.1:1985/ping")
                if ping and ping.status == 200 then
                    ok_n = ok_n + 1
                end
            end
            ngx.say(ok_n)
            ngx.say(c.budget:stats().hedges_won)
        }
    }
--- request
GET /t
--- response_body
fast
true
2
2
0
3
1
--- timeout: 8



=== TEST 8: p95 hedge waits for samples
--- config
    location = /t {
        content_by_lua_block {
            local c = require("resty.retry_budget").new_client("p95", {
                dict = "retry_budget",
                max_attempts = 2,
                min_retries_per_sec = 100,
            })
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/p95", nil, {
                hedge = { enabled = true, delay = "p95" },
            })
            if not res then
                ngx.say("ERR ", err)
                return
            end
            ngx.say(meta.hedged and "true" or "false")
            ngx.say(meta.hedge_skipped or "nil")
            ngx.say(meta.attempts)
            ngx.say(ngx.shared.state:get("p95"))
        }
    }
--- request
GET /t
--- response_body
false
not_enough_samples
1
1



=== TEST 9: the client refuses an unsupported phase
--- config
    set_by_lua_block $phase_err {
        local c, nerr = require("resty.retry_budget").new_client("phaseset", {
            dict = "retry_budget",
        })
        if not c then
            return "new " .. nerr
        end
        local _, err, meta = c:request_uri("http://127.0.0.1:1985/ping")
        return (err or "nil") .. " " .. (meta.retry_skipped or "nil")
    }
    location = /t {
        content_by_lua_block {
            ngx.say(ngx.var.phase_err)
        }
    }
--- request
GET /t
--- response_body
retry-budget: client API is not available in phase "set" phase



=== TEST 10: a full dict fails open
--- config
    location = /t {
        content_by_lua_block {
            local d = ngx.shared.tiny
            local n = 0
            local streak = 0
            while n < 100000 and streak < 20 do
                n = n + 1
                local ok, _, forcible = d:set("k" .. n, "xxxxxxxx")
                if not ok then
                    break
                end
                if forcible then
                    streak = streak + 1
                else
                    streak = 0
                end
            end
            local c, err = require("resty.retry_budget").new_client("full", {
                dict = "tiny",
                max_attempts = 2,
                min_retries_per_sec = 100,
                retry_ratio = 0.2,
                fail_mode = "open",
                backoff = { base_ms = 1, max_ms = 5 },
            })
            if not c then
                ngx.say("ERR ", err)
                return
            end
            local res, rerr, meta = c:request_uri("http://127.0.0.1:1985/once")
            ngx.say(res and res.status or rerr)
            ngx.say(meta.attempts)
            ngx.say(meta.budget_exhausted and "true" or "false")
        }
    }
--- request
GET /t
--- response_body
200
2
false
--- error_log
key=full shared dict evicted
--- timeout: 10



=== TEST 11: the deadline covers the retry, not only the first attempt
--- config
    location = /t {
        content_by_lua_block {
            local c = require("resty.retry_budget").new_client("twoslow", {
                dict = "retry_budget",
                max_attempts = 2,
                min_retries_per_sec = 100,
                deadline_ms = 500,
                connect_timeout = 2000,
                read_timeout = 5000,
                backoff = { base_ms = 1, max_ms = 1 },
            })
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/two-slow")
            ngx.say(meta.deadline_exceeded and "yes" or "no")
            local status = res and res.status or 0
            ngx.say(status == 200 and "full" or "cut")
            ngx.say(meta.attempts)
        }
    }
--- request
GET /t
--- response_body
yes
cut
2
--- timeout: 8



=== TEST 12: a hedge timer that hits the deadline does not send
--- config
    location = /t {
        content_by_lua_block {
            local c = require("resty.retry_budget").new_client("hedgedead", {
                dict = "retry_budget",
                max_attempts = 2,
                min_retries_per_sec = 100,
                deadline_ms = 200,
                connect_timeout = 1000,
                read_timeout = 5000,
            })
            local res, _, meta = c:request_uri("http://127.0.0.1:1985/hedge-dead", nil, {
                hedge = { enabled = true, delay = 500, max_hedges = 1 },
            })
            ngx.say(meta.deadline_exceeded and "yes" or "no")
            ngx.say(meta.hedged and "yes" or "no")
            ngx.say(ngx.shared.state:get("hedgedead") or 0)
            ngx.say(res and res.status or "none")
        }
    }
--- request
GET /t
--- response_body
yes
no
1
none
--- timeout: 8


=== TEST 13: the deadline cancels a trickled header with and without hedging
--- config
    location = /t {
        content_by_lua_block {
            local rb = require "resty.retry_budget"
            for i = 1, 2 do
                local c = assert(rb.new_client("header" .. i, {
                    dict = "retry_budget", max_attempts = 2,
                    deadline_ms = 500, read_timeout = 2000,
                }))
                local call_opts = i == 2 and {
                    hedge = { enabled = true, delay = 1000 },
                } or nil
                local started = ngx.now()
                local res, _, meta = c:request_uri("http://127.0.0.1:1985/slow-headers",
                                                  nil, call_opts)
                ngx.say(res == nil and "cancelled" or "late success")
                ngx.say(meta.deadline_exceeded and "yes" or "no")
                ngx.say(ngx.now() - started < 0.65 and "short" or "long")
                ngx.say(meta.attempts)
                ngx.say(meta.hedged and "yes" or "no")
            end
        }
    }
--- request
GET /t
--- response_body
cancelled
yes
short
1
no
cancelled
yes
short
1
no
--- timeout: 8


=== TEST 14: the deadline stops a yielding upload producer
--- config
    location = /t {
        content_by_lua_block {
            local c = assert(require("resty.retry_budget").new_client("upload", {
                dict = "retry_budget", deadline_ms = 500,
            }))
            local produced = 0
            local started = ngx.now()
            local res, _, meta = c:request_uri("http://127.0.0.1:1985/continue", {
                method = "POST", headers = { ["Content-Length"] = "3" },
                body = function()
                    if produced == 3 then return nil end
                    ngx.sleep(0.35)
                    produced = produced + 1
                    return "x"
                end,
            })
            ngx.say(res == nil and "cancelled" or "late success")
            ngx.say(meta.deadline_exceeded and "yes" or "no")
            ngx.say(ngx.now() - started < 0.65 and "short" or "long")
            ngx.sleep(0.7)
            ngx.say(produced)
            ngx.say(meta.attempts)
            ngx.say(meta.retry_skipped)
        }
    }
--- request
GET /t
--- response_body
cancelled
yes
short
1
1
streaming_body
--- timeout: 8


=== TEST 15: 100 Continue sends string, table, and stream bodies
--- config
    location = /t {
        content_by_lua_block {
            local c = assert(require("resty.retry_budget").new_client("continue", {
                dict = "retry_budget", deadline_ms = 1000,
            }))
            local sent = false
            local bodies = { "hello", { "he", "llo" }, function()
                if sent then return nil end
                sent = true
                return "hello"
            end }
            for i = 1, #bodies do
                local res, err, meta = c:request_uri("http://127.0.0.1:1985/continue", {
                    method = "POST", body = bodies[i],
                    headers = { ["expect"] = "100-continue", ["Content-Length"] = "5" },
                })
                ngx.say(res and res.status or err)
                ngx.say(res and res.body:gsub("%s+$", "") or "empty")
                ngx.say(meta.attempts)
            end
        }
    }
--- request
GET /t
--- response_body
200
hello
1
200
hello
1
200
hello
1
--- timeout: 8


=== TEST 16: an early final response does not consume the deferred body
--- config
    location = /t {
        content_by_lua_block {
            local c = assert(require("resty.retry_budget").new_client("reject", {
                dict = "retry_budget", deadline_ms = 1000,
            }))
            local consumed = false
            local res, err, meta = c:request_uri("http://127.0.0.1:1985/continue-reject", {
                method = "POST", headers = { Expect = "100-continue", ["Content-Length"] = "5" },
                body = function()
                    if consumed then return nil end
                    consumed = true
                    return "hello"
                end,
            })
            ngx.say(res and res.status or err)
            ngx.say(consumed and "sent" or "deferred")
            ngx.say(meta.attempts)
        }
    }
--- request
GET /t
--- response_body
417
deferred
1
--- timeout: 8


=== TEST 17: the deadline cancels sent hedges and preserves an earlier response
--- config
    location = /t {
        content_by_lua_block {
            local rb = require "resty.retry_budget"
            local paths = { "/slow-headers", "/hedge-headers" }
            for i = 1, #paths do
                local c = assert(rb.new_client("senthedge" .. i, {
                    dict = "retry_budget", max_attempts = 2,
                    deadline_ms = 500, read_timeout = 2000,
                }))
                local started = ngx.now()
                local res, _, meta = c:request_uri("http://127.0.0.1:1985" .. paths[i], nil, {
                    hedge = { enabled = true, delay = 40 },
                })
                ngx.say(res and res.status or "cancelled")
                ngx.say(meta.deadline_exceeded and "yes" or "no")
                ngx.say(ngx.now() - started < 0.65 and "short" or "long")
                ngx.say(meta.attempts)
                ngx.say(meta.hedged and "yes" or "no")
                ngx.say(c.budget:stats().hedges_won)
            end
        }
    }
--- request
GET /t
--- response_body
cancelled
yes
short
2
yes
0
503
yes
short
2
yes
0
--- timeout: 8
