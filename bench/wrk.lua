-- Status mix seen by the client. Amplification is counted inside nginx.
-- done() runs outside the thread state, so counts are stored per thread.
local threads = {}

function setup(thread)
    threads[#threads + 1] = thread
end

function init()
    ok_n = 0
    non2xx = 0
end

function response(status)
    if status >= 200 and status < 300 then
        ok_n = ok_n + 1
    else
        non2xx = non2xx + 1
    end
end

function done(summary)
    local good, bad = 0, 0
    for i = 1, #threads do
        good = good + (threads[i]:get("ok_n") or 0)
        bad = bad + (threads[i]:get("non2xx") or 0)
    end
    io.write(string.format("wrk_requests %d\n", summary.requests))
    io.write(string.format("wrk_duration_us %d\n", summary.duration))
    io.write(string.format("wrk_ok %d\n", good))
    io.write(string.format("wrk_non2xx %d\n", bad))
end
