package.path = "lib/?.lua;spec/?.lua;" .. package.path

local histogram = require "resty.retry_budget.histogram"
local budget = require "resty.retry_budget.budget"
local fake = require "fake_dict"

local function lcg(seed)
    local s = seed
    return function()
        s = (1103515245 * s + 12345) % 2147483648
        return s / 2147483648
    end
end

describe("histogram", function()
    before_each(function()
        histogram._reset()
        budget._reset()
    end)

    local function env_at(now)
        local box = { now = now or 0 }
        local clock = function()
            return box.now
        end
        box.clock = clock
        box.dict = fake.new(clock)
        return box
    end

    local function estimator(name, box, opts)
        opts = opts or {}
        opts.dict = "retry_budget"
        opts.dict_obj = box.dict
        opts.clock = box.clock
        if opts.min_samples == nil then
            opts.min_samples = 1
        end
        return histogram.new(name, opts)
    end

    it("maps bucket edges", function()
        assert.equal(1, histogram.bucket_of(0.2))
        assert.equal(1, histogram.bucket_of(1))
        assert.equal(2, histogram.bucket_of(histogram.RATIO))
        assert.equal(1, histogram.bucket_of(histogram.RATIO - 1e-6))
        assert.equal(127, histogram.bucket_of(60000))
        assert.equal(128, histogram.bucket_of(70000))
        assert.equal(128, histogram.BUCKETS)
    end)

    it("reports sub-millisecond samples inside the 1 ms bucket", function()
        local box = env_at(1)
        local est = estimator("subms", box)
        assert.is_true(est:record(0.2))
        local p, err = est:percentile(50)
        assert.is_nil(err)
        assert.is_true(p >= 1)
        assert.is_true(p < histogram.RATIO)
    end)

    it("reuses key strings until the slot changes and shares one registry entry", function()
        local box = env_at(1.2)
        local a = estimator("lat", box)
        local b = estimator("lat", box)
        assert.is_true(rawequal(a._st, b._st))
        assert.is_true(a:record(8))
        local key = a._st.keys[histogram.bucket_of(8)]
        assert.is_true(b:record(9))
        assert.is_true(rawequal(key, a._st.keys[histogram.bucket_of(8)]))
        local p = b:percentile(50)
        assert.is_true(p > 1)
        box.now = 11.2
        assert.is_true(a:record(8))
        assert.is_false(rawequal(key, a._st.keys[histogram.bucket_of(8)]))
    end)

    it("refuses a percentile until min_samples and rejects a bad p", function()
        local box = env_at(2)
        local est = estimator("lat", box, { min_samples = 100 })
        for _ = 1, 10 do
            est:record(15)
        end
        local p, err = est:percentile(95)
        assert.is_nil(p)
        assert.equal("not enough samples", err)
        p, err = est:percentile(0)
        assert.is_nil(p)
        assert.is_truthy(err:find("out of range"))
    end)

    it("forgets samples that slide out of the window", function()
        local box = env_at(0.5)
        local est = estimator("lat", box, {
            window_sec = 60,
            slot_sec = 10,
            min_samples = 1,
        })
        assert.is_true(est:record(1000))
        local p = est:percentile(50)
        assert.is_true(p > 500 and p < 2000)
        box.now = 70.5
        p = est:percentile(50)
        assert.is_nil(p)
    end)

    it("tracks an exponential within 5 percent", function()
        local box = env_at(3)
        local est = estimator("exp", box, { min_samples = 1 })
        local rng = lcg(99)
        local mean = 40
        local n = 20000
        for _ = 1, n do
            local u = rng()
            if u < 1e-12 then
                u = 1e-12
            end
            est:record(-math.log(u) * mean)
        end
        local function exact(p)
            return -math.log(1 - p) * mean
        end
        local p50 = est:percentile(50)
        local p95 = est:percentile(95)
        local p99 = est:percentile(99)
        assert.near(exact(0.50), p50, exact(0.50) * 0.05)
        assert.near(exact(0.95), p95, exact(0.95) * 0.05)
        assert.near(exact(0.99), p99, exact(0.99) * 0.05)
    end)

    it("tracks a lognormal within 5 percent", function()
        local box = env_at(4)
        local est = estimator("ln", box, { min_samples = 1 })
        local rng = lcg(7)
        local mu, sigma = 3, 0.5
        local n = 20000
        for _ = 1, n do
            local u1 = rng()
            local u2 = rng()
            if u1 < 1e-12 then
                u1 = 1e-12
            end
            local z = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
            est:record(math.exp(mu + sigma * z))
        end
        local function exact(p)
            -- rational approximation of inverse-normal, good to ~1e-4
            local function inv_norm(pp)
                local a = { 2.515517, 0.802853, 0.010328 }
                local b = { 1.432788, 0.189269, 0.001308 }
                local t = math.sqrt(-2 * math.log(pp < 0.5 and pp or (1 - pp)))
                local num = a[1] + a[2] * t + a[3] * t * t
                local den = 1 + b[1] * t + b[2] * t * t + b[3] * t * t * t
                local z = t - num / den
                if pp < 0.5 then
                    z = -z
                end
                return z
            end
            return math.exp(mu + sigma * inv_norm(p))
        end
        assert.near(exact(0.50), est:percentile(50), exact(0.50) * 0.05)
        assert.near(exact(0.95), est:percentile(95), exact(0.95) * 0.05)
        assert.near(exact(0.99), est:percentile(99), exact(0.99) * 0.05)
    end)
end)
