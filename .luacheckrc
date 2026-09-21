std = "ngx_lua"
codes = true
self = false
exclude_files = {
    "bench/**",
    "t/**",
}
files["spec"] = {
    std = "+busted",
}
