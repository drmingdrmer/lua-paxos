local w = require("worker_init")
local tableutil = require( "acid.tableutil" )

local _M = {}

function _M.get()
    local cc, ident = w.cc, w.ident

    -- strip /get/
    local fn = ngx.var.uri:sub(6)

    local mems = cc:members()
    ngx.log(ngx.ERR, tableutil.str(mems))
    local hsh = ngx.crc32_long(fn)

    local dst = mems[ (hsh % #mems) + 1 ]

    if dst == ident then
        ngx.req.set_uri("/www/" .. fn, true )
    else
        dst = dst:gsub("990", "980")
        ngx.req.set_uri("/proxy/" .. dst .. "/get/" .. fn, true)
    end
end

return _M
