local _M = { _VERSION = "0.1" }

local tableutil = require( "acid.tableutil" )
local strutil = require("acid.strutil")

function _M.get_info( offset )
    offset = offset or 0
    local thisfile = debug.getinfo(1).short_src
    local info
    for i = 2, 10 do
        info = debug.getinfo(i)

        if info.short_src ~= thisfile then
            if offset == 0 then
                break
            end
            offset = offset - 1
        end
    end
    return info
end

function _M.get_pos( offset )
    local info = _M.get_info( offset )
    local src = info.short_src
    src = strutil.split(src, "/")
    src = src[#src]

    local pos = ''
    pos = src .. ':' .. (info.name or '-') .. '():' .. info.currentline
    return pos
end

function _M.tostr(...)
    local args = {...}
    local n = select( "#", ... )
    local t = {}
    for i = 1, n do
        table.insert( t, tableutil.str(args[i]) )
    end
    return table.concat( t, " " )
end

function _M.output(s)
    print(s)
end

function _M.dd(...)
    _M.output( _M.make_logline(0, ...) )
end

function _M.make_logline(offset, ...)
    return _M.get_pos(offset) .. " " .. _M.tostr( ... )
end

return _M
