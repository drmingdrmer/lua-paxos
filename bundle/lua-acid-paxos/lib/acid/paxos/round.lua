local _M = { _VERSION = require("acid.paxos._ver") }

local tableutil = require( "acid.tableutil" )

function _M.new( elts )
    assert( elts[ 2 ] ~= nil and elts[ 3 ] == nil,
            "invalid nr of elts while creating new round" )
    local rnd = tableutil.duplist( elts )
    return rnd
end

function _M.zero()
    return _M.new({ 0, '' })
end

function _M.max( rounds )
    local max = _M.zero()
    for _, r in ipairs(rounds) do
        if _M.cmp( max, r ) < 0 then
            max = r
        end
    end
    return max
end

function _M.incr( rnd )
    return _M.new({ rnd[1] + 1, rnd[2] })
end

function _M.cmp( a, b )
    a = a or {}
    b = b or {}
    for i = 1, 2 do
        local x = cmp( a[i], b[i] )
        if x ~= 0 then
            return x
        end
    end
    return 0
end

function cmp(a, b)

    if a == nil then
        if b == nil then
            return 0
        else
            return -1
        end
    else
        if b == nil then
            return 1
        end
    end

    -- none of a or b is nil

    if a>b then
        return 1
    elseif a<b then
        return -1
    else
        return 0
    end
end

return _M
