local _M = {}
local _meta = { __index=_M }

local json = require( "cjson" )
local resty_mc = require( "resty.memcached" )

local function new_mc()
    local mc, err = resty_mc:new({ key_transform = {
        function(a) return a end,
        function(a) return a end,
    } })
    if err then
        return nil
    end
    mc:set_timeout( 1000 )
    local ok, err = mc:connect( "127.0.0.1", 8001 )
    if not ok then
        return nil
    end
    return mc
end

function _M.new(opt)

    local e = {}
    setmetatable( e, _meta )
    return e
end

function _M:load(pobj)

    local mc = new_mc()
    if mc == nil then
        return nil
    end

    local o, flag, err = mc:get( table.concat( { pobj.cluster_id, pobj.ident }, '/' ) )
    if err then
        return nil
    end

    -- leave this to detect too many link
    -- mc:set_keepalive(10000, 100)
    mc:close()

    if o ~= nil then
        o = json.decode( o )
    end
    return o
end
function _M:store(pobj)

    local ok, err

    local mc = new_mc()
    if mc == nil then
        return nil, nil, nil
    end

    local mckey = table.concat( { pobj.cluster_id, pobj.ident }, '/' )

    -- record being nil means to delete
    if pobj.record == nil then
        ok, err = mc:delete(mckey)
    else
        local o = json.encode( pobj.record )
        ok, err = mc:set( mckey, o )
    end

    -- set_keepalive()
    mc:close()

    if not ok then
        return nil, (err or 'mc error'), nil
    end
    return nil, nil, nil
end

return _M
