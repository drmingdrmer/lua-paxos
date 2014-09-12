local _M = {}
local _meta = { __index=_M }

local resty_lock = require( "resty.lock" )

function _M.new(opt)
    local e = {}
    setmetatable( e, _meta )
    return e
end

function _M:lock(pobj, exptime)
    if exptime == nil then
        exptime = 60
    end

    local lockname = table.concat( {'paxos', pobj.cluster_id, pobj.ident}, '/' )

    local _lock = resty_lock:new( "paxos_lock", { exptime=exptime, timeout=1 } )
    local elapsed, err = _lock:lock( lockname )
    if err then
        return nil, err
    end

    return _lock, nil
end
function _M:unlock(_lock)
    if _lock ~= nil then
        _lock:unlock()
    end
    return nil
end

return _M
