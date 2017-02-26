local json = require( "cjson" )
local resty_lock = require("resty.lock")
local tableutil = require("acid.tableutil")

local _M = {}

-- TODO test

-- use cache must be declare shared dict 'paxos_shared_lock'
-- in nginx configuration

_M.shared_dict_lock = 'paxos_shared_lock'

_M.accessor = {
    proc = {
            get = function( dict, key, opts )

                    if opts.flush then
                        return nil
                    end

                    local val = dict[key]

                    if val ~= nil and val.expires > os.time() then
                        if opts.dup ~= false then
                            return tableutil.dup( val.data, true )
                        end
                        return val.data
                    end

                    return nil
                end,
            set = function( dict, key, val, opts )

                    if val ~= nil then
                        val = { expires = os.time() + (opts.exptime or 60),
                                data = val }
                    end

                    dict[key] = val
                end
    },

    shdict = {
            get = function( dict, key, opts )
                    if opts.flush then
                        return nil
                    end

                    local val = dict:get( key )
                    if val ~= nil then

                        val = json.decode( val )

                        return val
                    end
                    return nil
                end,
            set = function( dict, key, val, opts )

                    if val ~= nil then
                        val = json.encode(val)
                    end

                    dict:set( key, val, opts.exptime or 60 )
                end
    }

}

function _M.cacheable( dict, key, func, opts )

    local val
    local elapsed
    local err_code
    local err_msg

    opts = tableutil.dup( opts or {}, true )

    if opts.accessor == nil then

        opts.accessor = _M.accessor.proc

        if type(dict.flush_all) == 'function' then
            opts.accessor = _M.accessor.shdict
        end

    end

    opts.accessor = {
        get = opts.accessor.get or _M.accessor.proc.get,
        set = opts.accessor.set or _M.accessor.proc.set,
    }

    val = opts.accessor.get( dict, key, opts )
    if val ~= nil then
        return val, nil, nil
    end

    local lock, err_msg = resty_lock:new( _M.shared_dict_lock,
            { exptime=30, timeout= 1 } )
    if err_msg ~= nil then
        return nil, 'SystemError',
                err_msg .. ' while new lock:' .. _M.shared_dict_lock
    end

    elapsed, err_msg = lock:lock( tostring(dict) .. key )
    if err_msg ~= nil then

        return nil, 'LockTimeout', err_msg .. ' while lock:' .. key
    end

    val, err_code, err_msg =
        _M.cacheable_nolock( dict, key, func, opts )

    lock:unlock()

    return val, err_code, err_msg
end

function _M.cacheable_nolock( dict, key, func, opts )

    local val
    local err_code
    local err_msg

    val = opts.accessor.get( dict, key, opts )
    if val ~= nil then
        return val, nil, nil
    end

    val, err_code, err_msg = func(unpack(opts.args or {}))
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    opts.accessor.set( dict, key, val, opts )

    return val, nil, nil
end

return _M
