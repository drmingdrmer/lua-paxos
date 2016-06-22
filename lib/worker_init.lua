
local tableutil = require( "acid.tableutil" )
local libluafs = require( "libluafs" )


local _M = {}

local err, errmes
local mem_index = ngx.config.prefix():sub(-2, -2)

local counter = 0
local function get_incr_num()
    counter = counter + 1
    return counter
end

local function _read(path)
    local f, err = io.open(path, "r")
    if err then
        if string.find(err, "No such file or directory") > 0 then
            return nil, nil, nil
        end
        return nil, "StorageError", err
    end

    local cont = f:read("*a")
    f:close()

    if cont == nil then
        return nil, "StorageError", path
    end

    return cont
end

local function _write(path, cont)

    local tmp_id = ngx.now() .. '_' .. ngx.worker.pid() .. '_' .. get_incr_num()

    local _path = path .. '_' .. tmp_id
    local f, err = io.open(_path, "w")
    f:write(cont)
    f:flush()
    f:close()

    local _, err = os.rename(_path, path)
    if err then
        os.remove(_path)
        return nil, "StorageError", err
    end

    return nil, nil, nil
end

local function fn_ids(fn)
    local hsh = ngx.crc32_long(fn)
    return {hsh, hsh+1}
end

local function fn_mems(fn)
    local ids = fn_ids(fn)
    local mems = cc:members()
    local rst = {}
    for i, id in ipairs(ids) do
        rst[i] = mems[ (id % #mems) + 1 ]
    end
    return rst
end

local function is_data_valid()
    local cont = _read('is_ok')
    if cont == nil then
        return false
    end

    local time = ngx.time()

    if ( tonumber(cont) or 0 ) > time - 30 then
        return true
    end
    return false
end

_M.ident = "127.0.0.1:990" .. mem_index
_M.cluster_id = "x"

_M.cc, err, errmes = require("nginx_cluster").new({
    cluster_id = _M.cluster_id,
    ident = _M.ident,
    get_standby = function()
        return {
            "127.0.0.1:9901",
            "127.0.0.1:9902",
            "127.0.0.1:9903",
            "127.0.0.1:9904",
            "127.0.0.1:9905",
            "127.0.0.1:9906",
        }
    end,

    is_data_valid = is_data_valid,

    restore = function()

        if is_data_valid() then
            return
        end

        local mems = _M.cc:members()
        local my_idx = 0
        for i, mm in ipairs(mems) do
            if mm == _M.ident then
                my_idx = i
                break
            end
        end

        local backups = {
            mems[ (my_idx-1) % #mems + 1 ],
            mems[ (my_idx+1) % #mems + 1 ],
        }

        -- for



    end,

    destory = function()
    end,

})

function _M.handle_get()
    local cc, ident = _M.cc, _M.ident

    -- strip /get/
    local fn = ngx.var.uri:sub(6)

    local mems = fn_mems(fn)

    local dst = mems[1]

    if dst == ident then
        ngx.req.set_uri("/www/" .. fn, true )
    else
        dst = dst:gsub("990", "980")
        ngx.req.set_uri("/proxy/" .. dst .. "/get/" .. fn, true)
    end
end

function _M.handle_ls()
    local rst, err_msg = libluafs.readdir( 'www' )
    table.sort(rst)
    local lst = table.concat('\n', rst)
    ngx.say(lst)
end

return _M
