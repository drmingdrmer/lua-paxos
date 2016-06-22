local _M = {}
local _meta = { __index=_M }

local json = require( "cjson" )
local libluafs = require( "libluafs" )
local strutil = require( "acid.strutil" )
local cache = require( "acid.cache" )
local tableutil = require("acid.tableutil")

local SP_LEN = 1

local VER_START = 1
local VER_LEN = 1
local VER_END = VER_START + VER_LEN - 1

local CHKSUM_START = VER_END + SP_LEN + 1
local CHKSUM_LEN = 8
local CHKSUM_END = CHKSUM_START + CHKSUM_LEN - 1

local CONT_START = CHKSUM_END + SP_LEN + 1

function _M.new(opt)

    opt = opt or {}

    local e = {
        sto_base_path = opt.sto_base_path or "/tmp"
    }
    setmetatable( e, _meta )
    return e
end

local counter = 0
local function get_incr_num()
    counter = counter + 1
    return counter
end

local function _chksum(cont)
    local c = ngx.crc32_long(cont)
    local chksum = string.format("%08x", c)
    return chksum
end
local function _read(path)
    local f, err = io.open(path, "r")
    if err then
        if string.find(err, "No such file or directory") ~= nil then
            return nil, nil, nil
        end
        return nil, "StorageError", 'open ' .. path .. ', ' .. tostring(err)
    end

    local cont = f:read("*a")
    f:close()

    if cont == nil then
        return nil, "StorageError", 'read ' .. path .. ', ' .. tostring(err)
    end

    return cont
end
local function _write(path, cont)

    local tmp_id = ngx.now() .. '_' .. ngx.worker.pid() .. '_' .. get_incr_num()

    local _path = path .. '_' .. tmp_id
    local f, err = io.open(_path, "w")
    if f == nil then
        return nil, "StorageError", 'open ' .. _path .. ', ' .. tostring(err)
    end

    local rst, err = f:write( cont )
    if rst == nil then
        f:close()
        os.remove(_path)
        return nil, "StorageError", 'read ' .. _path .. ', ' .. tostring(err)
    end

    f:flush()
    f:close()

    local _, err = os.rename(_path, path)
    if err then
        os.remove(_path)
        return nil, "StorageError", 'rename '.._path..' to '..path ..', '..tostring(err)
    end

    return nil, nil, nil
end

local function _base(path)
    local elts = strutil.split( path, '/' )
    elts[ #elts ] = nil
    local base = table.concat( elts, '/' )
    return base
end
local function _makedir(path)

    if libluafs.is_dir(path) then
        return nil, nil, nil
    end

    local _, err, errmes = _makedir(_base(path))
    if err then
        return nil, err, errmes
    end

    local ok = libluafs.makedir(path, 0755)
    if not ok then
        return nil, 'StorageError', 'failure makedir: ' .. tostring(path)
    end

    return nil, nil, nil
end

function _M:get_path(pobj)
    -- local path = table.concat( { pobj.cluster_id, pobj.ident }, '/' )

    local elts = {self.sto_base_path, pobj.cluster_id.."_"..pobj.ident..".paxos"}
    local path = table.concat(elts, "/")
    return path
end
function _M:_load(pobj)

    local path, err, errmes = self:get_path(pobj)
    if err then
        return nil, err, errmes
    end

    local raw, err, errmes = _read(path)
    if err then
        return nil, err, errmes
    end

    if raw == nil then
        return nil, nil, nil
    end

    local _ver = raw:sub( VER_START, VER_END )
    local ver = tonumber(_ver)
    if ver ~= 1 then
        return nil, "StorageError", "data version is invalid: " .. _ver
    end

    local chksum = raw:sub( CHKSUM_START, CHKSUM_END )

    local cont = raw:sub( CONT_START )
    local actual_chksum = _chksum(cont)

    if chksum ~= actual_chksum then
        return nil, "StorageError", "checksum unmatched: "..chksum .. ':' .. actual_chksum
    end

    local o = json.decode( cont )
    return o, nil, nil
end
function _M:load(pobj, isupdate)

    local key = table.concat( {'paxos', pobj.cluster_id, pobj.ident}, '/' )
    local opts = { exptime = 60 * 30,
                   args    = { self, pobj },
                   update_get = isupdate}

    local r, err, errmes = cache.cacheable( ngx.shared.paxos_shared_dict, key, _M._load, opts )
    if err then
        return nil, err, errmes
    end

    --temporary log error for debug cache
    if r ~= nil
        and r.committed ~= nil
        and r.committed.val ~= nil
        and r.committed.val.ec_meta ~= nil
        and r.committed.val.ec_meta.ec_name ~= pobj.cluster_id then
        local _r, _err, _errmes = self:_load( pobj )
        ngx.log(ngx.ERR, 'load committed error, key:', key, ', committed:', tableutil.repr(_r))
    end

    return r, nil, nil
end
function _M:store(pobj)

    local path, err, errmes = self:get_path(pobj)
    if err then
        return nil, err, errmes
    end

    if pobj.record == nil then
        ngx.log(ngx.INFO, "delete: ", path)
        os.remove(path)
        self:load( pobj, true )
        return nil, nil, nil
    end

    local _, err, errmes = _makedir( _base( path ) )
    if err then
        return nil, err, errmes
    end

    local cont = json.encode( pobj.record )

    local ver = "1"
    local chksum = _chksum(cont)

    _, err, errmes = _write(path, ver .. ' ' .. chksum .. ' ' .. cont )
    if err then
        return nil, err, errmes
    end

    self:load( pobj, true )
end

return _M
