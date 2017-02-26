local _M = { _VERSION = "0.1" }

local json = require( "cjson" )

-- TODO test plain
function  _M.split( str, pat, plain )

    local t = {}  -- NOTE: use {n = 0} in Lua-5.0

    if pat == '' then
        for i = 1, #str do
            table.insert(t, str:sub(i, i))
        end
        t[1] = t[1] or ''
        return t
    end

    local last_end, s, e = 1, 1, 0

    while s do
        s, e = string.find( str, pat, last_end, plain )
        if s then
            table.insert( t, str:sub( last_end, s-1 ) )
            last_end = e + 1
        end
    end

    table.insert( t, str:sub( last_end ) )
    return t
end

function  _M.strip( s, ptn )

    if ptn == nil or ptn == "" then
        ptn = "%s"
    end

    local r = s:gsub( "^[" .. ptn .. "]+", '' ):gsub( "[" .. ptn .. "]+$", "" )
    return r
end

function  _M.startswith( s, pref )
    return s:sub( 1, pref:len() ) == pref
end

function  _M.endswith( s, suf )
    if suf == '' then
        return true
    end
    return s:sub( -suf:len(), -1 ) == suf
end

function _M.to_str(...)

    local argsv = {...}
    local v

    for i=1, select('#', ...) do
        v = argsv[i]
        if type(v) == 'table' then
            argsv[i] = json.encode(v)
        else
            argsv[i] = tostring(v)
        end
    end

    return table.concat(argsv)
end

-- TODO test
function _M._placeholder(val, pholder)
    pholder = pholder or '-'

    if val == '' or val == nil then
        return pholder
    else
        return val
    end
end


function _M.ljust(str, n, ch)
    return str .. string.rep(ch or ' ', n - string.len(str))
end

function _M.rjust(str, n, ch)
    return string.rep(ch or ' ', n - string.len(str)) .. str
end

function _M.replace(s, src, dst)
    return table.concat(_M.split(s, src), dst)
end

local function _parse_fnmatch_char(a1)
    if a1 == "*" then
        return ".*"
    elseif a1 == "?" then
        return "."
    elseif a1 == "." then
        return "[.]"
    else
        return a1
    end
end

function _M.fnmatch(s, ptn)
    local p = ptn
    local p = p:gsub('([\\]*)(.)', function(a0, a1)
        local l = #a0
        if l % 2 == 0 then
            return string.rep('[\\]', l/2).._parse_fnmatch_char(a1)
        else
            return string.rep('[\\]', (l-1)/2)..'['.. a1 ..']'
        end
    end)

    return s:match(p) == s
end

function _M.fromhex(str)
    return (str:gsub('..', function (cc)
        return string.char(tonumber(cc, 16))
    end))
end

function _M.tohex(str)
    return (str:gsub('.', function (c)
        return string.format('%02X', string.byte(c))
    end))
end

return _M
