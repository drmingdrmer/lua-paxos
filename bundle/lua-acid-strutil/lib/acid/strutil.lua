local _M = { _VERSION = "0.1" }

function  _M.split( str, pat )

    local t = {}  -- NOTE: use {n = 0} in Lua-5.0
    local last_end, s, e = 1, 1, 0

    while s do
        s, e = string.find( str, pat, last_end )
        if s then
            table.insert( t, str:sub( last_end, s-1 ) )
            last_end = e + 1
        end
    end

    table.insert( t, str:sub( last_end ) )
    return t
end

function  _M.strip( s, ptn )

    if ptn == nil then
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

return _M
