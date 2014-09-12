local strutil = require( "acid.strutil" )
local tableutil = require( "acid.tableutil" )

--example, how to use
--   local h = s2http:new( ip, port, timeout )
--   h:request( uri, {method='GET', headers={}, body=''} )
--   status = h.status
--   headers = h.headers
--   buf = h:read_body( size )
--or
--   local h = s2http:new( ip, port, timeout )
--   h:send_request( uri, {method='GET', headers={}, body=''} )
--   h:send_body( body )
--   h:finish_request()
--   status = h.status
--   headers = h.headers
--   buf = h:read_body( size )


local DEF_PORT = 80
local DEF_METHOD = 'GET'
local DEF_TIMEOUT = 60000

local NO_CONTENT = 204
local NOT_MODIFIED = 304

local _M = { _VERSION = '1.0' }
local mt = { __index = _M }

local function to_str(...)

    local argsv = {...}

    for i=1, select('#', ...) do
        argsv[i] = tableutil.str(argsv[i])
    end

    return table.concat( argsv )

end
local function _trim( s )
    if type( s ) ~= 'string' then
        return s
    end
    return ( s:gsub( "^%s*(.-)%s*$", "%1" ) )
end

local function _read_line( self )
    return self.sock:receiveuntil('\r\n')()
end

local function _read( self, size )
    if size <= 0 then
        return '', nil
    end

    return self.sock:receive( size )
end

local function discard_lines_until( self, sequence )
    local skip, err_msg
    sequence = sequence or ''

    while skip ~= sequence do
        skip, err_msg = _read_line( self )
        if err_msg ~= nil then
            return 'SocketError', err_msg
        end
    end

    return nil, nil
end

local function _load_resp_status( self )
    local status
    local line
    local err_code
    local err_msg
    local elems

    while true do
        line, err_msg = _read_line( self )
        if err_msg ~= nil then
            return 'SocketError', to_str('read status line:', err_msg)
        end

        elems = strutil.split( line, ' ' )
        if table.getn(elems) < 3 then
            return 'BadStatus', to_str('invalid status line:', line)
        end

        status = tonumber( elems[2] )

        if status == nil or status < 100 or status > 999 then
            return 'BadStatus', to_str('invalid status value:', status)
        elseif 100 <= status and status < 200 then
            err_code, err_msg = discard_lines_until( self, '' )
            if err_code ~= nil then
                return err_code, to_str('read header:', err_msg )
            end
        else
            self.status = status
            break
        end
    end

    return nil, nil
end

local function _load_resp_headers( self )
    local elems
    local err_msg
    local line
    local hname, hvalue

    self.ori_headers = {}
    self.headers = {}

    while true do

        line, err_msg = _read_line( self )
        if err_msg ~= nil then
            return 'SocketError', to_str('read header:', err_msg)
        end

        if line == '' then
            break
        end

        elems = strutil.split( line, ':' )
        if table.getn(elems) < 2 then
            return 'BadHeader', to_str('invalid header:', line)
        end

        hname = string.lower( _trim( elems[1] ) )
        hvalue = _trim( line:sub(string.len(elems[1]) + 2) )

        self.ori_headers[_trim(elems[1])] = hvalue
        self.headers[hname] = hvalue
    end

    if self.status == NO_CONTENT or self.status == NOT_MODIFIED
        or self.method == 'HEAD' then
        return nil, nil
    end

    if self.headers['transfer-encoding'] == 'chunked' then
        self.chunked = true
        return nil, nil
    end

    local cont_len = self.headers['content-length']
    if cont_len ~= nil then
        cont_len = tonumber( cont_len )
        if cont_len == nil then
            return 'BadHeader', to_str('invalid content-length header:',
                    self.headers['content-length'])
        end
        self.cont_len = cont_len
        return nil, nil
    end

    return nil, nil
end

local function _norm_headers( headers )
    local hs = {}

    for h, v in pairs( headers ) do
        if type( v ) ~= 'table' then
            v = { v }
        end
        for _, header_val in ipairs( v ) do
            table.insert( hs, to_str( h, ': ', header_val ) )
        end
    end

    return hs
end

local function _read_chunk_size( self )
    local line, err_msg = _read_line( self )
    if err_msg ~= nil then
        return nil, 'SocketError', to_str('read chunk size:', err_msg)
    end

    local idx = line:find(';')
    if idx ~= nil then
        line = line:sub(1,idx-1)
    end

    local size = tonumber(line, 16)
    if size == nil then
        return nil, 'BadChunkCoding', to_str('invalid chunk size:', line)
    end

    return size, nil, nil
end

local function _next_chunk( self )

    local size, err_code, err_msg = _read_chunk_size( self )
    if err_code ~= nil then
        return err_code, err_msg
    end

    self.chunk_size = size
    self.chunk_pos = 0

    if size == 0 then
        self.body_end = true

        --discard trailer
        local err_code, err_msg = discard_lines_until( self, '' )
        if err_code ~= nil then
            return err_code, to_str('read trailer:', err_msg )
        end
    end

    return nil, nil
end

local function _read_chunk( self, size )
    local buf
    local err_code
    local err_msg
    local bufs = {}

    while size > 0 do
        if self.chunk_size == nil then
            err_code, err_msg = _next_chunk( self )
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            if self.body_end then
                break
            end
        end

        buf, err_msg = _read( self, math.min(size,
                self.chunk_size - self.chunk_pos))
        if err_msg ~= nil then
            return nil, 'SocketError', to_str('read chunked:', err_msg)
        end

        table.insert( bufs, buf )
        size = size - #buf
        self.chunk_pos = self.chunk_pos + #buf
        self.has_read = self.has_read + #buf

        -- chunk end, ignore '\r\n'
        if self.chunk_pos == self.chunk_size then
            buf, err_msg =  _read( self, #'\r\n')
            if err_msg ~= nil then
                return nil, 'SocketError', to_str('read chunked:', err_msg)
            end
            self.chunk_size = nil
            self.chunk_pos = nil
        end
    end

    return table.concat( bufs ), nil, nil
end

function _M.new( _, ip, port, timeout )

    timeout = timeout or DEF_TIMEOUT

    local sock= ngx.socket.tcp()
    sock:settimeout( timeout )

    local h = {
        ip = ip,
        port = port or DEF_PORT,
        timeout = timeout,
        sock = sock,
        has_read = 0,
        cont_len = 0,
        body_end = false,
        chunked  = false
    }

    return setmetatable( h, mt )
end

function _M.request( self, uri, opts )

    local err_code, err_msg = self:send_request( uri, opts )
    if err_code ~= nil then
        return err_code, err_msg
    end

    return self:finish_request()
end

function _M.send_request( self, uri, opts )

    opts = opts or {}

    self.uri = uri
    self.method = opts.method or DEF_METHOD

    local body = opts.body or ''
    local headers = opts.headers or {}
    headers.Host = headers.Host or self.ip
    if #body > 0 and headers['Content-Length'] == nil then
        headers['Content-Length'] = #body
    end

    local sbuf = {to_str(self.method, ' ', self.uri, ' HTTP/1.1'),
            unpack( _norm_headers( headers ) )
    }
    table.insert( sbuf, '' )
    table.insert( sbuf, body )

    sbuf = table.concat( sbuf, '\r\n' )

    local ret, err_msg = self.sock:connect( self.ip, self.port )
    if err_msg ~= nil then
        return 'SocketError', to_str('connect:', err_msg)
    end

    ret, err_msg = self.sock:send( sbuf )
    if err_msg ~= nil then
        return 'SocketError', to_str('request:', err_msg)
    end

    return nil, nil
end

function _M.send_body( self, body )
    local bytes = 0
    local err_msg

    if body ~= nil then
        bytes, err_msg = self.sock:send( body )
        if err_msg ~= nil then
            return nil, 'SocketError',
                to_str('send body:', err_msg)
        end
    end

    return bytes, nil, nil
end

function _M.finish_request( self )
    local err_code
    local err_msg

    err_code, err_msg = _load_resp_status( self )
    if err_code ~= nil then
        return err_code, err_msg
    end

    err_code, err_msg = _load_resp_headers( self )
    if err_code ~= nil then
        return err_code, err_msg
    end

    return nil, nil
end

function _M.read_body( self, size )

    if self.body_end then
        return '', nil, nil
    end

    if self.chunked then
       return _read_chunk( self, size )
    end

    local rest_len = self.cont_len - self.has_read

    local buf, err_msg = _read( self, math.min(size, rest_len))
    if err_msg ~= nil then
        return nil, 'SocketError', to_str('read body:', err_msg)
    end

    self.has_read = self.has_read + #buf

    if self.has_read == self.cont_len then
        self.body_end = true
    end


    return buf, nil, nil
end

function _M.set_keepalive( self, timeout, size )
    local rst, err_msg = self.sock:setkeepalive( timeout, size )
    if err_msg ~= nil then
        return 'SocketError', to_str('set keepalive:', err_msg)
    end

    return nil, nil
end

function _M.set_timeout( self, time )
    self.sock:settimeout( time )
end

function _M.close( self )
    local rst, err_msg = self.sock:close()
    if err_msg ~= nil then
        return 'SocketError', to_str('close:', err_msg)
    end

    return nil, nil
end

return _M
