local _M = {}
local _meta = { __index=_M }

local json = require( "cjson" )
local tableutil = require( "acid.tableutil" )
local strutil = require( "acid.strutil" )
local paxos = require( "acid.paxos" )
local http = require( "acid.impl.http" )

local errors = paxos.errors

local _status = {
    OK = 200,
    BadRequest = 400,
    InternalError = 500,
}

local err_to_status = {
    [errors.InvalidArgument]  = _status.BadRequest,
    [errors.InvalidMessage]   = _status.BadRequest,
    [errors.InvalidCommand]   = _status.BadRequest,
    [errors.InvalidCluster]   = _status.BadRequest,
    [errors.InvalidPhase]     = _status.BadRequest,
    [errors.VerNotExist]      = _status.BadRequest,
    [errors.AlreadyCommitted] = _status.BadRequest,
    [errors.OldRound]         = _status.BadRequest,
    [errors.NoView]           = _status.InternalError,
    [errors.QuorumFailure]    = _status.InternalError,
    [errors.LockTimeout]      = _status.InternalError,
    [errors.StorageError]     = _status.InternalError,
    [errors.NotAccepted]      = _status.BadRequest,
    [errors.DuringChange]     = _status.BadRequest,
    [errors.NoChange]         = _status.BadRequest,
    [errors.Conflict]         = _status.BadRequest,
    ["."]                     = _status.BadRequest,
}

function _M.new(opt)
    local e = {}
    setmetatable( e, _meta )
    return e
end

function _M:send_req(pobj, id, req)

    req = tableutil.dup( req )

    local uri = self.api_uri .. '/' .. table.concat({pobj.cluster_id, id, req.cmd}, '/')
    local query = ngx.encode_args({
        ver = req.ver
    })

    req.cluster_id = nil
    req.cmd = nil
    req.ver = nil

    local body = json.encode( req )
    local members = tableutil.union( pobj.view )
    local ipports = self:get_addrs({cluster_id=req.cluster_id, ident=id}, members[id])
    local ipport = ipports[1]
    local ip, port = ipport[1], ipport[2]
    local timeout = 6000 -- milliseconds
    local uri = uri .. '?' .. query

    local args = {
        body = body,
    }

    local h = http:new( ip, port, timeout )
    local err, errmes = h:request( uri, args )
    if err then
        self:track(
            "send_req-err:"..tostring(err)..','..tostring(errmes)
            ..",to:"..tostring(args.ip)..":"..tostring(args.port)..tostring(args.url)
        )
        return nil, err, errmes
    end
    local rstbody, err, errmes = h:read_body( 1024*1024 )
    if err then
        return nil, err, errmes
    end

    local rst, jbody = pcall(json.decode, rstbody)
    if not rst then
        return nil, errors.InvalidMessage, "body is not valid json"
    end

    return jbody
end

function _M:api_recv()

    local uri = ngx.var.uri
    uri = uri:sub( #self.api_uri + 2 )

    local elts = strutil.split( uri, '/' )

    local cluster_id, ident, cmd = elts[1], elts[2], elts[3]
    local uri_args = {
        cluster_id = cluster_id,
        ident = ident,
        cmd = cmd,
    }
    local query_args = ngx.req.get_uri_args()
    query_args.ver = tonumber( query_args.ver )

    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    local req = {}
    if body ~= "" and body ~= nil then
        req = json.decode( body )
        if req == nil then
            self:track( "api_recv-err:BodyIsNotJson" )
            return nil, errors.InvalidMessage, "body is not valid json"
        end
    end

    req = tableutil.merge(req, query_args, uri_args)
    return req, nil, nil
end
function _M:api_resp(rst)

    self:set_resp_log(rst)

    local code
    if type(rst) == 'table' and rst.err then
        code = err_to_status[rst.err] or err_to_status["."]
    else
        code = _status.OK
    end

    rst = json.encode( rst )
    ngx.status = code
    ngx.print( rst )
    ngx.eof()
    ngx.exit( ngx.HTTP_OK )
end

function _M:set_resp_log(rst)

    local str = tableutil.str

    if type(rst) == 'table' then

        local err = rst.err

        if err ~= nil then
            if err.Code == nil then
                self:logerr( "err without Code: ", err )
                return
            end

            self:track('err:' .. str(err.Code) .. ',' .. str(err.Message))
        else
            self:track('rst:' .. str(rst))
        end
    end
end

function _M:get_addrs(member_id, member)
    return nil, "NotImplemented"
end

return _M
