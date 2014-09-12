local _M = { _VERSION = require("acid.paxos._ver") }

local tableutil = require( "acid.tableutil" )
local base = require( "acid.paxos.base" )
local acceptor = require( "acid.paxos.acceptor" )
local proposer = require( "acid.paxos.proposer" )

local errors = base.errors

_M.acceptor = acceptor
_M.proposer = proposer
_M.errors = errors

local _meth = {
    _VERSION = _M._VERSION,
    acceptor = acceptor,
    proposer = proposer,
    errors = errors,
    field_filter = {
        leader = function (c)
            if c.val == nil or c.val.__lease < 0 then
                c.val = nil
            end
            return c
        end,
    },
}
local _mt = { __index=_meth }

function _M.new(member_id, impl, ver)

    local member_id, err, errmes = _M.extract_memberid( member_id )
    if err then
        return nil, err, errmes
    end

    local p = {
        member_id = member_id,
        impl = impl,

        ver = ver
    }
    setmetatable(p, _mt)
    return p
end
function _M.extract_memberid(req)
    local member_id = tableutil.sub(req or {}, {"cluster_id", "ident"})
    if member_id.cluster_id and member_id.ident then
        return member_id, nil
    else
        return nil, errors.InvalidArgument, "cluster_id or ident not found"
    end
end

function _meth:set(field_key, field_val)

    if type(field_key) ~= 'string' then
        return nil, errors.InvalidArgument, 'field_key must be string for set'
    end

    local c, err, errmes = self:read()
    if err then
        return nil, err, errmes
    end

    if tableutil.eq( c.val[field_key], field_val ) then

        local p, err, errmes = self:new_proposer()
        if err then
            return nil, err, errmes
        end

        local c, err, errmes = p:commit_specific(c)
        if err then
            return nil, err, errmes
        end
        return { ver=c.ver, key=field_key, val=field_val }, nil
    end

    c.val[field_key] = field_val

    local c, err, errmes = self:write(c.val)
    if err then
        return nil, err, errmes
    end

    return { ver=c.ver, key=field_key, val=c.val[field_key] }
end
function _meth:get(field_key)

    if type(field_key) ~= 'string' then
        return nil, errors.InvalidArgument, 'field_key must be string or nil for get'
    end

    local c, err, errmes = self:quorum_read()
    if err then
        return nil, err, errmes
    end

    return self:_make_get_rst(field_key, c)
end
function _meth:quorum_read()

    local p, err, errmes = self:new_proposer()
    if err then
        return nil, err, errmes
    end

    local c, err, errmes = p:quorum_read()
    if err then
        return nil, err, errmes
    end
    return { ver=c.ver, val=c.val }, nil
end
function _meth:sync()

    local p, err, errmes = self:new_proposer()
    if err then
        return nil, err, errmes
    end

    local c, err, errmes = p:quorum_read()
    if err then
        return nil, err, errmes
    end

    local c, err, errmes = p:commit_specific(c)
    if err then
        return nil, err, errmes
    end

    -- newer version seen
    if self.ver ~= nil and self.ver ~= c.ver then
        return nil, errors.VerNotExist
    end

    return c, nil, nil
end

function _meth:write(val)
    local p, err, errmes = self:new_proposer()
    if err then
        return nil, err, errmes
    end

    local c, err, errmes = p:write(val)
    if err then
        return nil, err, errmes
    end

    if self.ver == nil or c.ver > self.ver then
        self.ver = c.ver
    end
    return c, nil, nil
end
function _meth:read()
    local p, err, errmes = self:new_proposer()
    if err then
        return nil, err, errmes
    end

    return p:read()
end

function _meth:send_req(ident, req)
    local p, err, errmes = self:new_proposer()
    if err then
        return nil, err, errmes
    end

    return self.impl:send_req(p, ident, req)
end

function _meth:local_get_mem(ident)
    if ident == nil then
        ident = self.member_id.ident
    end
    local _members, err, errmes = self:local_get_members()
    if err then
        return nil, err, errmes
    end
    return { ver=_members.ver, val=_members.val[ident] }, nil, nil
end
function _meth:local_get_members()
    local _view, err, errmes = self:local_get( 'view' )
    if err then
        return nil, err, errmes
    end

    local members = tableutil.union( _view.val )
    return {ver=_view.ver, val=members}, nil, nil
end
function _meth:local_get(field_key)

    if type(field_key) ~= 'string' then
        return nil, errors.InvalidArgument, 'field_key must be string or nil for get'
    end

    local c, err, errmes = self:read()
    if err then
        return nil, err, errmes
    end

    return self:_make_get_rst(field_key, c)
end
function _meth:_make_get_rst(key, c)
    -- if not found
    local val = c.val or {}

    c = { ver=c.ver, key=key, val=val[ key ] }

    local flt = self.field_filter[ key ]
    if flt ~= nil then
        c = flt(c)
    end

    return c, nil, nil
end

function _meth:logerr(...)
    self.impl:_log(1, ...)
end

function _meth:new_proposer()
    return proposer.new(self:_paxos_args(), self.impl)
end
function _meth:new_acceptor()
    return acceptor.new(self:_paxos_args(), self.impl)
end
function _meth:_paxos_args()
    local mid = tableutil.dup( self.member_id )
    mid.ver = self.ver
    return mid
end

return _M
