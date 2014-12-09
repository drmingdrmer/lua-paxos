local _M = {}
local _meta = { __index=_M }

local tableutil = require( "acid.tableutil" )
local acid_paxos = require( "acid.paxos" )
local paxoshelper = require( "acid.paxoshelper" )

local errors = acid_paxos.errors

_M._leader_lease = 1 -- seconds
_M.adm_method = {
    change_view = true,
    get_or_elect_leader = true,
    get = true,
    set = true,
    isalive = true,

    read = true,
}

local function _true() return true, nil, nil end

function _M.new(impl, opt)

    opt = opt or {}

    local srv = {
        impl = impl,
        handlers = opt.handlers or {},
    }

    setmetatable(srv, _meta)
    return srv
end

function _M:handle_req()

    local req, err, errmes = self.impl:api_recv()
    if err then
        self:err_exit(err, errmes)
    end

    local rst, err, errmes = self:_handle_req( req )

    if err then
        self:err_exit(err, errmes)
    end
    self:resp_exit(rst or {})
end
function _M:_handle_req(req)
    local cmd = req.cmd

    local member_id, err, errmes = acid_paxos.extract_memberid(req)
    if err then
        return nil, err, errmes
    end

    local paxos, err, errmes = self:new_paxos(member_id)
    if err then
        return nil, err, errmes
    end

    if paxos.acceptor.is_cmd(cmd) then

        local acceptor, err, errmes = paxos:new_acceptor()
        if err then
            return nil, err, errmes
        end
        return acceptor:process(req)

    elseif self.adm_method[ cmd ] then

        paxos.ver = req.ver

        local ph = paxoshelper
        local locked = paxoshelper.with_cluster_locked

        if cmd == 'change_view' then
            local changes = tableutil.sub( req, {"add", "del"} )
            return locked( paxos, function()
                return ph.change_view( paxos, changes )
            end)

        elseif cmd == 'get_or_elect_leader' then
            return locked( paxos, function()
                return ph.get_or_elect_leader( paxos, self._leader_lease )
            end)

        elseif cmd == 'get' then
            return paxos:get( req.key )

        elseif cmd == 'set' then
            local rst, err, errmes = paxos:set( req.key, req.val )
            if err then
                paxos:sync()
            end
            return rst, err, errmes

        elseif cmd == 'isalive' then
            return self:_isalive(paxos, req)

        elseif cmd == 'read' then
            -- low level read return entire paxos content in a single "val"
            -- field.
            return paxos:quorum_read()
        end

    elseif self.handlers[cmd] then
        paxos.ver = req.ver
        return self.handlers[cmd]( paxos, req )

    else
        return nil, errors.InvalidArgument, "invalid cmd: " .. tostring(cmd)
    end
end

function _M:_isalive(paxos, req)
    local _mem, err, errmes = paxos:local_get_mem()
    if err then
        return nil, err, errmes
    end
    if _mem.val == nil then
        return nil, "NotMember", nil
    end

    if self.impl:is_data_valid(paxos, _mem.val) then
        return {}, nil, nil
    else
        return nil, "Damaged", nil
    end
end

function _M:new_paxos(member_id, ver)
    return acid_paxos.new(member_id, self.impl, ver)
end

function _M:resp_exit(rst)
    self.impl:api_resp(rst)
end
function _M:err_exit(err, errmes)
    self.impl:api_resp({ err={ Code=err, Message=errmes } })
end
return _M
