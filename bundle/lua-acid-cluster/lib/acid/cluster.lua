local acid_paxos = require( "acid.paxos" )
local paxoshelper = require( "acid.paxoshelper" )
local paxosserver = require( "acid.paxosserver" )

local _M = {
    _VERSION="0.1",

    dead_wait = 60*20,
    dead_timeout = 86400,
    admin_lease = 60*2,

    max_dead = 4,

    _dead = {},
}
local _mt = { __index = _M }

local function _true() return true, nil, nil end

function _M.new(impl, opt)
    opt = opt or {}

    local cluster = {
        impl = impl,

        dead_wait = opt.dead_wait,
        dead_timeout = opt.dead_timeout,
        admin_lease = opt.admin_lease,

        max_dead = opt.max_dead,
    }
    setmetatable( cluster, _mt )
    assert( cluster.admin_lease > 4, "lease must be long enough: > 4" )

    cluster.repair_timeout = math.floor(cluster.admin_lease / 2)

    cluster.server = paxosserver.new(impl, {
        handlers = opt.handlers,
    })

    return cluster
end

function _M:member_check(member_id)

    local paxos, err, errmes = acid_paxos.new(member_id, self.impl)
    if err then
        paxos:logerr("new paxos error:", err, errmes)
        return nil, err, errmes
    end

    local _, err, errmes = self:data_check(paxos)
    if err then
        return nil, err, errmes
    end

    local rst, err, errmes = paxoshelper.get_or_elect_leader(paxos, self.admin_lease)
    if err then
        paxos:logerr( "get_or_elect_leader:", err, errmes, paxos.member_id )

        -- Incomplete view change might cause it to be unable to form a quorum
        -- in either previous or next view, thus it stalls leader election.
        --
        -- Push view to consistent state by finishing unfinished view change.
        local _, err, errmes = paxoshelper.change_view( paxos, {} )
        if err then
            paxos:logerr( "change_view with {}:", err, errmes )
        end

        return
    end

    local leader = rst.val
    -- start to track version change. if version changed, paxos stops any write
    -- operation.
    paxos.ver = rst.ver

    if leader.ident == member_id.ident then

        if self:extend_lease(paxos, leader) then
            local _, err, errmes = paxos.impl:wait_run(
                    self.repair_timeout*0.9,
                    self.repair_cluster, self, paxos)

            if err then
                paxos:logerr( 'repair_cluster', paxos.member_id, err, errmes )
            end
        end
    end

end
function _M:data_check(paxos)
    -- For leader or not:
    -- Local storage checking does not require version tracking for paxos.
    --
    -- If it is not a member of any view, it is definitely correct to destory
    -- this member with all data removed.
    --
    -- Race condition happens if:
    --
    --      While new version of view has this member and someone else has been
    --      initiating this member.
    --
    --      While timer triggered routine has been removing data of this
    --      member.
    --
    -- But it does not matter. Data will be re-built in next checking.

    local _mem, err, errmes = paxos:local_get_mem()
    if err then
        paxos:logerr("local_get_mem err:", err, errmes)
        return nil, err, errmes
    end

    if _mem.val ~= nil then
        self.impl:restore(paxos, _mem.val)
        return
    end

    paxos:logerr("i am no longer a member of cluster:", paxos.member_id)

    local _, err, errmes = self.impl:destory(paxos)
    if err then
        paxos:logerr("destory err:", err, errmes)
    else
        local acc, err, errmes = paxos:new_acceptor()
        if err then
            return nil, err, errmes
        end
        local rst, err, errmes = acc:destory(_mem.ver)
        paxos:logerr("after destory acceptor:", rst, err, errmes)
    end
end
function _M:extend_lease(paxos, leader)

    if leader.__lease < self.repair_timeout then

        local rst, err, errmes = paxoshelper.elect_leader(paxos, self.admin_lease)
        if err then
            paxos:logerr( "failure extend lease:", leader )
            return nil, err, errmes
        end
    end
    return true, nil, nil
end
function _M:repair_cluster(paxos)

    local dead_members, err, errmes = self:find_dead(paxos)
    if err then
        return nil, err, errmes
    end

    local nr_dead = #dead_members
    if nr_dead > 0 then
        paxos:logerr( "dead members confirmed:", dead_members )
    end

    if nr_dead > self.max_dead then
        paxos:logerr( nr_dead, " members down, too many, can not repair" )
        return
    end

    for _, _m in ipairs( dead_members ) do
        local ident, member = _m[1], _m[2]
        self:replace_dead( paxos, ident, member )

        -- fix only one each time
        return
    end
end
function _M:find_dead(paxos)

    local _members, err, errmes = paxos:local_get_members()
    if err then
        return nil, err, errmes
    end

    local dead_members = {}

    for ident, member in pairs(_members.val) do
        if self:is_confirmed_dead(paxos, ident, member) then
            table.insert( dead_members, { ident, member } )
        end
    end

    return dead_members, nil, nil
end
function _M:is_confirmed_dead(paxos, ident, member)

    local cluster_id = paxos.member_id.cluster_id

    if self:is_member_alive(paxos, ident) then
        self:record_dead(cluster_id, ident, nil)
        return false
    end

    paxos:logerr("dead detected:", ident, member)

    local now = paxos.impl:time()
    local dead_time = self:record_dead(cluster_id, ident, now)

    if dead_time > self.dead_wait then
        return true
    end

    return false
end
function _M:replace_dead(paxos, dead_ident, dead_mem)

    local _members, err, errmes = paxos:local_get_members()
    if err then
        return nil, err, errmes
    end

    local new_mem, err, errmes = self.impl:new_member(paxos, dead_ident, _members.val )
    if err then
        return nil, err, errmes
    end

    local changes = {
        add = new_mem,
        del = { [dead_ident]=dead_mem },
    }
    local rst, err, errmes = paxoshelper.change_view( paxos, changes )

    paxos:logerr( "view changed: ", rst, err, errmes )
    return rst, err, errmes
end
function _M:record_dead(cluster_id, ident, time)

    local cd = self._dead
    cd[cluster_id] = cd[cluster_id] or {}

    local d = cd[cluster_id]

    if time == nil then
        d[ident] = nil
        return nil
    end

    local prev = d[ident] or time
    -- Discard record that is too old, which might be caused by leader
    -- switching
    if prev < time - self.dead_timeout then
        d[ident] = nil
    end
    d[ident] = d[ident] or time
    return time - d[ident]
end
function _M:is_member_alive(paxos, ident)
    local rst, err, errmes = paxos:send_req(ident, { cmd = "isalive", })
    return (err == nil and rst.err == nil), nil, nil
end

return _M
