local acid_paxos = require( "acid.paxos" )
local paxoshelper = require( "acid.paxoshelper" )
local paxosserver = require( "acid.paxosserver" )
local tableutil = require( "acid.tableutil" )

local _M = {
    _VERSION="0.1",

    dead_wait = {die_away = 60 * 20, restore = 60 * 60 * 2},
    dead_timeout = 86400,
    admin_lease = 60 * 2,
    longer_wait = 60 * 60 * 24,
    check_itv = {data_check = 60 * 2, leader_check = 10, cluster_check = 60 * 2},
    restore_data_check = 60 * 10,

    max_dead = 4,

    _check = {},
    _longer = {},
    _dead = {},
}
local _mt = { __index = _M }

function _M.new(impl, opt)
    opt = opt or {}

    local cluster = {
        impl = impl,

        check_itv = tableutil.merge(_M.check_itv, opt.check_itv or {}),
        dead_wait = tableutil.merge(_M.dead_wait, opt.dead_wait or {}),
        dead_timeout = opt.dead_timeout,
        admin_lease = opt.admin_lease,

        max_dead = opt.max_dead,
    }
    setmetatable( cluster, _mt )
    assert( cluster.admin_lease > 4, "lease must be long enough: > 4" )

    cluster.repair_timeout = math.min(math.ceil(cluster.admin_lease / 2), 5 * 60 )

    cluster.server = paxosserver.new(impl, {
        handlers = opt.handlers,
    })

    return cluster
end

function _M:delete_cluster(paxos)

    local members, err, errmes = paxos:local_get_members()
    if err then
        paxos:logerr( "delete_cluster, local_get_members :", err, errmes )
        return nil, err, errmes
    end

    local _, err, errmes = paxoshelper.change_view( paxos, { del = members.val } )
    if err then
        paxos:logerr( "delete_cluster, change_view :", err, errmes )
        return nil, err, errmes
    end

    return nil, nil, nil

end

function _M:is_member_longer( paxos, member_id )
    local _mem, err, errmes = paxos:local_get_mem()
    if err ~= nil then
        if err == 'NoView' then
            local now = ngx.now()
            local rec_ts = self:record_longer(
                    member_id.cluster_id, member_id.ident, now )
            paxos:logerr(" longer member :", member_id, now - rec_ts)
            return now - rec_ts > self.longer_wait
        end

        self:record_longer( member_id.cluster_id, member_id.ident, nil )
        return false
    end

    self:record_longer( member_id.cluster_id, member_id.ident, nil )
    return _mem.val == nil
end

function _M:record_longer( cluster_id, ident, time )
    self._longer[ident] = self._longer[ident] or {}

    local v = self._longer[ident]
    if time == nil then
        v[cluster_id] = nil
        return nil
    elseif time == -1 then
        return v[cluster_id]
    end

    v[cluster_id] = v[cluster_id] or time
    return v[cluster_id]
end

function _M:is_check_needed( check_type, member_id )
    -- check_type: data_check, leader_check, cluster_check
    self._check[check_type] = self._check[check_type] or {}

    local c = self._check[check_type]
    local exptime = c[member_id.cluster_id]

    local now = self.impl:time()
    if exptime == nil or exptime - now <= 0 then
        self:reset_check_exptime(
            check_type, member_id, now + self.check_itv[check_type])
        return true
    end

    return false
end

function _M:reset_check_exptime( check_type, member_id, exptime )
    self._check[check_type] = self._check[check_type] or {}

    local c = self._check[check_type]
    c[member_id.cluster_id] = exptime
end

function _M:is_leader( leader, member_id )
    return leader.ident == member_id.ident
end

function _M:leader_check( paxos )

    local rst, err, errmes = paxoshelper.get_or_elect_leader(paxos, self.admin_lease)
    if err then
        paxos.ver = nil
        paxos:sync()
        -- Incomplete view change might cause it to be unable to form a quorum
        -- in either previous or next view, thus it stalls leader election.
        --
        -- Push view to consistent state by finishing unfinished view change.
        local _, _err, _errmes = self:check_view( paxos )
        if _err ~= nil then
            paxos:logerr( "check view:", _err, _errmes, paxos.member_id )
        end
        return nil, err, 'get or elect leader, ' .. tostring(errmes)
    end

    local leader = rst.val

    if not self:is_leader( leader, paxos.member_id )
        or not self:is_check_needed( 'leader_check', paxos.member_id ) then
        return leader, nil, nil
    end

    local rst, err, errmes = self:extend_lease(paxos, leader)
    if err then
        return nil, err, 'failure extend leader lease, ' .. tostring(errmes)
    end

    return leader, nil, nil
end

function _M:cluster_check( paxos )

    if not self:is_check_needed( 'cluster_check', paxos.member_id ) then
        return nil, nil, nil
    end

    local _, err, errmes = self:check_view( paxos )
    if err ~= nil then
        paxos:logerr( "check view, ", err, errmes, paxos.member_id )
    end

    local _, err, errmes = self.impl:write_database(paxos)
    if err then
        paxos:logerr( 'write database, ', err, errmes, paxos.member_id )

        if err == 'SameIDCInDB' then
           local _, err, errmes = self:delete_cluster(paxos)
            if err then
                paxos:logerr( "delete cluster fail, ", err, errmes, paxos.member_id )
            end
            return nil, nil, nil
        end
    end

    local _, err, errmes = paxos.impl:wait_run(
            self.repair_timeout*0.9, self.repair_cluster, self, paxos)
    if err then
        paxos:logerr( 'repair cluster, ', paxos.member_id, err, errmes )
    end

    return nil, nil, nil
end

function _M:member_check(member_id)

    local paxos, err, errmes = acid_paxos.new(member_id, self.impl)
    if err then
        paxos:logerr("new paxos error:", err, errmes, member_id)
        return nil, err, errmes
    end

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

    if self:is_member_longer( paxos, paxos.member_id ) then
        return self:destory_member( paxos, paxos.member_id )
    elseif self:record_longer(
        member_id.cluster_id, member_id.ident, -1) ~= nil then
        return nil, nil, nil
    end

    local _, err, errmes = self:data_check(paxos)
    if err then
        paxos:logerr("data check:", err, errmes, member_id)
        return nil, err, errmes
    end

    local leader, err, errmes = self:leader_check(paxos)
    if err then
        paxos:logerr("leader check:", err, errmes, member_id)
        return nil, err, errmes
    end

    if self:is_leader( leader, member_id ) then
        local rst, err, errmes = self:cluster_check(paxos)
        if err then
            paxos:logerr("cluster check:", err, errmes, member_id)
            return nil, err, errmes
        end
    else
        self._dead[member_id.cluster_id] = nil
    end

    return nil, nil, nil
end

function _M:data_check(paxos)

    if not self:is_check_needed( 'data_check', paxos.member_id ) then
        return nil, nil, nil
    end

    local _mem, err, errmes = paxos:local_get_mem()
    if err then
        return nil, err, errmes
    end

    local _, err, errmes = self.impl:restore(paxos, _mem.val)
    if err then
        -- to data check, after restore_data_check times
        local now = self.impl:time()
        local exptime = now + self.restore_data_check
        self:reset_check_exptime('data_check', paxos.member_id, exptime)
    end
end

function _M:destory_member( paxos, member_id )

    paxos:logerr("i am no longer a member of cluster:", member_id)

    local _, err, errmes = self.impl:destory(paxos)
    if err then
        paxos:logerr("destory err:", err, errmes)
        return nil, err, errmes
     else
        local acc, err, errmes = paxos:new_acceptor()
        if err then
            return nil, err, errmes
        end

        local _mem, err, errmes = paxos:local_get_mem()
        if err then
            return nil, err, errmes
        end

        local rst, err, errmes = acc:destory(_mem.ver)
        paxos:logerr("after destory acceptor:", rst, err, errmes)
    end

    self:record_longer( member_id.cluster_id, member_id.ident, nil )
end

function _M:extend_lease(paxos, leader)

    if leader.__lease < self.repair_timeout then

        local rst, err, errmes = paxoshelper.elect_leader(paxos, self.admin_lease)
        if err then
            return nil, err, errmes
        end
    end
    return true, nil, nil
end
function _M:repair_cluster(paxos)

    local down_members, err, errmes = self:find_down(paxos)
    if err then
        return nil, err, errmes
    end

    local migrate_members = down_members.migrating or {}
    down_members.migrating = nil

    local dead_members = down_members.dead or {}

    if #dead_members > 0 then
        paxos:logerr( "dead members confirmed:", dead_members )
    end

    local cluster_id = paxos.member_id.cluster_id

    local nr_down = 0
    for _, m in pairs(down_members) do
        nr_down = nr_down + #m
    end

    if nr_down > self.max_dead then
        paxos:logerr( cluster_id, #dead_members, nr_down,
                        " members down, too many, can not repair" )
        return
    end

    for _, _m in ipairs( dead_members ) do
        local ident, member = _m[1], _m[2]
        local _, err, errmes = self:replace_dead( paxos, ident, member )
        if err then
            paxos:logerr( " replace_dead error, ", err, ":", errmes )
        else
            self:record_down(cluster_id, ident, nil, nil)
        end

        -- fix only one each time
        return
    end

    -- only all member is alive, to migrate it
    -- TODO : if the first sorted member of a cluster is repairing for a long time,
    --        disk space of the member will be full all the time.
    if nr_down == 0 then
        for _, _m in ipairs( migrate_members ) do
            local ident, member = _m[1], _m[2]
            local _, err, errmes = self:replace_dead( paxos, ident, member )
            if err then
                paxos:logerr( " migrate member error, ", err, ":", errmes )
            end
            return
        end
    end
end

function _M:find_down(paxos)

    local _members, err, errmes = paxos:local_get_members()
    if err then
        return nil, err, errmes
    end

    local down_members = {}

    for ident, member in pairs(_members.val) do
        local status, ts, mes = self:confirmed_status(paxos, ident, member)
        if status ~= 'alive' then
            down_members[status] = down_members[status] or {}
            table.insert( down_members[status], { ident, member, ts, mes } )
        end
    end

    if next(down_members) ~= nil then
        paxos.impl:wait_run(30,
                self.report_cluster, self, paxos, down_members)
    end

    return down_members, nil, nil
end

-- status: alive, die_away, restore, migrating, dead
function _M:confirmed_status(paxos, ident, member)

    local cluster_id = paxos.member_id.cluster_id

    local rst, err, errmes = self:send_member_alive(paxos, ident)
    if err == nil then
        self:record_down(cluster_id, ident, nil, nil)
        return 'alive', nil, nil
    end

    local status = 'die_away'
    if err == 'Damaged' then
        status = 'restore'
    elseif err == 'Migrating' then
        paxos:logerr( "detect status : migrating ", ident, cluster_id)
        return 'migrating', nil, nil
    end

    local now = paxos.impl:time()
    local rec = self:record_down(cluster_id, ident, now, status)

    if rec['restore'] ~= nil then
        status = 'restore'
    end

    local ts = now - rec[status]
    paxos:logerr( "detect status :", status,
                    ' times :', ts, ident, cluster_id, err, errmes )

    if ts > ( self.dead_wait[status] or 0  ) then
        paxos:logerr("confirmed dead :", ident, cluster_id, status, ts)
        status = 'dead'
    end

    return status, ts, tostring(err) .. ':' .. tostring(errmes)
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

    local ec_meta, err, errmes = paxos:local_get('ec_meta')
    if err then
        return nil, err, errmes
    end

    ec_meta = ec_meta.val
    local set_db = ec_meta.set_db or {cur=ec_meta.ec_name, next=ec_meta.ec_name}

    local changes = {
        add = new_mem,
        del = { [dead_ident]=dead_mem },
        merge = {ec_meta={set_db=set_db}}
    }

    local rst, err, errmes = paxoshelper.change_view( paxos, changes )
    if err then
        return nil, err, errmes
    end

    local _, err, errmes = self.impl:write_database(paxos)
    if err then
        return nil, err, errmes
    end

    paxos:logerr( "view changed, changes: ", changes, ", view:", rst )
    return rst, err, errmes
end
function _M:record_down(cluster_id, ident, time, status)

    local cd = self._dead
    cd[cluster_id] = cd[cluster_id] or {}

    local d = cd[cluster_id]

    if time == nil then
        d[ident] = nil
        return nil
    end

    d[ident] = tableutil.merge( {[status]=time}, d[ident] or {} )

    return d[ident]
end
function _M:send_member_alive(paxos, ident)
    local rst, err, errmes = paxos:send_req(ident, { cmd = "isalive", })
    if err == nil and rst.err == nil then
        return nil, nil, nil
    end

    local cluster_id = paxos.member_id.cluster_id

    if err == nil then
        local e = rst.err or {}
        err = e.Code
        errmes = e.Message
    end

    return nil, err, errmes
end

function _M:check_view(paxos)
    local view, err, errmes = paxos:local_get( 'view' )
    if err ~= nil then
        return nil, err, errmes
    end

    if #view.val == 1 then
        return nil, nil, nil
    end

    paxos:sync()
    return paxoshelper.change_view( paxos, {} )
end

function _M:report_cluster(paxos, down_members)
    local tb = {}
    for status, members in pairs( down_members ) do
        for _, member in pairs( members ) do
            local ident, mem, ts, mes = unpack( member )
            ts = ts or 0
            if status ~= 'die_away'
                or ts > math.max(60 * 60 * 4, self.dead_wait[status])  then
                table.insert(tb,
                    {status=status, index=mem.index, ident=ident, ts=ts, mes=mes})
            end
        end
    end

    if #tb > 0 then
        self.impl:report_cluster( paxos, tb )
    end
end

return _M
