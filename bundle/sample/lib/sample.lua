local acid_cluster = require( "acid.cluster" )
local impl_ngx = require( "acid.impl_ngx" )

local _M = {}

local impl = impl_ngx.new({})
impl.api_uri = '/api'
impl.tracking_varname = 'paxos_log'
impl.get_addrs = function( impl, member_id, member )
    local ident = member_id.ident
    return { {'127.0.0.1', 9080+tonumber(ident)} }
end
-- impl.store = function(impl, pobj) end
-- impl.load = function(impl, pobj) end
-- impl.is_data_valid = function(impl, paxos, member) return true end
-- impl.restore = function(impl, paxos, memeber) end
-- impl.destory = function(impl, paxos) end
impl.new_member = function (impl, paxos, dead_ident, members)

    -- members = { <ident>={..}, <ident>={..}, ... }
    local cluster_id = paxos.member_id.cluster_id
    local dead_mem = members[dead_ident]

    for i = 1, 4 do
        local ident = tostring(i)
        if members[ident] == nil then
            return { [ident]=ident }
        end
    end
    return nil, "NoFreeMember"
end

_M.cluster = acid_cluster.new(impl, {
    dead_wait = 3,
    admin_lease = 5,
    max_dead = 1,
})

local function list_member_ids()
    local ms = {}
    for _, ident in ipairs(_M.members_on_this_node) do
        table.insert( ms, {cluster_id="x", ident=ident} )
    end
    return ms
end
function _M.init_cluster_check(enabled)

    if not enabled then
        return
    end

    local check_interval = 1

    local timer_work

    timer_work = function (premature)

        if premature then
            -- worker is shutting down
            return
        end

        for _, mid in ipairs( list_member_ids() ) do
            local rst, err, errmes = _M.cluster:member_check(mid)
            if err then
                ngx.log( ngx.ERR, 'member_check: ', rst, ' ', err, ' ', tostring(errmes) )
            end
        end

        local ok, err = ngx.timer.at( check_interval, timer_work )
    end
    local ok, err = ngx.timer.at( check_interval, timer_work )
end

return _M
