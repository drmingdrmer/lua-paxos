local acid_cluster = require( "acid.cluster" )
local impl_ngx = require( "acid.impl_ngx" )

local _M = {}

local function make_loop_member_creator(idents_getter)

    local i = 0

    return function(impl, paxos, dead_ident, members)
        local idents = idents_getter(paxos)
        for ii = 1, #idents do
            i = i + 1
            local new_id = idents[(i % #idents) + 1]
            if members[new_id] == nil then
                return {[new_id]=members[dead_ident]}
            end
        end
        return nil, 'NoFreeMember'
    end
end

local function init_cluster_check(cluster, member_id, interval)

    local checker

    checker = function(premature)

        if premature then
            return
        end

        cluster:member_check(member_id)

        local ok, err = ngx.timer.at(interval, checker)
    end
    local ok, err = ngx.timer.at( 0, checker )
end

function _M.new(opt)
    -- opt = {
    --  path = 'paxos data path',
    --  check_interval = 5,
    --  new_member = function() end,
    --  cb_commit = funtion(committed) end,
    -- }

    local check_interval = opt.check_interval or 5
    local dead_wait = opt.dead_wait or 60

    local impl = impl_ngx.new({})
    local cache = {}

    impl.sto_base_path = opt.path or ngx.config.prefix()
    impl.cb_commit = function (committed)
        cache.committed = committed
    end

    if opt.get_standby ~= nil then
        impl.new_member = make_loop_member_creator(opt.get_standby)
    else
        impl.new_member = function()
            return nil, 'NoFreeMember'
        end
    end

    impl.is_data_valid = function(impl, paxos, member)
        if opt.is_data_valid then
            return opt.is_data_valid()
        else
            return true
        end
    end
    impl.restore = function(impl, paxos, member)
        if opt.restore then
            return opt.restore()
        else
            return nil
        end
    end
    impl.destory = function(impl, paxos)
        if opt.destory then
            return opt.destory()
        else
            return nil
        end
    end

    local cluster = acid_cluster.new(impl, {
        dead_wait = dead_wait,
        admin_lease = opt.admin_lease or (check_interval*2) or 60,
        max_dead = opt.max_dead or 1,
    })

    local mid = {cluster_id=opt.cluster_id, ident=opt.ident}

    -- init cache
    local paxos, err, errmes = cluster.server:new_paxos(mid)
    if err then
        return nil, err, errmes
    end
    cache.committed = paxos:read()

    cluster.get = function(_, field_key)
        local c = cache.committed or {}

        -- _make_get_rst does not return err
        local _v = paxos:_make_get_rst(field_key, c)
        return _v.val
    end
    cluster.members = function(_)
        local view = cluster:get('view')
        if view == nil then
            return nil
        end

        local mems = {}
        for id, i in pairs(view[1]) do
            mems[i] = id
        end
        return mems
    end

    init_cluster_check(cluster, mid, check_interval)

    return cluster
end

return _M
