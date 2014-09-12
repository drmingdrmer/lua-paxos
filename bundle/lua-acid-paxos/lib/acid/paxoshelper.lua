local _M = { _VERSION = require("acid.paxos._ver") }
local tableutil = require( "acid.tableutil" )
local base = require( "acid.paxos.base" )
local errors = base.errors

local nr_retry = 5

function _M.get_or_elect_leader(paxos, lease)

    local rst, err, errmes = paxos:get( 'leader' )
    if err then
        return nil, err, errmes
    end

    if rst.val ~= nil then
        return rst, nil, nil
    end

    return _M.elect_leader( paxos, lease )
end
function _M.elect_leader(paxos, lease)
    return paxos:set( 'leader', {
        ident = paxos.member_id.ident,
        __lease = lease,
    })
end
function _M.change_view(paxos, changes)
    -- changes = {
    --     add = { a=1, b=1 },
    --     del = { c=1, d=1 },
    -- }

    -- change_view push cluster to a consistent state:
    --      . changes applied to cluster
    --      . other process is in progress changing view, apply it and return
    --        error DuringChange
    --      . no change. do nothing and return error
    --      . paxos race condition. update it

    -- after this step completed, it is required to track version changing to
    -- make sure no other change_view to break this procedure.

    for _ = 1, nr_retry do

        local c, err, errmes = _M._change_view( paxos, changes )

        if err == errors.QuorumFailure then
            _M._sleep(paxos)
            paxos:sync()
        else
            return c, err, errmes
        end
    end
    return nil, errors.QuorumFailure
end
function _M._change_view(paxos, changes)

    local c, err, errmes = paxos:local_get("view")
    if err then
        return nil, err, errmes
    end

    local view, err, errmes = _M._make_2group_view(c.val, changes )
    if err then

        if err == errors.DuringChange then
            paxos:sync()
            local view = c.val
            if #view == 2 then
                table.remove( view, 1 )
                paxos:set( "view", view )
            end
        elseif err == errors.NoChange then
            return c, nil, nil
        end

        return nil, err, errmes
    end

    local c, err, errmes = paxos:set( "view", view )
    if err then
        return nil, err, errmes
    end

    -- After update to dual-group view, commit once more with the new view to
    -- create new member.
    local c, err, errmes = paxos:sync()
    if err then
        return nil, err, errmes
    end

    table.remove( view, 1 )
    return paxos:set( "view", view )
end

function _M.with_cluster_locked(paxos, f, exptime)

    local cluster_id = paxos.member_id.cluster_id
    local _mid = {
        cluster_id= cluster_id,
        ident='__admin',
    }

    local _l, err = paxos.impl:lock(_mid, exptime)
    if err then
        return nil, errors.LockTimeout, err
    end

    local rst, err, errmes = f()

    paxos.impl:unlock(_l)
    return rst, err, errmes
end

function _M._make_2group_view(view, changes)
    -- Create intermedia view.
    -- Intermedia view of val contains 2 groups: the current group and the
    -- group to change to.

    local view = tableutil.dup( view, true )
    if view[ 2 ] ~= nil then
        return nil, errors.DuringChange
    end

    view[ 2 ] = tableutil.dup( view[ 1 ], true )

    tableutil.merge( view[ 2 ], changes.add or {} )
    for k, v in pairs(changes.del or {}) do
        view[ 2 ][ k ] = nil
    end

    if tableutil.eq( view[1], view[2] ) then
        return nil, errors.NoChange
    end

    return view, nil, nil
end
function _M._sleep(paxos)
    if paxos.impl.sleep then
        paxos.impl:sleep()
    end
end

return _M
