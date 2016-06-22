local _M = { _VERSION = require("acid.paxos._ver") }
local tableutil = require( "acid.tableutil" )
local base = require( "acid.paxos.base" )
local errors = base.errors

local nr_retry = 5

function _M.get_or_elect_leader(paxos, lease)

    local rst, err, errmes = paxos:local_get( 'leader' )
    if err then
        paxos:logerr( {err=err, errmes=errmes}, "while local_get('leader'):", paxos.member_id)
        return nil, err, errmes
    end

    -- start to track version change. if version changed,
    -- paxos stops any write operation.
    paxos.ver = rst.ver

    if rst.val ~= nil then
        return rst, nil, nil
    end

    paxos:logerr( rst, "leader not found for:", paxos.member_id)

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
    --     merge = { f1={ f2={} } },
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
    local c, err, errmes = paxos:read()
    if err then
        return nil, err, errmes
    end

    _M._merge_changes( c.val, changes.merge or {} )

    local cval = c.val

    local view, err, errmes = _M._make_2group_view(cval.view, changes )
    if err then

        if err == errors.DuringChange then
            paxos:sync()
            if #cval.view == 2 then
                table.remove( cval.view, 1 )
                _M._set_change_view( paxos, c )
            end
        elseif err == errors.NoChange then
            return c, nil, nil
        end

        return nil, err, errmes
    end

    cval.view = view

    local _, err, errmes = _M._set_change_view( paxos, c )
    if err then
        return nil, err, errmes
    end

    -- After update to dual-group view, commit once more with the new view to
    -- create new member.
    local _, err, errmes = paxos:sync()
    if err then
        return nil, err, errmes
    end

    table.remove( cval.view, 1 )
    return _M._set_change_view( paxos, c )
end

function _M._set_change_view( paxos, c )
    local _c, err, errmes = paxos:read()
    if err then
        return nil, err, errmes
    end

    if tableutil.eq( c.val, _c.val ) then

        local p, err, errmes = paxos:new_proposer()
        if err then
            return nil, err, errmes
        end

        local c, err, errmes = p:commit_specific(_c)
        if err then
            return nil, err, errmes
        end
        return c, nil, nil
    end

    local c, err, errmes = paxos:write(c.val)
    if err then
        return nil, err, errmes
    end

    return c, nil, nil
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

-- merge cannot be nested loop table
function _M._merge_changes(c, merge)

    for k, v in pairs(merge) do
        if type(v) == 'table' and type(c[k]) == 'table' then
            _M._merge_changes( c[k], v )
        else
            c[ k ] = v
        end
    end

    return c
end

function _M._sleep(paxos)
    if paxos.impl.sleep then
        paxos.impl:sleep()
    end
end

return _M
