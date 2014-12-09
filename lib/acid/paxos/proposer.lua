local _M = { _VERSION = require("acid.paxos._ver") }

local tableutil = require( "acid.tableutil" )
local round = require( "acid.paxos.round" )
local base = require( "acid.paxos.base" )

local errors = base.errors

local _meth = { _VERSION = _M._VERSION }
local _mt = { __index = _meth }
tableutil.merge( _meth, base )

function _M.new( args, impl )

    local proposer = {
        cluster_id = args.cluster_id,
        ident = args.ident .. "",

        ver = args.ver,
        view = nil,
        acceptors = nil,

        -- stat becomes valid after self:decide()
        stat = nil,
        -- -- stat cummulated during paxos
        -- latest_committed = nil,
        -- latest_rnd = nil,
        -- stale_vers = nil,
        -- accepted_val = nil,

        rnd = round.new( { 0, args.ident } ),
        impl = impl,
    }
    setmetatable( proposer, _mt )

    if impl.new_rnd then
        proposer.rnd = round.new( { impl.new_rnd( proposer ), proposer.ident } )
    end

    -- load currently committed view for this proposer round

    local _, err, errmes = proposer:load_rec()
    if err then
        return nil, err, errmes
    end

    local _, err, errmes = proposer:init_view()
    if err then
        return nil, err, errmes
    end
    proposer.acceptors = tableutil.union( proposer.view, true )

    return proposer
end

-- high level api
function _meth:write( myval )
    -- accepted by quorum and commit to quorum.
    -- This might fail in two ways:
    --      1. not enough member to form a quorum.
    --      2. there is currently another value accepted.

    local is_mine, val, err = self:decide( myval )
    if err then
        return nil, err
    end

    local c, err = self:commit()
    if err then
        return nil, err
    end

    if c.val == myval then
        return { ver=c.ver, val=c.val }, nil
    else
        -- other value is committed
        return nil, errors.VerNotExist
    end

end
function _meth:read()
    local c = self.record.committed
    return { ver=c.ver, val=c.val }
end
function _meth:remote_read()
    return self:_remote_read(false)
end
function _meth:quorum_read()
    return self:_remote_read(true)
end
function _meth:_remote_read(need_quorum)

    -- to commit with empty data, response data contains committed data stored
    local _resps = self:phase3({ ver=0 })

    local resps = {}
    for id, resp in pairs(_resps) do
        if resp.err.Code == errors.AlreadyCommitted then
            resps[ id ] = resp
        end
    end

    if need_quorum and not self:is_quorum( resps ) then
        return nil, errors.QuorumFailure
    end

    local latest = { ver=0, val=nil }
    for _, resp in pairs(resps) do
        local c = resp.err.Message
        if c.ver ~= nil and c.ver > latest.ver then
            latest = c
        end
    end

    return latest
end
function _meth:decide( my_val )

    if self.stat ~= nil then
        return false, nil, errors.InvalidPhase, 'phase 1 and 2 has already run'
    end

    self.rnd = round.incr( self.rnd )

    local resps = self:phase1()
    local accepted_resps, val, stat = self:choose_p1( resps, my_val )

    self.stat = {}
    local st = self.stat

    st.phase1 = {
        ok = tableutil.keys( accepted_resps ),
        err = {},
    }
    for id, resp in pairs(resps) do
        if accepted_resps[ id ] == nil then
            st.phase1.err[ id ] = (resp.err or {}).Code
        end
    end

    tableutil.merge( st, stat )

    if round.cmp( stat.latest_rnd, self.rnd ) > 0 then
        self.rnd = round.new({ stat.latest_rnd[1], self.rnd[2] })
        return false, nil, errors.OldRound
    end

    if not self:is_quorum( accepted_resps ) then
        return false, nil, errors.QuorumFailure
    end

    resps = self:phase2( val )
    self.p2 = resps
    resps = self:choose_p2( resps )

    if not self:is_quorum( resps ) then
        return false, nil, errors.QuorumFailure
    end

    self.stat.accepted_val = val

    return val == my_val, val, nil
end
function _meth:commit()
    local c, err = self:make_commit_data()
    if err then
        return nil, err
    end
    return self:commit_specific(c)
end
function _meth:commit_specific(c)

    local resps = self:phase3(c)
    local positive = self:choose_p3( resps )
    local ok = self:is_quorum( positive )
    if ok then
        return { ver=c.ver, val=c.val }, nil
    else
        return nil, errors.QuorumFailure
    end
end

-- paxos level api
function _meth:phase1()
    local mes = {
        cmd = 'phase1',
        cluster_id = self.cluster_id,
        ver = self.ver + 1,
        rnd = self.rnd
    }
    return self:send_mes_all( mes )
end
function _meth:phase2(val)
    local mes = {
        cmd = 'phase2',
        cluster_id = self.cluster_id,
        ver = self.ver + 1,
        rnd = self.rnd,
        val = val,
    }
    return self:send_mes_all( mes )
end
function _meth:phase3(c)
    local req = {
        cmd = 'phase3',
        cluster_id = self.cluster_id,

        ver = c.ver,
        val = c.val,

        __tag = c.__tag,
    }
    return self:send_mes_all( req )
end
function _meth:choose_p1( resps, my_val )

    local accepted = {}
    local latest_rnd = self.rnd
    local latest_v_resp = { vrnd = round.zero(), v = nil, }
    local latest_committed = { ver=0, val=nil }
    local stale_vers = {}

    for id, resp in pairs(resps) do

        if resp.err == nil then

            if round.cmp( resp.rnd, self.rnd ) == 0 then
                accepted[ id ] = resp
            end

            latest_rnd = round.max( { latest_rnd, resp.rnd } )

            if round.cmp( resp.vrnd, latest_v_resp.vrnd ) > 0 then
                latest_v_resp = resp
            end

        elseif resp.err.Code == errors.AlreadyCommitted then

            local c = resp.err.Message
            if c.ver > latest_committed.ver then
                latest_committed = c
            end
        elseif resp.err.Code == errors.VerNotExist then
            local ver = resp.err.Message
            stale_vers[ id ] = ver
        end
    end

    if latest_committed.ver == 0 then
        latest_committed = nil
    end

    local stat = {
        latest_committed = latest_committed,
        latest_rnd = latest_rnd,
        stale_vers = stale_vers,
    }

    return accepted, ( latest_v_resp.val or my_val ), stat
end
function _meth:choose_p2( resps )
    return self:_choose_no_err( resps )
end
function _meth:choose_p3( resps )
    return self:_choose_no_err( resps )
end
function _meth:_choose_no_err(resps)
    local positive = {}
    for id, resp in pairs(resps) do
        if resp.err == nil then
            positive[ id ] = resp
        end
    end
    return positive
end

function _meth:make_commit_data()
    if self.stat == nil then
        return nil, errors.InvalidPhase, 'phase 1 or 2 has not yet run'
    end
    if self.stat.accepted_val == nil then
        return nil, errors.NotAccepted
    end
    local c = {
        ver=self.ver+1,
        val=self.stat.accepted_val,
        __tag = table.concat({
            self.cluster_id or '',
            self.ident or '',
            self.ver+1,
            table.concat(self.rnd, "-"),
        }, '/'),
    }
    return c
end
function _meth:is_quorum( accepted )

    for _, group in ipairs(self.view) do

        local n = tableutil.nkeys( tableutil.intersection( { accepted, group } ) )
        local total = tableutil.nkeys( group )

        if n <= total / 2 then
            return false
        end

    end

    return true
end
function _meth:send_mes_all( mes )
    local resps = {}
    for id, _ in pairs( self.acceptors ) do
        resps[ id ] = self.impl:send_req( self, id, mes )
    end
    return resps
end

function _meth:sync_committed()
    -- commit if there is stale record:
    --      1, version greater than current proposer found
    --      2, version lower than current proposer found

    local c = self:get_committed_to_sync()
    if c == nil then
        return nil
    end

    assert( c.ver > 0 )
    assert( c.ver >= self.ver )
    assert( c.val ~= nil )

    return self:commit_specific(c)
end
function _meth:get_committed_to_sync()
    local mine = self.record.committed
    local latest = self.stat.latest_committed or mine

    if latest.ver > mine.ver
        or tableutil.nkeys(self.stat.stale_vers) > 0 then
        return latest
    else
        return nil
    end
end

return _M
