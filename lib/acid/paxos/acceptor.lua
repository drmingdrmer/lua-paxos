local _M = { _VERSION = require("acid.paxos._ver") }

local tableutil = require( "acid.tableutil" )
local base = require( "acid.paxos.base" )
local round = require( "acid.paxos.round" )

local errors = base.errors

local _meth = { _VERSION = _M._VERSION }
local _mt = { __index = _meth }
tableutil.merge( _meth, base )

local _api = {
    phase1 = true,
    phase2 = true,
    phase3 = true,
}

local function is_next_ver(self)

    if self.mes.ver <= self.ver then
        self.err = {
            Code = errors.AlreadyCommitted,
            Message = self.record.committed,
        }
        return false

    elseif self.mes.ver > self.ver + 1 then
        self.err = {
            Code = errors.VerNotExist,
            Message = self.ver,
        }
        return false
    end
    return true
end
local function committable(self)
    local c = self.record.committed

    if self.mes.ver < c.ver or self.mes.ver < 1 then
        self.err = {
            Code = errors.AlreadyCommitted,
            Message = c,
        }
        return false
    end
    return true
end

local function is_committed_valid( self )

    -- TODO move to upper level
    -- local r_cluster_id = self.record.committed.val.ec_meta.ec_name
    -- if  r_cluster_id ~= self.cluster_id then
    --     self.err = {
    --         Code = errors.InvalidCommitted,
    --         Message = {r_cluster_id=r_cluster_id, cluster_id=self.cluster_id}
    --     }
    --     return false
    -- end
    return true
end

function _M.new(args, impl)
    local acc = {
        cluster_id = args.cluster_id,
        ident = args.ident,
        ver = args.ver,

        mes = nil,

        view = nil,
        record = nil,
        err = nil,

        impl = impl,
    }
    setmetatable( acc, _mt )

    if acc.cluster_id == nil
        or acc.ident == nil
        or acc.impl.store == nil
        or acc.impl.load == nil
        or acc.impl.lock == nil
        or acc.impl.unlock == nil
        then
        return nil, errors.InvalidArgument
    end

    return acc, nil
end
function _M.is_cmd(cmd)
    return _api[ cmd ]
end

function _meth:destory(ver)
    local _l, err = self.impl:lock(self)
    if err then
        return nil, errors.LockTimeout, nil
    end

    local _, err, errmes = self:load_rec()
    if err then
        return nil, err, errmes
    end

    local _, err, errmes = self:init_view()
    if err then
        return nil, err, errmes
    end

    local c = self.record.committed

    local rst, err, errmes
    if ver == c.ver then

        self.record = nil
        rst, err, errmes = self.impl:store(self)
        if err then
            rst, err, errmes = nil, errors.StorageError, nil
        else
            rst, err, errmes = nil, nil, nil
        end
    else
        rst, err, errmes = nil, errors.VerNotExist, c.ver
    end

    self.impl:unlock(_l)

    return rst, err, errmes
end

function _meth:process(mes)

    mes = mes or {}
    self.mes = mes

    if mes.cluster_id == nil then
        return nil, errors.InvalidMessage, "cluster_id is nil"
    end

    if not _api[ mes.cmd ] then
        return nil, errors.InvalidCommand, nil
    end

    if mes.cluster_id ~= self.cluster_id then
        return nil, errors.InvalidCluster, self.cluster_id
    end

    -- TODO pcall
    local _l, err = self.impl:lock(self)
    if err then
        self.impl:logerr("acceptor process lock timeout: ", self.cluster_id)
        return nil, errors.LockTimeout, nil
    end

    local rst, err, errmes = self[ self.mes.cmd ]( self )

    self.impl:unlock(_l)

    return rst, err, errmes
end
function _meth:store_or_err()

    if not is_committed_valid( self ) then
        return nil, self.err.Code, self.err.Message
    end

    self:lease_to_expire()
    local _, err, errmes = self.impl:store(self)
    if err then
        return nil, errors.StorageError, nil
    else
        return nil
    end
end
function _meth:phase1()
    -- aka prepare

    local _, err, errmes = self:load_rec()
    if err then
        return nil, err, errmes
    end

    local _, err, errmes = self:init_view()
    if err then
        return nil, err, errmes
    end

    if not is_committed_valid( self ) then
        return nil, self.err.Code, self.err.Message
    end

    if not is_next_ver( self ) then
        return nil, self.err.Code, self.err.Message
    end

    local r = self.record.paxos_round

    if round.cmp( self.mes.rnd, r.rnd ) >= 0 then
        r.rnd = self.mes.rnd
        local _, err, errmes = self:store_or_err()
        if err then
            return nil, err, errmes
        end
    end

    local rst = {
        rnd=r.rnd,
    }
    if r.val then
        rst.val = r.val
        rst.vrnd = r.vrnd
    end

    return rst, nil, nil
end
function _meth:phase2()
    -- aka accept

    local _, err, errmes = self:load_rec()
    if err then
        return nil, err, errmes
    end

    local _, err, errmes = self:init_view()
    if err then
        return nil, err, errmes
    end

    if not is_committed_valid( self ) then
        return nil, self.err.Code, self.err.Message
    end

    if not is_next_ver( self ) then
        return nil, self.err.Code, self.err.Message
    end

    local r = self.record.paxos_round

    if round.cmp( self.mes.rnd, r.rnd ) == 0 then
        r.val = self.mes.val
        r.vrnd = self.mes.rnd

        return self:store_or_err()
    else
        return nil, errors.OldRound, nil
    end
end
function _meth:phase3()
    -- aka commit

    self:load_rec({ignore_err=true})

    if self.record.committed.ver > 0
        and not is_committed_valid(self) then
        if self.mes.ver < 1 then
            return nil, self.err.Code, self.err.Message
        else
            -- initial record, make it can store the correct committed
            self:init_rec()
        end
    end

    if not committable( self ) then
        return nil, self.err.Code, self.err.Message
    end

    -- val with higher version is allowed to commit because some proposer has
    -- confirmed that it has been accepted by a quorum.

    local rec = self.record

    if self.mes.ver > rec.committed.ver then
        rec.paxos_round = {
            rnd = round.zero(),
            vrnd = round.zero(),
        }
    end

    -- for bug tracking
    if rec.committed.__tag ~= nil
        and self.mes.__tag ~= nil
        and rec.committed.__tag ~= self.mes.__tag
        and rec.committed.ver == self.mes.ver then

        local cval = tableutil.dup( rec.committed.val, true )
        local mval = tableutil.dup( self.mes.val, true )
        local cleader = cval.leader or {}
        local mleader = mval.leader or {}
        cleader.__lease = nil
        mleader.__lease = nil

        if not tableutil.eq(cval, mval) then
            local err = errors.Conflict
            local errmes = {
                ver=self.mes.ver,
                mes_tag=self.mes.__tag,
                committed_tag=rec.committed.__tag,
                mes_val=self.mes.val,
                committed_val=rec.committed.val,
                mes_val=self.mes.val,
                committed_val=rec.committed.val
            }

            self.impl:logerr( 'conflict: ', err, errmes )

            return nil, err, errmes
        end
    end

    rec.committed = {
        ver = self.mes.ver,
        val = self.mes.val,
        __tag = self.mes.__tag,
    }

    return self:store_or_err()
end
return _M
