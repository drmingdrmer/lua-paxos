local _M = { _VERSION = require("acid.paxos._ver") }

local tableutil = require( "acid.tableutil" )
local round = require( "acid.paxos.round" )

_M.errors = {
    InvalidArgument  = 'InvalidArgument',
    InvalidMessage   = 'InvalidMessage',
    InvalidCommand   = 'InvalidCommand',
    InvalidCluster   = 'InvalidCluster',
    InvalidPhase     = 'InvalidPhase',
    VerNotExist      = 'VerNotExist',
    AlreadyCommitted = 'AlreadyCommitted',
    OldRound         = 'OldRound',
    NoView           = 'NoView',
    QuorumFailure    = 'QuorumFailure',
    LockTimeout      = 'LockTimeout',
    StorageError     = 'StorageError',
    NotAccepted      = 'NotAccepted',
    DuringChange     = 'DuringChange',
    NoChange         = 'NoChange',
    Conflict         = 'Conflict',
    InvalidCommitted = 'InvalidCommitted',
}
local errors = _M.errors

function _M.init_rec( self, r )
    r = r or {}
    r.committed = r.committed or {
        ver = 0,
        val = nil,
    }

    r.paxos_round = r.paxos_round or {
        rnd = round.zero(),
        val = nil,
        vrnd = round.zero(),
    }

    self.record = r

    self:expire_to_lease()

    return nil, nil, nil
end

function _M.load_rec( self, opt )

    opt = opt or {}
    local ignore_err = opt.ignore_err == true

    local r, err, errmes = self.impl:load(self)

    if err and not ignore_err then
        return nil, err, errmes
    end

    return self:init_rec( r )
end

function _M.init_view( self )

    for _ = 0, 0 do

        local c = self.record.committed
        if c == nil then
            break
        end

        local ver, val = c.ver, c.val
        if ver == nil or val == nil then
            break
        end

        local v = val.view
        if v == nil or v[ 1 ] == nil then
            break
        end

        if self.ver ~= nil and self.ver ~= ver then
            return nil, errors.VerNotExist, ver
        end

        self.ver = ver
        self.view = v
        return nil
    end

    return nil, errors.NoView, nil
end

function _M:lease_to_expire()

    if self.record == nil then
        return nil, nil, nil
    end

    local val = self.record.committed.val
    if type(val) ~= 'table' then
        return nil, nil, nil
    end

    local now = self.impl:time()

    for k, v in pairs(val or {}) do
        if type( v ) == 'table' then
            if v.__lease ~= nil then
                v.__expire = now + v.__lease
                v.__lease = nil
            end
        end
    end
    return nil, nil, nil
end

function _M:expire_to_lease()

    if self.record == nil then
        return nil, nil, nil
    end

    local val = self.record.committed.val
    if type(val) ~= 'table' then
        return nil, nil, nil
    end

    local now = self.impl:time()

    for k, v in pairs(val or {}) do
        if type( v ) == 'table' then
            if v.__expire ~= nil then
                v.__lease = v.__expire - now
                v.__expire = nil
            end
        end
    end
end

return _M
