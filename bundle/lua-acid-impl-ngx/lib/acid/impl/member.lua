local _M = {_VERSION="0.1"}
local _meta = { __index=_M }

function _M.new(opt)
    local e = {}
    setmetatable( e, _meta )
    return e
end

function _M:new_member(paxos, dead_ident, members)
    return nil, "NotImplemented"
end

return _M
