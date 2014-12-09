local _M = {}
local _meta = { __index=_M }

function _M.new(opt)
    local e = {}
    setmetatable( e, _meta )
    return e
end

function _M:restore(paxos, member) end
function _M:destory(paxos, _) end
function _M:is_data_valid(paxos, member) return true end

return _M
