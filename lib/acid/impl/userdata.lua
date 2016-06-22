local _M = {}
local _meta = { __index=_M }

function _M.new(opt)
    local e = {}
    setmetatable( e, _meta )
    return e
end

function _M:restore(paxos, member) end
function _M:destory(paxos, _) end
function _M:is_member_valid(paxos, member, opts) return true end
function _M:is_needed_migrate(paxos, member) return false end
function _M:report_cluster(paxos, down_members) end

return _M
