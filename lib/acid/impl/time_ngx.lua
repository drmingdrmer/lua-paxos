local _M = {}
local _meta = { __index=_M }

local counter = 0
local function get_incr_num()
    counter = counter + 1
    return counter
end

function _M.new(opt)
    local e = {}
    setmetatable( e, _meta )
    return e
end

function _M:sleep(n_sec)
    ngx.sleep(n_sec or 0.5)
end
function _M:time()
    return ngx.time()
end
function _M:new_rnd()
    -- round number is not required to monotonically incremental universially.
    -- incremental for each version is ok
    local ms = math.floor(ngx.now()*1000) % (86400*1000)
    -- distinguish different process
    local pid = ngx.worker.pid() % 1000
    -- distinguish different request
    local c = get_incr_num() % 1000

    return (ms*1000 + pid)*1000 + c
end

function _M:wait_run(timeout, f, ...)

    local co = ngx.thread.spawn(f, ...)

    local expire = self:time() + timeout
    while self:time() < expire do
        if coroutine.status( co ) ~= 'running' then
            return nil, nil, nil
        end
        self:sleep(1)
    end

    ngx.thread.kill( co )
    return nil, "Timeout", timeout
end

return _M
