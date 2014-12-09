local _M = {}
local _meta = { __index=_M }

local tableutil = require( "acid.tableutil" )
local transport = require("acid.impl.transport_ngx_http")
local storage = require("acid.impl.storage_ngx_mc")
local locking = require("acid.impl.locking_ngx")
local time = require("acid.impl.time_ngx")
local logging = require("acid.impl.logging_ngx")
local userdata = require("acid.impl.userdata")
local member = require("acid.impl.member")

tableutil.merge( _M, transport, storage, locking, time, logging, userdata, member )

function _M.new(opt)
    local e = {}
    tableutil.merge( e,
                     transport.new(opt),
                     storage.new(opt),
                     locking.new(opt),
                     time.new(opt),
                     logging.new(opt),
                     userdata.new(opt),
                     member.new(opt)
                     )
    setmetatable( e, _meta )
    return e
end

return _M
