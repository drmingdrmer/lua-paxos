local nginx_cluster = require("nginx_cluster")

local _M = {}

local cc = nginx_cluster.new({
    cluster_id = 'x',
    ident = '127.0.0.1:9081',
    path = "/tmp/paxos",

    standby = {
        '127.0.0.1:9081',
        '127.0.0.1:9082',
        '127.0.0.1:9083',
        '127.0.0.1:9084',
        '127.0.0.1:9085',
        '127.0.0.1:9086',
    },
})

return _M
