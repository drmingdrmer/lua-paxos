local member_id = { cluster_id="123.dx.GZ", ident="<partition_id>" }
local committed = {
    ver = 1,
    val = {
        view = {
            {
                ['<ident>']={index=0, ip="127.0.0.1"},
                ['<ident>']={index=1, ip="127.0.0.1"},
            },
        },
        leader = { ident="id", __lease=10 },
        action = {
            { name="bla", args={} },
            { name="foo", args={} },
        },
    }
}
