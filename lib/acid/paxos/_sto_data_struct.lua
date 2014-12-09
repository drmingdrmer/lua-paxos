local committed = {
    ver = 1,
    val = {
        view = {
            { a=1, b=1, c=1 },
            { b=1, c=1, d=1 },
        },
        leader = { ident="id", __lease=10 },
        action = {
            { name="bla", args={} },
            { name="foo", args={} },
        },
    }
}
