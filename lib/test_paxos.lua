
local base = require( "acid.paxos.base" )
local paxos = require( "acid.paxos" )
local tableutil = require( "acid.tableutil" )

local errors = base.errors

function test_new(t)
    local cases = {
        {
            mid=nil,
            impl=nil,
            rst=nil,
            err=errors.InvalidArgument,
        },
        {
            mid={ cluster_id=nil, ident=nil },
            impl=nil,
            rst=nil,
            err=errors.InvalidArgument,
        },
        {
            mid={ cluster_id="x", ident=nil },
            impl=nil,
            rst=nil,
            err=errors.InvalidArgument,
        },
        {
            mid={ cluster_id=nil, ident="x" },
            impl=nil,
            rst=nil,
            err=errors.InvalidArgument,
        },
        {
            mid={ cluster_id="x", ident="x" },
            impl=nil,
            rst=nil,
            err=nil,
        },

    }
    for i, case in ipairs( cases ) do
        local mes = "" .. i .. (case.mes or "")
        local p, err, errmes = paxos.new(case.mid, case.impl)
        if case.err then
            t:eq( nil, p )
            t:eq( case.err, err )
        else
            t:eq( nil, err )
        end
    end
end

local function make_implementation( opt )

    local resps = tableutil.dup( opt.resps, true ) or {}
    local stores = tableutil.dup( opt.stores, true ) or {}
    local def_sto = tableutil.dup( opt.def_sto, true ) or {}

    function _sendmes( self, p, id, mes )

        local r = resps[ id ] or {}

        if mes.cmd == 'phase1' then
            r = r.p1
            if r == nil then r = { rnd=mes.rnd } end
        elseif mes.cmd == 'phase2' then
            r = r.p2
            if r == nil then r = {} end
        elseif mes.cmd == 'phase3' then
            r = r.p3
            if r == nil then r = {} end
        else
            error( "invalid cmd" .. tableutil.repr( mes ) )
        end

        if r == false then
            return nil
        end
        return r
    end

    local impl = {
        send_req = _sendmes,
        load = function( p )
            local id = p.ident
            local c = stores[ id ] or def_sto
            return tableutil.dup( c, true )
        end,
        store = function( self, p )
            local id = p.ident
            stores[ id ] = tableutil.dup( p.record, true )
        end,
        time= function( self ) return os.time() end,
    }
    return impl
end

function test_set(t)

    local def_sto = {
        committed = {
            ver=1,
            val = {
                view = { { a=1, b=1, c=1 } },
            }
        }
    }
    cases = {
        {
            mes = 'set ok',
            key="akey", val="aval", ver=nil,
            rst = { ver=2, key="akey", val="aval" },
            err = nil,
            p_ver=2,
            resps = {
                a={},
                b={},
                c={},
            },
            def_sto = def_sto,
        },
        {
            mes = 'set with specific ver',
            key="akey", val="aval", ver=1,
            rst = { ver=2, key="akey", val="aval" },
            err = nil,
            p_ver=2,
            resps = {
                a={},
                b={},
                c={},
            },
            def_sto = def_sto,
        },
        {
            mes = 'set with unsatisfied ver',
            key="akey", val="aval", ver=3,
            rst = nil,
            err = 'VerNotExist',
            p_ver=3,
            resps = {
                a={},
                b={},
                c={},
            },
            def_sto = def_sto,
        },
        {
            mes = 'set with quorum failure',
            key="akey", val="aval", ver=1,
            rst = nil,
            err = 'QuorumFailure',
            p_ver=1,
            resps = {
                a={p1=false},
                b={p1=false},
                c={},
            },
            def_sto = def_sto,
        },
    }

    for i, case in ipairs( cases ) do

        mes = i .. ": " .. (case.mes or '')
        resps = case.resps

        local impl = make_implementation({
            resps = case.resps,
            stores = case.stores,
            def_sto = case.def_sto
        })

        local mid = { cluster_id="x", ident="a" }

        local p, err = paxos.new( mid, impl, case.ver )
        t:eq( nil, err, mes )

        local c, err, errmes = p:set( case.key, case.val )
        t:eq( case.err, err, mes )
        t:eqdict( case.rst, c, mes )

        t:eq( case.p_ver, p.ver, mes )
    end

end
function test_get(t)

    local def_sto = {
        committed = {
            ver=1,
            val = {
                foo = "bar",
                view = { { a=1, b=1, c=1 } },
            }
        }
    }
    cases = {
        {
            mes = 'get failure 3',
            key="akey", ver=nil,
            rst = nil,
            err = errors.QuorumFailure,
            resps = {
                a={p3=false},
                b={p3=false},
                c={p3=false},
            },
            def_sto = def_sto,
        },
        {
            mes = 'get failure 2',
            key="akey", ver=nil,
            rst = nil,
            err = errors.QuorumFailure,
            resps = {
                a={p3={err={Code=errors.AlreadyCommitted, Message=def_sto.committed}}},
                b={p3=false},
                c={p3=false},
            },
            def_sto = def_sto,
        },
        {
            mes = 'get not found',
            key="akey", ver=nil,
            rst = {ver=1, key="akey", val=nil},
            err = nil,
            resps = {
                a={p3={err={Code=errors.AlreadyCommitted, Message=def_sto.committed}}},
                b={p3={err={Code=errors.AlreadyCommitted, Message=def_sto.committed}}},
                c={p3=false},
            },
            def_sto = def_sto,
        },
        {
            mes = 'get found',
            key="foo", ver=nil,
            rst = {ver=1, key="foo", val="bar"},
            err = nil,
            resps = {
                a={p3={err={Code=errors.AlreadyCommitted, Message=def_sto.committed}}},
                b={p3={err={Code=errors.AlreadyCommitted, Message=def_sto.committed}}},
                c={p3=false},
            },
            def_sto = def_sto,
        },
        {
            mes = 'get found latest',
            key="foo", ver=nil,
            rst = {ver=2, key="foo", val="bar2"},
            err = nil,
            resps = {
                a={p3={err={Code=errors.AlreadyCommitted, Message=def_sto.committed}}},
                b={p3={err={Code=errors.AlreadyCommitted, Message={
                    ver=2,
                    val = {
                        foo = "bar2",
                        view = { { a=1, b=1, c=1 } },
                    }
                }}}},
                c={p3=false},
            },
            def_sto = def_sto,
        },
        {
            mes = 'set with unsatisfied ver',
            key="akey", ver=3,
            rst = nil,
            err = 'VerNotExist',
            resps = {
                a={},
                b={},
                c={},
            },
            def_sto = def_sto,
        },
    }

    for i, case in ipairs( cases ) do

        mes = i .. ": " .. (case.mes or '')
        resps = case.resps

        local impl = make_implementation({
            resps = case.resps,
            stores = case.stores,
            def_sto = case.def_sto
        })

        local mid = { cluster_id="x", ident="a" }

        local p, err = paxos.new( mid, impl, case.ver )
        t:eq( nil, err, mes )

        local c, err, errmes = p:get( case.key )
        t:eq( case.err, err, mes )
        t:eqdict( case.rst, c, mes )
    end

end
function test_sendmes(t)

    local def_sto = {
        committed = {
            ver=1,
            val = {
                foo = "bar",
                view = { { a=1, b=1, c=1 } },
            }
        }
    }
    cases = {
        {
            mes = 'send get nothing back',
            key="akey", ver=nil,
            req = {cmd="phase3"},
            to_id = 'a',
            rst = nil,
            err = nil,
            resps = {
                a={p3=false},
                b={p3=false},
                c={p3=false},
            },
            def_sto = def_sto,
        },
        {
            mes = 'send get something',
            key="akey", ver=nil,
            req = {cmd="phase3"},
            to_id = 'a',
            rst = {a=1},
            err = nil,
            resps = {
                a={p3={a=1}},
                b={p3={b=2}},
                c={p3=false},
            },
            def_sto = def_sto,
        },
    }

    for i, case in ipairs( cases ) do

        mes = i .. ": " .. (case.mes or '')
        resps = case.resps

        local impl = make_implementation({
            resps = case.resps,
            stores = case.stores,
            def_sto = case.def_sto
        })

        local mid = { cluster_id="x", ident="a" }

        local p, err = paxos.new( mid, impl, case.ver )
        t:eq( nil, err, mes )

        local c, err, errmes = p:send_req( case.to_id, case.req )
        t:eq( case.err, err, mes )
        t:eqdict( case.rst, c, mes )
    end

end
