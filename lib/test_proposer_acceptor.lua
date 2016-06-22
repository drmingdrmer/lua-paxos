local base = require( "acid.paxos.base" )
local paxos = require( "acid.paxos" )
local round = require( "acid.paxos.round" )
local tableutil = require( "acid.tableutil" )

local logging = require( "acid.logging" )

local errors = base.errors

local prop_sto = {
    committed= {
        ver=1,
        val = {
            view={
                { a=1, b=1, c=1 },
                { c=1, d=1, e=1 },
            },
        },
    },
}

local mes_receiver = {}

local prop_args = {
    cluster_id = 'cluster_a',
    ident='1',
}
local prop_impl = {
    load= function( self, pp )
        return tableutil.dup(prop_sto, true)
    end,
    time= function( self ) return os.time() end,
    send_req= function( self, pp, id, mes )
        mes_receiver[ id ] = mes
        return 'resp'
    end,
}

function test_new(t)

    t:err( function() paxos.proposer.new( nil ) end, 'nil' )
    t:err( function() paxos.proposer.new( {} ) end, 'empty table' )

    local x, err = paxos.proposer.new( prop_args, prop_impl )
    t:contain( prop_args, x )
    t:eq( prop_impl, x.impl )
    t:eqdict( prop_sto.committed.val.view, x.view, 'load view' )
    t:eqdict( { a=true, b=true, c=true, d=true, e=true }, x.acceptors, 'acceptors' )
    t:eqlist( { 0, prop_args.ident }, x.rnd, 'initial rnd' )

end
function test_new_with_ver(t)

    local args = tableutil.dup( prop_args )
    args.ver = 1
    local x, err = paxos.proposer.new( args, prop_impl )
    t:eq( nil, err )
    t:eq( args.ver, x.ver )

    args.ver = 2
    local x, err = paxos.proposer.new( args, prop_impl )
    t:eq( errors.VerNotExist, err )
end

function test_new_with_roundmaker(t)
    local impl = tableutil.dup( prop_impl )
    impl.new_rnd = function (self) return 123 end
    local prop, err = paxos.proposer.new( { cluster_id="1", ident="2" }, impl )
    t:eq( nil, err )
    t:eq( 0, round.cmp( prop.rnd, { 123, "2" } ) )
end

function test_new_no_view(t)

    local sto = {}
    local args = {
        ident='a',
    }
    local impl = {
        load=function( key ) return sto end,
        time= function( self ) return os.time() end,
    }

    local cases = {
        { committed=nil, rst=nil, err=errors.NoView },
        { committed={ ver=1, val=nil }, rst=nil, err=errors.NoView },
        { committed={ ver=1, val={view={}} }, rst=nil, err=errors.NoView },
        { committed={ ver=1, val={view={{}}} }, rst={}, err=nil },
    }

    for i, case in ipairs( cases ) do
        sto.committed = case.committed

        local x, err = paxos.proposer.new( args, impl )
        if case.rst then
            t:neq( nil, x, i..' committed: ' .. tableutil.repr( case.committed ) )
        else
            t:eq( nil, x, i..' committed: ' .. tableutil.repr( case.committed ) )
        end
        t:eq( case.err, err )
    end

    sto.committed = { ver=1, val={ view={ { a=1 } } } }
    local x, err = paxos.proposer.new( args, impl )
    t:neq( nil, x )
    t:eq( nil, err )
    t:neq( 0, tableutil.nkeys( x.acceptors ) )

end

function test_sendmes(t)
    local prop = paxos.proposer.new( prop_args, prop_impl )
    mes_receiver = {}

    local resp = prop.impl:send_req( prop, 'theid', 1 )
    t:eq( 1, mes_receiver.theid, 'receive mes' )
    t:eq( 'resp', resp, 'got resp from send_req' )
end

function test_send_mes_all(t)
    local x = paxos.proposer.new( prop_args, prop_impl )
    mes_receiver = {}

    local rs = x:send_mes_all(2)
    t:eqdict( { a=2, b=2, c=2, d=2, e=2 }, mes_receiver, 'all got message' )
    t:eqdict( {
        a='resp',
        b='resp',
        c='resp',
        d='resp',
        e='resp',
    }, rs )
end

function test_phase1(t)
    local x = paxos.proposer.new( prop_args, prop_impl )
    mes_receiver = {}

    local rs = x:phase1()
    t:eqdict( {
        a='resp',
        b='resp',
        c='resp',
        d='resp',
        e='resp',
    }, rs )

    t:eqdict( { a=2, b=2, c=2, d=2, e=2 },
            tableutil.intersection( { mes_receiver }, 2 ), 'got 5 mes' )

    for k, v in pairs(mes_receiver) do
        t:eq( 'phase1', v.cmd )
        t:eq( prop_args.cluster_id, v.cluster_id )
        t:eqlist( { 0, x.ident }, v.rnd )
    end
end

function test_phase2(t)
    local x = paxos.proposer.new( prop_args, prop_impl )
    mes_receiver = {}

    local rs = x:phase2('val')
    t:eqdict( {
        a='resp',
        b='resp',
        c='resp',
        d='resp',
        e='resp',
    }, rs )

    t:eqdict( { a=2, b=2, c=2, d=2, e=2 },
            tableutil.intersection( { mes_receiver }, 2 ), 'got 5 mes' )

    for k, v in pairs(mes_receiver) do
        t:eq( 'phase2', v.cmd )
        t:eq( prop_args.cluster_id, v.cluster_id )
        t:eqlist( { 0, x.ident }, v.rnd )
        t:eq( 'val', v.val )
    end
end

function test_phase3(t)
    local x = paxos.proposer.new( prop_args, prop_impl )
    mes_receiver = {}

    -- TODO test run decide twice

    local rs, err = x:commit()
    t:eq( nil, rs )
    t:eq( errors.InvalidPhase, err, 'no accepted_val cause err' )

    x.stat = {}
    x.stat.accepted_val = 'accepted_val'
    x.ver = 100
    local c, err = x:commit()
    t:eq( nil, err )
    -- t:eqdict( {
    --     a='resp',
    --     b='resp',
    --     c='resp',
    --     d='resp',
    --     e='resp',
    -- }, rs )

    t:eqdict( { a=2, b=2, c=2, d=2, e=2 },
            tableutil.intersection( { mes_receiver }, 2 ), 'got 5 mes' )

    for k, v in pairs(mes_receiver) do
        t:eq( 'phase3', v.cmd )
        t:eq( prop_args.cluster_id, v.cluster_id )
        t:eq( 100+1, v.ver )
        t:eq( 'accepted_val', v.val )
    end
end

function test_decide_twice(t)
    local x = paxos.proposer.new( prop_args, prop_impl )
    mes_receiver = {}

    local _, err = x:decide(0)
    local _, err = x:decide(2)
    t:eq( errors.InvalidPhase, err )
end

function test_choose_p1(t)

    local x = paxos.proposer.new( prop_args, prop_impl )

    local accepted, v, stat = x:choose_p1( {}, 1 )
    t:eqdict( {}, accepted )
    t:eqdict( x.rnd, stat.latest_rnd )
    t:eq( 1, v )


    -- val exists
    local resps = {
        a={ rnd=round.new({ 5, 'x' }), val='a', vrnd=round.new({ 2, 'c' }) },
        b={ rnd=x.rnd },
        c={ rnd=x.rnd, val='c', vrnd=round.new({ 1, 'a' }) },
    }

    local accepted, v, stat = x:choose_p1( resps, 10 )
    t:eqdict( {b=2, c=2}, tableutil.union({accepted}, 2) )
    t:eqdict( {5, 'x'}, stat.latest_rnd )
    t:eq( 'a', v )

    -- choose my val
    local resps = {
        a={ rnd=round.new({ 5, 'x' }) },
        b={ rnd=x.rnd },
        c={ rnd=x.rnd },
    }

    local accepted, v, stat = x:choose_p1( resps, 10 )
    t:eqdict( {b=2, c=2}, tableutil.union({accepted}, 2) )
    t:eqdict( {5, 'x'}, stat.latest_rnd )
    t:eq( 10, v )

    -- committed
    local resps = {
        a={ rnd=round.new({ 5, 'x' }) },
        b={ err={ Code=errors.AlreadyCommitted, Message={ ver=3, val='committed-3' } } },
        c={ err={ Code=errors.AlreadyCommitted, Message={ ver=2, val='committed-2' } } },
    }

    local accepted, v, stat = x:choose_p1( resps, 10 )
    t:eqdict( {}, tableutil.union({accepted}, true) )
    t:eqdict( { 5, 'x' }, stat.latest_rnd )
    t:eq( 10, v )
    t:eqdict( {ver=3, val='committed-3'}, stat.latest_committed )


    -- stale_vers
    local resps = {
        a={ rnd=round.new({ 5, 'x' }) },
        b={ err={ Code=errors.VerNotExist, Message=1 } },
        c={ err={ Code=errors.VerNotExist, Message=0 } },
    }

    x.ver = 2
    local accepted, v, stat = x:choose_p1( resps, 10 )
    t:eqdict( {}, tableutil.union({accepted}, true) )
    t:eqdict( {c=0, b=1}, stat.stale_vers )
end

function test_choose_p2(t)

    local x = paxos.proposer.new( prop_args, prop_impl )

    local accepted = x:choose_p2( {} )
    t:eqdict( {}, accepted )

    local resps = {
        a={err={}},
        b={},
        c={},
    }

    local accepted = x:choose_p2( resps )
    t:eqdict( {b=2, c=2}, tableutil.union({accepted}, 2) )

end

function test_is_quorum(t)
    local x = paxos.proposer.new( prop_args, prop_impl )

    x.view = {
        { a=1, b=1, c=1 },
    }

    t:eq( false, x:is_quorum( {} ) )
    t:eq( false, x:is_quorum( {a=1} ) )
    t:eq( false, x:is_quorum( {c=1} ) )

    t:eq( true, x:is_quorum( {a=1, b=1} ) )
    t:eq( true, x:is_quorum( {a=1, b=1, c=1} ) )

    t:eq( false, x:is_quorum( {a=1, x=1, y=1} ) )

    -- dual group
    x.view = {
        { a=1, b=1, c=1 },
        { c=1, d=1, e=1 },
    }

    t:eq( true, x:is_quorum( { a=nil, b=1, c=1, d=1 } ) )
    t:eq( true, x:is_quorum( { a=1, b=1, c=1, d=1 } ) )
    t:eq( true, x:is_quorum( { a=1, b=1, c=nil, d=1, e=1 } ) )

    t:eq( false, x:is_quorum( { a=1, b=nil, c=nil, d=1, e=1 } ) )
    t:eq( false, x:is_quorum( { a=nil, b=nil, c=1, d=1, e=1 } ) )
end

function test_decide(t)

    local resps
    local args
    local impl
    local myval = 10
    local none = {}

    function handler( self, p, id, mes )
        local r = resps[ id ] or {}
        if mes.cmd == 'phase1' then
            r = r.p1 or { rnd=mes.rnd }
            if r == none then
                return nil
            end
        elseif mes.cmd == 'phase2' then
            r = r.p2 or {}
            if r == none then
                return nil
            end
        else
            error( "invalid cmd " .. tableutil.repr( mes ) )
        end
        return r
    end

    args = {
        ident='a',
    }
    impl = {
        send_req=handler,
        load=function( key )
            return { committed={
                ver=1,
                val={ view={ { a=1, b=1, c=1 } } }
            }}
        end,
        time= function( self ) return os.time() end,
    }

    cases = {
        {
            mes = 'all accept',
            statchange = {
                accepted_val = 10,
            },
            rst = { ok=true, val=myval, err=nil },
            resps = {
                a={ p1={ rnd={ 1, 'a' }, vrnd=nil, val=nil }, p2={}, },
                b={ p1={ rnd={ 1, 'a' }, vrnd=nil, val=nil }, p2={}, },
                c={ p1={ rnd={ 1, 'a' }, vrnd=nil, val=nil }, p2={}, },
            },
        },
        {
            mes = 'old round',
            rst = { ok=false, val=nil, err=errors.OldRound },
            resps = {
                a={ p1={ rnd={ 1, 'a' } } },
                b={ p1={ rnd={ 2, 'b' } } },
            },
        },
        {
            mes = 'only 1 on phase 1',
            rst = { ok=false, val=nil, err=errors.QuorumFailure },
            resps = {
                a={ p1=nil },
                b={ p1=none },
                c={ p1=none },
            },
        },
        {
            mes = 'quorum failure',
            rst = { ok=true, val=myval, err=nil },
            resps = {
                a={ p1=nil },
                b={ p1=nil },
                c={ p1=none, p2=none },
            },
        },
        {
            mes = 'quorum failure on phase 2',
            rst = { ok=false, val=nil, err=errors.QuorumFailure },
            resps = {
                a={ p1=nil, p2=none },
                b={ p1=nil },
                c={ p1=none, p2=none },
            },
        },
        {
            mes = 'failure on phase 2',
            rst = { ok=false, val=nil, err=errors.QuorumFailure },
            resps = {
                a={ p1=nil, p2={ err={Code="xx"} } },
                b={ p1=nil },
                c={ p1=none, p2=none },
            },
        },
        {
            mes = 'existent seen',
            rst = { ok=false, val=2, err=nil },
            resps = {
                b={ p1={ rnd={ 0, 'b' }, vrnd={ 0, 'b' }, val=2 }, p2={}, },
            },
        },
        {
            mes = 'existent seen, newest one',
            rst = { ok=false, val=2, err=nil },
            resps = {
                a={ p1={ rnd={ 1, 'a' }, vrnd={ 0, 'a' }, val=1 }, p2={}, },
                b={ p1={ rnd={ 0, 'b' }, vrnd={ 0, 'b' }, val=2 }, p2={}, },
            },
        },
        {
            mes = 'committed',
            statchange = {
                latest_committed = { ver=4, val="committed-4" },
            },
            rst = { ok=true, val=10, err=nil },
            resps = {
                c={
                    p1={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                    p2={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                },
            },
        },
        {
            mes = 'stale_vers, latest_rnd, latest_committed',
            statchange = {
                latest_committed = { ver=4, val="committed-4" },
                stale_vers = { b=0 },
                latest_rnd = { 100, 'x' },
            },
            rst = { ok=false, val=nil, err=errors.OldRound },
            resps = {
                a={
                    p1={
                        rnd = { 100, 'x' },
                    },
                },
                b={
                    p1={
                        err={
                            Code=errors.VerNotExist,
                            Message=0,
                        },
                    },
                },
                c={
                    p1={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                    p2={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                },
            },
        },
    }

    for i, case in ipairs( cases ) do
        mes = i .. ": " .. (case.mes or '')
        resps = case.resps

        local x, err = paxos.proposer.new( args, impl )
        t:eq( nil, err, mes )

        local val, err = x:decide( myval )
        local rst = case.rst
        t:eq( rst.val, val, mes )
        t:eq( rst.err, err, mes )
        if case.statchange then
            for k, v in pairs(case.statchange) do
                t:eqdict( v, x.stat[ k ], mes )
            end
        end
    end

end

function test_write(t)

    local resps
    local args
    local impl
    local myval = 10
    local none = {}

    function handler( self, p, id, mes )
        local r = resps[ id ] or {}
        if mes.cmd == 'phase1' then
            r = r.p1 or { rnd=mes.rnd }
            if r == none then
                return nil
            end
        elseif mes.cmd == 'phase2' then
            r = r.p2 or {}
            if r == none then
                return nil
            end
        elseif mes.cmd == 'phase3' then
            r = r.p3 or {}
            if r == none then
                return nil
            end
        else
            error( "invalid cmd" .. tableutil.repr( mes ) )
        end
        return r
    end

    args = {
        ident='a',
    }
    impl = {
        send_req=handler,
        load=function( key )
            return { committed={
                ver=1,
                val={ view={ { a=1, b=1, c=1 } } }
            }}
        end,
        time= function( self ) return os.time() end,
    }

    cases = {
        {
            mes = 'all accept',
            statchange = {
                accepted_val = 10,
            },
            rst = { err=nil },
            resps = {
                a={ p1=nil, p2={}, p3=nil},
                b={ p1=nil, p2={}, p3=nil},
                c={ p1=nil, p2={}, p3=nil},
            },
        },
        {
            mes = 'old round',
            rst = { err=errors.OldRound },
            resps = {
                a={ p1={ rnd={ 1, 'a' } } },
                b={ p1={ rnd={ 2, 'b' } } },
            },
        },
        {
            mes = 'quorum failure on phase 1',
            rst = { err=errors.QuorumFailure },
            resps = {
                a={ p1=nil },
                b={ p1=none },
                c={ p1=none },
            },
        },
        {
            mes = 'quorum failure on phase 2',
            rst = { err=errors.QuorumFailure },
            resps = {
                a={ p1=none, p2=nil },
                b={ p1=nil, p2=none },
                c={ p1=nil, p2=none },
            },
        },
        {
            mes = 'quorum failure on phase 3',
            rst = { err=errors.QuorumFailure },
            resps = {
                a={ p1=none, p2=nil, p3=none },
                b={ p1=nil, p2=nil, p3=nil },
                c={ p1=nil, p2=none, p3=none },
            },
        },
        {
            mes = 'failure on phase 2',
            rst = { err=errors.QuorumFailure },
            resps = {
                a={ p1=nil, p2={ err={Code="xx"} } },
                b={ p1=nil },
                c={ p1=none, p2=none },
            },
        },
        {
            mes = 'existent seen',
            rst = { val=2, err=errors.VerNotExist },
            resps = {
                b={ p1={ rnd={ 0, 'b' }, vrnd={ 0, 'b' }, val=2 }, },
            },
        },
        {
            mes = 'existent seen, newest one',
            rst = { val=2, err=errors.VerNotExist },
            resps = {
                a={ p1={ rnd={ 1, 'a' }, vrnd={ 0, 'a' }, val=1 }, },
                b={ p1={ rnd={ 0, 'b' }, vrnd={ 0, 'b' }, val=2 }, },
            },
        },
        {
            mes = 'committed',
            statchange = {
                latest_committed = { ver=4, val="committed-4" },
            },
            rst = { val=10, err=nil },
            resps = {
                c={
                    p1={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                    p2={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                },
            },
        },
        {
            mes = 'stale_vers, latest_rnd, latest_committed',
            statchange = {
                latest_committed = { ver=4, val="committed-4" },
                stale_vers = { b=0 },
                latest_rnd = { 100, 'x' },
            },
            rst = { val=nil, err=errors.OldRound },
            resps = {
                a={
                    p1={
                        rnd = { 100, 'x' },
                    },
                },
                b={
                    p1={
                        err={
                            Code=errors.VerNotExist,
                            Message=0,
                        },
                    },
                },
                c={
                    p1={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                    p2={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                },
            },
        },
    }

    for i, case in ipairs( cases ) do
        mes = i .. ": " .. (case.mes or '')
        resps = case.resps

        local x, err = paxos.proposer.new( args, impl )
        t:eq( nil, err, mes )

        local c, err = x:write( myval )
        local rst = case.rst
        t:eq( rst.err, err, mes )
        if err == nil then
            if rst.val ~= nil then
                t:eq( rst.val, c.val, mes )
            end
        end

        if case.statchange then
            for k, v in pairs(case.statchange) do
                t:eqdict( v, x.stat[ k ], mes )
            end
        end
    end

end
function test_remote_read(t)

    local resps
    local args
    local impl
    local myval = 10
    local none = {}

    function _sendmes( self, p, id, mes )
        local r = resps[ id ] or {}
        if mes.cmd == 'phase3' then
            r = r.p3 or {}
            if r == none then
                return nil
            end
        else
            error( "invalid cmd" .. tableutil.repr( mes ) )
        end
        return r
    end

    args = {
        ident='a',
    }
    impl = {
        send_req=_sendmes,
        load=function( key )
            return { committed={
                ver=1,
                val={ view={ { a=1, b=1, c=1 } } }
            }}
        end,
        time= function( self ) return os.time() end,
    }

    cases = {
        {
            mes = 'nothing responded',
            rst = nil,
            err = errors.VerNotExist,
            resps = {
                a={ p3=none},
                b={ p3=none},
                c={ p3=none},
            },
        },
        {
            mes = 'other error',
            rst = nil,
            err = errors.VerNotExist,
            resps = {
                a={ p3=none},
                b={ p3=none},
                c={ p3={err={ Code=errors.StorageError }}},
            },
        },
        {
            mes = 'one seen',
            rst = { ver=1, val=2 },
            resps = {
                a={ p3=none},
                b={ p3=none},
                c={ p3={err={ Code=errors.AlreadyCommitted, Message={ ver=1, val=2 } }}},
            },
        },
        {
            mes = 'one seen, need quorum',
            need_quorum = true,
            rst = nil,
            err = errors.QuorumFailure,
            resps = {
                a={ p3=none},
                b={ p3=none},
                c={ p3={err={ Code=errors.AlreadyCommitted, Message={ ver=1, val=2 } }}},
            },
        },
        {
            mes = 'two seen, need quorum',
            need_quorum = true,
            rst = { ver=3, val=4 },
            err = nil,
            resps = {
                a={ p3=none},
                b={ p3={err={ Code=errors.AlreadyCommitted, Message={ ver=3, val=4 } }}},
                c={ p3={err={ Code=errors.AlreadyCommitted, Message={ ver=1, val=2 } }}},
            },
        },
        {
            mes = 'one seen with other error',
            rst = { ver=1, val=2 },
            resps = {
                a={ p3=none},
                b={ p3={err={ Code=errors.StorageError }}},
                c={ p3={err={ Code=errors.AlreadyCommitted, Message={ ver=1, val=2 } }}},
            },
        },
        {
            mes = 'committed',
            rst = { ver=5, val="committed-5" },
            resps = {
                a={ p3=none },
                b={
                    p3={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=5, val="committed-5" },
                        },
                    },
                },
                c={
                    p3={
                        err={
                            Code=errors.AlreadyCommitted,
                            Message={ ver=4, val="committed-4" },
                        },
                    },
                },
            },
        },
    }

    for i, case in ipairs( cases ) do
        mes = i .. ": " .. (case.mes or '')
        resps = case.resps

        local x, err = paxos.proposer.new( args, impl )
        t:eq( nil, err, mes )

        local c, err
        if case.need_quorum then
            c, err = x:quorum_read()
        else
            c, err = x:remote_read()
        end
        if case.err then
            t:eq( nil, c, mes )
            t:eqdict( case.err, err, mes )
        else
            t:eqdict( case.rst, c, mes )
            t:eq( nil, err, mes )
        end
    end

end

local acc_view = { ver=1, val={ { a=1 } } }
local acc_store = {
    kempty = {},
    kp1 = {
        committed = {
            ver=2,
            val= {
                view={ {a=1} },
            }
        },
        paxos_round = { rnd={ 2, 'b' }, vrnd=round.zero() },
    },
    kp2 = {
        committed = {
            ver=2,
            val= {
                view={ {a=1} },
            }
        },
        paxos_round = { rnd={ 2, 'b' }, vrnd={ 2, 'b' }, val='val-b' },
    },
    kcmt = {
        committed = {
            ver=2,
            val= {
                view={ {a=1} },
                val = 'val-3'
            }
        },
    },
    kcmtp1 = {
        committed = {
            ver=2,
            val= {
                view={ {a=1} },
                val = 'val-c'
            }
        },
        paxos_round={ rnd={ 3, 'c' }, vrnd=round.zero() }
    },

}
local acc_reflect = {r=nil}

local function default_acc(t, sto)

    local acc_args = {
        cluster_id='cl',
        ident='a',
    }
    local lock_ph = {}
    local acc_impl = {
        store=function( self, p )
            t:eq( acc_args.cluster_id, p.cluster_id )
            t:eq( acc_args.ident, p.ident )
            acc_reflect.r = p.record
        end,
        load=function( self, p )
            t:eq( acc_args.cluster_id, p.cluster_id )
            t:eq( acc_args.ident, p.ident )
            return tableutil.dup( sto or {}, true )
        end,
        time= function( self ) return os.time() end,
        lock=function(self, p)
            t:eq( acc_args.cluster_id, p.cluster_id )
            t:eq( acc_args.ident, p.ident )
            return lock_ph
        end,
        unlock=function(self, l)
            t:eq( lock_ph, l, "lock object is sent back to unlock" )
        end,
    }

    local acc, err = paxos.acceptor.new( acc_args, acc_impl )
    t:eq( nil, err )
    return acc
end

function test_new_acc(t)
    local view = { { a=1, b=1 }, { b=1, c=1 } }
    local args = {
        cluster_id = 'cl',
        ident = 'a',
    }

    local impl = {
        store=function( self, pp ) end,
        load=function( self, pp )
            return {
                committed={ ver=1, val={
                    foo={ __expire=os.time()-1 },
                    view=tableutil.dup(view, true)
                } }
            }
        end,
        time= function( self ) return os.time() end,
        lock=function(self, key) return nil end,
        unlock=function(self, key) end,
    }

    t:err( function() paxos.acceptor.new( nil ) end, 'nil' )

    local x, err = paxos.acceptor.new( args, impl )
    t:eq( nil, err )
    t:contain( args, x )

    x:load_rec()
    t:eq( -1, x.record.committed.val.foo.__lease )
end

function test_new_acc_with_ver(t)
    local view = { { a=1, b=1 }, { b=1, c=1 }, foo={ __expire=os.time()-1 } }
    local args = {
        cluster_id = 'cl',
        ident = 'a',
        ver = 2,
    }

    local impl = {
        store=function( self, pp ) end,
        load=function( self, pp )
            return {
                committed={ ver=1, val={view=tableutil.dup(view, true)} }
            }
        end,
        time= function( self ) return os.time() end,
        lock=function(self, key) return nil end,
        unlock=function(self, key) end,
    }

    local x, err = paxos.acceptor.new( args, impl )
    t:eq( nil, err )
    t:eq( args.ver, x.ver )

    local _, err, errmes = x:load_rec()
    t:eq(nil, err)

    local _, err, errmes = x:init_view()
    t:eq( errors.VerNotExist, err )
end

function test_new_acc_args(t)
    local view = { { a=1, b=1 }, { b=1, c=1 } }
    local args = {
        cluster_id = 'cl',
        ident = 'a',
    }
    local impl = {
        store=nil,
        load=function( key )
            return {
                committed={ ver=1, val=view, }
            }
        end,
        time= function( self ) return os.time() end,
        lock=function(self, key) return nil end,
        unlock=function(self, key) end,
    }
    for _, key in ipairs({ 'cluster_id', 'ident', 'load', 'lock', 'store' }) do
        local a = tableutil.dup( args )
        local e = tableutil.dup( impl )
        a[ key ] = nil
        e[ key ] = nil

        local x, err = paxos.acceptor.new( a, e )
        t:eq( nil, x, 'without ' .. key )
        t:neq( nil, err, 'without ' .. key )
        t:eq( errors.InvalidArgument, err, 'without ' .. key )
    end

end

function test_acc_view_err(t)

    local sto = {}

    local args = {
        cluster_id='cl',
        ident='a',
    }

    local impl = {
        store=function( key ) end,
        load=function( key ) return sto end,
        time= function( self ) return os.time() end,
        lock=function(self, key) return nil end,
        unlock=function(self, key) end,
    }

    local cases = {
        { committed=nil, err=errors.NoView },
        { committed={ ver=2, val=nil }, err=errors.NoView },
        { committed={ ver=2, val={} }, err=errors.NoView },
        { committed={ ver=2, val={view={{}}} }, err=errors.AlreadyCommitted },
    }

    for i, case in ipairs( cases ) do
        sto.committed = case.committed

        local x, err = paxos.acceptor.new( args, impl )

        for _, cmd in ipairs({ 'phase1', 'phase2' }) do

            local req = {
                cmd = cmd,
                cluster_id = "cl",
                ver = 2,
            }
            local r, err, errmes = x:process( req )
            local mes = i .. ': req=' .. tableutil.repr( req ) .. '\n resp=' .. tableutil.repr( r )
            t:eq( case.err, err, mes )
        end
    end
end

function test_acc_process(t)
    local x = default_acc(t)

    local r, err, errmes = x:process( nil )
    t:eq( errors.InvalidMessage, err )

    r, err, errmes = x:process( {} )
    t:eq( errors.InvalidMessage, err )

    r, err, errmes = x:process( {cluster_id=1, ident=1, cmd=1} )
    t:eq( errors.InvalidCommand, err )
end

function test_acc_not_my_mes(t)

    local x = default_acc(t)

    local mes = {
        rnd={ 1, 'a' },
        cluster_id = 'cl',
    }

    local cases = {
        { rst={ nil, errors.InvalidCluster, 'cl' },  mes={cluster_id='x'}, },
    }

    for i, case in ipairs(cases) do
        local mes = tableutil.dup( mes )
        tableutil.merge( mes, case.mes )

        for _, phase in ipairs({ 'phase1', 'phase2', 'phase3' }) do
            mes.cmd = phase

            local r, err, errmes = x:process( mes )

            local m = i .. ' req=' .. tableutil.repr( mes ) .. ' resp=' .. tableutil.repr( {r, err, errmes} )
            t:eqdict( case.rst, {r, err, errmes}, m )
        end
    end

end

function test_acc_lock(t)
    local acc_args = {
        cluster_id='cl',
        ident='a',
    }
    local impl = {
        store=function( self )
            acc_reflect.r = self.record
        end,
        load=function( self )
            return tableutil.dup( sto or {}, true )
        end,
        time= function( self ) return os.time() end,
        lock=function(self, key) return nil, "lockerr" end,
        unlock=function(self, key) end,
        logerr=function() end,
    }
    local reqbase = {
        cmd = 'phase1',
        cluster_id = 'cl',
        ver = 1,
    }

    local acc, err = paxos.acceptor.new( acc_args, impl )
    t:eq( nil, err )

    local req = { rnd={0, 'x'}, ver=0 }
    tableutil.merge( req, reqbase )

    acc_reflect = {}
    local r, err, errmes  = acc:process(req)
    t:eq(nil, r)
    t:eqdict(errors.LockTimeout, err)

end
function test_acc_phase1(t)
    local committed = { val=nil, ver=0 }
    local req = {
        cmd = 'phase1',
        cluster_id = 'cl',
        ver = 1,
    }

    local cases = {
        {
            sto = acc_store.kempty,
            req = { rnd={ 0, 'x' }, ver=0, },
            rst = { nil, errors.NoView },
            stored = nil
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 0, 'x' }, ver=0, },
            rst = { nil, errors.AlreadyCommitted, acc_store.kp1.committed },
            stored = nil
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 0, 'x' }, ver=1, },
            rst = { nil, errors.AlreadyCommitted, acc_store.kp1.committed },
            stored = nil
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 0, 'x' }, ver=2, },
            rst = { nil, errors.AlreadyCommitted, acc_store.kp1.committed },
            stored = nil
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 0, 'x' }, ver=3, },
            rst = { {rnd={ 2, 'b' }}, },
            stored = nil,
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 3, 'x' }, ver=3 },
            rst = { {rnd={ 3, 'x' }} },
            stored = {
                committed = acc_store.kp1.committed,
                paxos_round = { rnd={ 3, 'x' }, vrnd=round.zero() },
            },
        },
        {
            sto = acc_store.kp2,
            req = { rnd={ 1, 'a' }, ver=3 },
            rst = { {rnd={ 2, 'b' }, vrnd={ 2, 'b' }, val='val-b'} },
            stored = nil,
        },
        {
            sto = acc_store.kp2,
            req = { rnd={ 3, 'a' }, ver=3 },
            rst = { {rnd={ 3, 'a' }, vrnd={ 2, 'b' }, val='val-b'} },
            stored = {
                committed = acc_store.kp2.committed,
                paxos_round = { rnd={ 3, 'a' }, vrnd={ 2, 'b' }, val='val-b' },
            },
        },
        {
            sto = acc_store.kcmt,
            req = { rnd={ 1, 'a' }, ver=1 },
            rst = { nil, errors.AlreadyCommitted, acc_store.kcmt.committed },
            stored = nil,
        },
        {
            sto = acc_store.kcmt,
            req = { rnd={ 1, 'a' }, ver=2, },
            rst = { nil, errors.AlreadyCommitted, acc_store.kcmt.committed },
            stored = nil,
        },
        {
            sto = acc_store.kcmt,
            req = { rnd={ 1, 'a' }, ver=3, },
            rst = { {rnd={ 1, 'a' }} },
            stored = {
                committed = acc_store.kcmt.committed,
                paxos_round = { rnd={ 1, 'a' }, vrnd=round.zero() },
            },
        },
    }

    for i, case in ipairs(cases) do
        local x = default_acc( t, case.sto )
        local req = tableutil.dup( req )
        tableutil.merge( req, case.req )

        acc_reflect = {}
        local r, err, errmes = x:process( req )

        local mes = i..": " .. tableutil.repr( { rnd=req.rnd, resp=r, stored=acc_reflect.r } )
        t:eqdict( case.rst, {r, err, errmes}, mes )
        t:eqdict( case.stored, acc_reflect.r, mes )
    end
end
function test_acc_phase2(t)
    local req = {
        cmd = 'phase2',
        cluster_id = 'cl',
        ver = 1,
        val = 'myval',
    }
    local committed = { val=nil, ver=0 }
    local pr = {
        rnd = round.zero(),
        val = nil,
        vrnd = round.zero(),
    }

    local cases = {
        {
            sto = acc_store.kp1,
            req = { rnd={ 0, 'x' }, ver=0 },
            rst = { nil, errors.AlreadyCommitted, acc_store.kp1.committed },
            stored = nil,
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 0, 'x' }, ver=1 },
            rst = { nil, errors.AlreadyCommitted, acc_store.kp1.committed },
            stored = nil,
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 0, 'x' }, ver=2 },
            rst = { nil, errors.AlreadyCommitted, acc_store.kp1.committed },
            stored = nil,
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 1, 'a' }, ver=3 },
            rst = { nil, errors.OldRound },
            stored = nil,
        },
        {
            sto = acc_store.kp1,
            req = { rnd={ 2, 'b' }, ver=3, },
            rst = {nil, nil, nil},
            stored = {
                committed = acc_store.kp1.committed,
                paxos_round = {
                    rnd = { 2, 'b' },
                    vrnd = { 2, 'b' },
                    val = 'myval',
                }
            }
        },
        {
            mes = "__lease not affected in phase2",
            sto = acc_store.kp1,
            req = { rnd={ 2, 'b' }, ver=3, val={ foo={ __lease=10, } } },
            rst = {nil, nil, nil},
            stored = {
                committed = acc_store.kp1.committed,
                paxos_round = {
                    rnd = { 2, 'b' },
                    vrnd = { 2, 'b' },
                    val = { foo={ __lease=10 } },
                }
            }
        },
        {
            sto = acc_store.kcmt,
            req = { rnd={ 1, 'a' }, ver=2, },
            rst = { nil, errors.AlreadyCommitted, acc_store.kcmt.committed },
            stored = nil,
        },
        {
            sto = acc_store.kcmt,
            req = { rnd={ 1, 'a' }, ver=3, },
            rst = { nil, errors.OldRound },
            stored = nil,
        },
    }

    for i, case in ipairs(cases) do

        local x = default_acc( t, case.sto )

        local req = tableutil.dup( req )
        tableutil.merge( req, case.req )

        acc_reflect = {}
        local r, err, errmes = x:process( req )

        local mes = i..": " .. tableutil.repr( { rnd=req.rnd, resp=r, stored=acc_reflect.r } )

        t:eqdict( case.rst, {r, err, errmes}, mes )
        t:eqdict( case.stored, acc_reflect.r, mes )
    end
end
function test_acc_phase3(t)
    local req = {
        cmd = 'phase3',
        cluster_id = 'cl',
        ver = 1,
        val = 'myval',
    }
    local committed = { val=nil, ver=0 }
    local empty_pr = {
        rnd = round.zero(),
        val = nil,
        vrnd = round.zero(),
    }

    local cases = {
        {
            mes = "ver=0 is not allowed to commit",
            sto = acc_store.kempty,
            req = { ver=0, },
            rst = { nil, errors.AlreadyCommitted, { ver=0 } },
            stored = nil,
        },
        {
            mes = "empty storage is allowed to commit",
            sto = acc_store.kempty,
            req = { ver=1, },
            rst = {},
            stored = {
                committed = { ver=1, val='myval' },
                paxos_round = empty_pr,
            }
        },
        {
            mes = "empty storage is allowed to commit ver=2",
            sto = acc_store.kempty,
            req = { ver=2, },
            rst = {},
            stored = {
                committed = { ver=2, val='myval' },
                paxos_round = empty_pr,
            }
        },
        {
            mes = 'commmit ver=0 to existent ver=2',
            sto = acc_store.kp1,
            req = { ver=0, },
            rst = { nil, errors.AlreadyCommitted, acc_store.kp1.committed },
            stored = nil,
        },
        {
            mes = 'commmit ver=1 to existent ver=2',
            sto = acc_store.kp1,
            req = { ver=1, },
            rst = { nil, errors.AlreadyCommitted, acc_store.kp1.committed },
            stored = nil,
        },
        {
            mes = 'commmit ver=2 to existent ver=2, paxos_round will not be clear',
            sto = acc_store.kp1,
            req = { ver=2, },
            rst = {},
            stored = {
                committed = { ver=2, val='myval' },
                paxos_round = { rnd={ 2, 'b' }, vrnd=round.zero() },
            }
        },
        {
            mes = 'commmit ver=3 to existent ver=2, paxos_round will be cleared',
            sto = acc_store.kp1,
            req = { ver=3, },
            rst = {},
            stored = {
                committed = { ver=3, val='myval' },
                paxos_round = empty_pr,
            }
        },
        {
            mes = '__lease should be converted to __expire',
            sto = acc_store.kp1,
            req = { ver=3, val={ foo={ __lease=10 } } },
            rst = {},
            stored = {
                committed = { ver=3, val={ foo={ __expire=os.time()+10 } } },
                paxos_round = empty_pr,
            }
        },
        {
            mes = 'commmit ver=100 to existent ver=2, paxos_round will be cleared',
            sto = acc_store.kp1,
            req = { ver=100, },
            rst = {},
            stored = {
                committed = { ver=100, val='myval' },
                paxos_round = empty_pr,
            }
        },
        {
            mes = 'commmit ver=2 to existent ver=2, with accepted val',
            sto = acc_store.kp2,
            req = { ver=2, },
            rst = {},
            stored = {
                committed = { ver=2, val='myval' },
                paxos_round = acc_store.kp2.paxos_round,
            }
        },
    }

    for i, case in ipairs(cases) do
        local x = default_acc( t, case.sto )

        local req = tableutil.dup( req )
        tableutil.merge( req, case.req )

        acc_reflect = {}
        local r, err, errmes = x:process( req )

        local mes = i..': '..(case.mes or '') .. ' req=' .. tableutil.repr( req ) .. ' resp='..tableutil.repr( r )
        t:eqdict( case.rst, {r, err, errmes}, mes )
        t:eqdict( case.stored, acc_reflect.r, mes )
    end
end
