local tableutil = require("acid.tableutil")
local strutil = require("acid.strutil")

local tb_eq = tableutil.eq
local to_str = strutil.to_str

function test_nkeys(t)
    local cases = {
        {0, {}, 'nkeys of empty'},
        {1, {0}, 'nkeys of 1'},
        {2, {0, nil, 1}, 'nkeys of 0, nil and 1'},
        {2, {0, 1}, 'nkeys of 2'},
        {2, {0, 1, nil}, 'nkeys of 0, 1 and nil'},
        {1, {a=0, nil}, 'nkeys of a=1'},
        {2, {a=0, b=2, nil}, 'nkeys of a=1'},
    }

    for i, case in ipairs(cases) do
        local n, tbl, mes = case[1], case[2], case[3]
        local rst = tableutil.nkeys(tbl)
        t:eq(n, rst, 'nkeys:' .. mes)

        rst = tableutil.get_len(tbl)
        t:eq(n, rst, 'get_len:' .. mes)
    end
end

function test_keys(t)
    t:eqdict( {}, tableutil.keys({}) )
    t:eqdict( {1}, tableutil.keys({1}) )
    t:eqdict( {1, 'a'}, tableutil.keys({1, a=1}) )
    t:eqdict( {1, 2, 'a'}, tableutil.keys({1, 3, a=1}) )
end

function test_duplist(t)
    local du = tableutil.duplist

    local a = { 1 }
    local b = tableutil.duplist( a )
    a[ 2 ] = 2
    t:eq( nil, b[ 2 ], "dup not affected" )

    t:eqdict( {1}, du( { 1, nil, 2 } ) )
    t:eqdict( {1}, du( { 1, a=3 } ) )
    t:eqdict( {1}, du( { 1, [3]=3 } ) )
    t:eqdict( {}, du( { a=1, [3]=3 } ) )

    local a = { { 1, 2, 3, a=4 } }
    a[2] = a[1]
    local b = du(a)
    t:eqdict({ { 1, 2, 3, a=4 }, { 1, 2, 3, a=4 } }, b)
    t:eq( b[1], b[2] )
end

function test_sub(t)
    local a = { a=1, b=2, c={} }
    t:eqdict( {}, tableutil.sub( a, nil ) )
    t:eqdict( {}, tableutil.sub( a, {} ) )
    t:eqdict( {b=2}, tableutil.sub( a, {"b"} ) )
    t:eqdict( {a=1, b=2}, tableutil.sub( a, {"a", "b"} ) )

    local b = tableutil.sub( a, {"a", "b", "c"} )
    t:neq( b, a )
    t:eq( b.c, a.c, "reference" )

    -- sub list

    local cases = {
        {{1, 2, 3}, {}, {}},
        {{1, 2, 3}, {2, 3}, {2, 3}},
        {{1, 2, 3}, {2, 3, 4}, {2, 3}},
        {{1, 2, 3}, {3, 4, 2}, {3, 2}},
    }

    for i, case in ipairs(cases) do
        local rst = tableutil.sub(tbl, ks, list=true)
        t:eqdict(expected, rst, to_str(case))
    end
end

function test_dup(t)
    local a = { a=1, 10, x={ y={z=3} } }
    a.self = a
    a.selfref = a
    a.x2 = a.x

    local b = tableutil.dup( a )
    b.a = 'b'
    t:eq( 1, a.a, 'dup not affected' )
    t:eq( 10, a[ 1 ], 'a has 10' )
    t:eq( 10, b[ 1 ], 'b inherit 10' )
    b[ 1 ] = 11
    t:eq( 10, a[ 1 ], 'a has still 10' )

    a.x.y.z = 4
    t:eq( 4, b.x.y.z, 'no deep' )

    local deep = tableutil.dup( a, true )
    a.x.y.z = 5
    t:eq( 4, deep.x.y.z, 'deep dup' )
    t:eq( deep, deep.self, 'loop reference' )
    t:eq( deep, deep.selfref, 'loop reference should be dup only once' )
    t:eq( deep.x, deep.x2, 'dup only once' )
    t:neq( a.x, deep.x, 'dup-ed x' )
    t:eq( deep.x.y, deep.x2.y )

end

function test_contains(t)
    local c = tableutil.contains
    t:eq( true, c( nil, nil ) )
    t:eq( true, c( 1, 1 ) )
    t:eq( true, c( "", "" ) )
    t:eq( true, c( "a", "a" ) )

    t:eq( false, c( 1, 2 ) )
    t:eq( false, c( 1, nil ) )
    t:eq( false, c( nil, 1 ) )
    t:eq( false, c( {}, 1 ) )
    t:eq( false, c( {}, "" ) )
    t:eq( false, c( "", {} ) )
    t:eq( false, c( 1, {} ) )

    t:eq( true, c( {}, {} ) )
    t:eq( true, c( {1}, {} ) )
    t:eq( true, c( {1}, {1} ) )
    t:eq( true, c( {1, 2}, {1} ) )
    t:eq( true, c( {1, 2}, {1, 2} ) )
    t:eq( true, c( {1, 2, a=3}, {1, 2} ) )
    t:eq( true, c( {1, 2, a=3}, {1, 2, a=3} ) )

    t:eq( false, c( {1, 2, a=3}, {1, 2, b=3} ) )
    t:eq( false, c( {1, 2 }, {1, 2, b=3} ) )
    t:eq( false, c( {1}, {1, 2, b=3} ) )
    t:eq( false, c( {}, {1, 2, b=3} ) )

    t:eq( true, c( {1, 2, a={ x=1 }}, {1, 2} ) )
    t:eq( true, c( {1, 2, a={ x=1, y=2 }}, {1, 2, a={}} ) )
    t:eq( true, c( {1, 2, a={ x=1, y=2 }}, {1, 2, a={x=1}} ) )
    t:eq( true, c( {1, 2, a={ x=1, y=2 }}, {1, 2, a={x=1, y=2}} ) )

    t:eq( false, c( {1, 2, a={ x=1 }}, {1, 2, a={x=1, y=2}} ) )

    -- self reference
    local a = { x=1 }
    local b = { x=1 }

    a.self = { x=1 }
    b.self = {}
    t:eq( true, c( a, b ) )
    t:eq( false, c( b, a ) )

    a.self = a
    b.self = nil
    t:eq( true, c( a, b ) )
    t:eq( false, c( b, a ) )

    a.self = a
    b.self = b
    t:eq( true, c( a, b ) )
    t:eq( true, c( b, a ) )

    a.self = { self=a }
    b.self = nil
    t:eq( true, c( a, b ) )
    t:eq( false, c( b, a ) )

    a.self = { self=a, x=1 }
    b.self = b
    t:eq( true, c( a, b ) )

    a.self = { self={ self=a, x=1 }, x=1 }
    b.self = { self=b }
    t:eq( true, c( a, b ) )

    -- cross reference
    a.self = { x=1 }
    b.self = { x=1 }
    a.self.self = b
    b.self.self = a
    t:eq( true, c( a, b ) )
    t:eq( true, c( b, a ) )

end

function test_eq(t)
    local c = tableutil.eq
    t:eq( true, c( nil, nil ) )
    t:eq( true, c( 1, 1 ) )
    t:eq( true, c( "", "" ) )
    t:eq( true, c( "a", "a" ) )

    t:eq( false, c( 1, 2 ) )
    t:eq( false, c( 1, nil ) )
    t:eq( false, c( nil, 1 ) )
    t:eq( false, c( {}, 1 ) )
    t:eq( false, c( {}, "" ) )
    t:eq( false, c( "", {} ) )
    t:eq( false, c( 1, {} ) )

    t:eq( true, c( {}, {} ) )
    t:eq( true, c( {1}, {1} ) )
    t:eq( true, c( {1, 2}, {1, 2} ) )
    t:eq( true, c( {1, 2, a=3}, {1, 2, a=3} ) )

    t:eq( false, c( {1, 2}, {1} ) )
    t:eq( false, c( {1, 2, a=3}, {1, 2} ) )

    t:eq( false, c( {1, 2, a=3}, {1, 2, b=3} ) )
    t:eq( false, c( {1, 2 }, {1, 2, b=3} ) )
    t:eq( false, c( {1}, {1, 2, b=3} ) )
    t:eq( false, c( {}, {1, 2, b=3} ) )

    t:eq( true, c( {1, 2, a={ x=1, y=2 }}, {1, 2, a={x=1, y=2}} ) )

    t:eq( false, c( {1, 2, a={ x=1 }}, {1, 2, a={x=1, y=2}} ) )

    -- self reference
    local a = { x=1 }
    local b = { x=1 }

    a.self = { x=1 }
    b.self = {}
    t:eq( false, c( a, b ) )

    a.self = { x=1 }
    b.self = { x=1 }
    t:eq( true, c( a, b ) )

    a.self = a
    b.self = nil
    t:eq( false, c( b, a ) )

    a.self = a
    b.self = b
    t:eq( true, c( a, b ) )
    t:eq( true, c( b, a ) )

    a.self = { self=a }
    b.self = nil
    t:eq( false, c( a, b ) )

    a.self = { self=a, x=1 }
    b.self = b
    t:eq( true, c( a, b ) )

    a.self = { self={ self=a, x=1 }, x=1 }
    b.self = { self=b, x=1 }
    t:eq( true, c( a, b ) )

    -- cross reference
    a.self = { x=1 }
    b.self = { x=1 }
    a.self.self = b
    b.self.self = a
    t:eq( true, c( a, b ) )
    t:eq( true, c( b, a ) )

end

function test_intersection(t)
    local a = { a=1, 10 }
    local b = { 11, 12 }
    local c = tableutil.intersection( { a, b }, true )

    t:eq( 1, tableutil.nkeys( c ), 'c has 1' )
    t:eq( true, c[ 1 ] )

    local d = tableutil.intersection( { a, { a=20 } }, true )
    t:eq( 1, tableutil.nkeys( d ) )
    t:eq( true, d.a, 'intersection a' )

    local e = tableutil.intersection( { { a=1, b=2, c=3, d=4 }, { b=2, c=3 }, { b=2, d=5 } }, true )
    t:eq( 1, tableutil.nkeys( e ) )
    t:eq( true, e.b, 'intersection of 3' )

end

function test_union(t)
    local a = tableutil.union( { { a=1, b=2, c=3 }, { a=1, d=4 } }, 0 )
    t:eqdict( { a=0, b=0, c=0, d=0 }, a )
end

function test_mergedict(t)
    t:eqdict( { a=1, b=2, c=3 }, tableutil.merge( { a=1, b=2, c=3 } ) )
    t:eqdict( { a=1, b=2, c=3 }, tableutil.merge( {}, { a=1, b=2 }, { c=3 } ) )
    t:eqdict( { a=1, b=2, c=3 }, tableutil.merge( { a=1 }, { b=2 }, { c=3 } ) )
    t:eqdict( { a=1, b=2, c=3 }, tableutil.merge( { a=0 }, { a=1, b=2 }, { c=3 } ) )

    local a = { a=1 }
    local b = { b=2 }
    local c = tableutil.merge( a, b )
    t:eq( true, a==c )
    a.x = 10
    t:eq( 10, c.x )
end

function test_repr(t)
    local r = tableutil.repr
    local s1 = { sep=' ' }
    local s2 = { sep='  ' }

    t:eq( '1', r( 1 ) )
    t:eq( '"1"', r( '1' ) )
    t:eq( 'nil', r( nil ) )
    t:eq( '{}', r( {} ) )
    t:eq( '{}', r( {}, s1 ) )
    t:eq( '{ 1 }', r( { 1 }, s1 ) )
    t:eq( '{ 1, 2 }', r( { 1, 2 }, s1 ) )
    t:eq( '{ a=1 }', r( { a=1 }, s1 ) )
    t:eq( '{ 0, a=1, b=2 }', r( { 0, a=1, b=2 }, s1 ) )
    t:eq( '{  0,  a=1,  b=2  }', r( { 0, a=1, b=2 }, s2 ) )

    local literal=[[{
    1,
    2,
    3,
    {
        1,
        2,
        3,
        4
    },
    [100]=33333,
    a=1,
    c=100000,
    d=1,
    ["fjklY*("]={
        b=3,
        x=1
    },
    x={
        1,
        {
            1,
            2
        },
        y={
            a=1,
            b=2
        }
    }
}]]
    local a = {
        1, 2, 3,
        { 1, 2, 3, 4 },
        a=1,
        c=100000,
        d=1,
        x={
            1,
            { 1, 2 },
            y={
                a=1,
                b=2
            }
        },
        ['fjklY*(']={
            x=1,
            b=3,
        },
        [100]=33333
    }
    t:eq( literal, r(a, { indent='    ' }) )


end

function test_str(t)
    local r = tableutil.str
    local s1 = { sep=' ' }
    local s2 = { sep='  ' }

    t:eq( '1', r( 1 ) )
    t:eq( '1', r( '1' ) )
    t:eq( 'nil', r( nil ) )
    t:eq( '{}', r( {} ) )
    t:eq( '{}', r( {}, s1 ) )
    t:eq( '{ 1 }', r( { 1 }, s1 ) )
    t:eq( '{ 1, 2 }', r( { 1, 2 }, s1 ) )
    t:eq( '{ a=1 }', r( { a=1 }, s1 ) )
    t:eq( '{ 0, a=1, b=2 }', r( { 0, a=1, b=2 }, s1 ) )
    t:eq( '{  0,  a=1,  b=2  }', r( { 0, a=1, b=2 }, s2 ) )
    t:eq( '{0,a=1,b=2}', r( { 0, a=1, b=2 } ) )

    local literal=[[{
    1,
    2,
    3,
    {
        1,
        2,
        3,
        4
    },
    100=33333,
    a=1,
    c=100000,
    d=1,
    fjklY*(={
        b=3,
        x=1
    },
    x={
        1,
        {
            1,
            2
        },
        y={
            a=1,
            b=2
        }
    }
}]]
    local a = {
        1, 2, 3,
        { 1, 2, 3, 4 },
        a=1,
        c=100000,
        d=1,
        x={
            1,
            { 1, 2 },
            y={
                a=1,
                b=2
            }
        },
        ['fjklY*(']={
            x=1,
            b=3,
        },
        [100]=33333
    }
    t:eq( literal, r(a, { indent='    ' }) )


end

function test_iter(t)

    for ks, v in tableutil.deep_iter({}) do
        t:err( "should not get any keys" )
    end

    for ks, v in tableutil.deep_iter({1}) do
        t:eqdict( {1}, ks )
        t:eq( 1, v )
    end

    for ks, v in tableutil.deep_iter({a="x"}) do
        t:eqdict( {{"a"}, "x"}, {ks, v} )
    end

    local a = {
        1, 2, 3,
        { 1, 2, 3, 4 },
        a=1,
        c=100000,
        d=1,
        x={
            1,
            { 1, 2 },
            y={
                a=1,
                b=2
            }
        },
        ['fjklY*(']={
            x=1,
            b=3,
        },
        [100]=33333
    }
    a.z = a.x

    local r = {
        { {1}, 1 },
        { {100}, 33333 },
        { {2}, 2 },
        { {3}, 3 },
        { {4,1}, 1 },
        { {4,2}, 2 },
        { {4,3}, 3 },
        { {4,4}, 4 },
        { {"a"}, 1 },
        { {"c"}, 100000 },
        { {"d"}, 1 },
        { {"fjklY*(","b"}, 3 },
        { {"fjklY*(","x"}, 1 },
        { {"x",1}, 1 },
        { {"x",2,1}, 1 },
        { {"x",2,2}, 2 },
        { {"x","y","a"}, 1 },
        { {"x","y","b"}, 2 },
        { {"z",1}, 1 },
        { {"z",2,1}, 1 },
        { {"z",2,2}, 2 },
        { {"z","y","a"}, 1 },
        { {"z","y","b"}, 2 },
    }

    local i = 0
    for ks, v in tableutil.deep_iter(a) do
        i = i + 1
        t:eqdict( r[i], {ks, v} )
    end

end


function test_has(t)
    local cases = {
        {nil, {}, true},
        {1, {1}, true},
        {1, {1, 2}, true},
        {1, {1, 2, 'x'}, true},
        {'x', {1, 2, 'x'}, true},

        {1, {x=1}, true},

        {'x', {x=1}, false},
        {"x", {1}, false},
        {"x", {1, 2}, false},
        {1, {}, false},
    }

    for i, case in ipairs(cases) do
        local val, tbl, expected = case[1], case[2], case[3]
        t:eq(expected, tableutil.has(tbl, val), i .. 'th case: ' .. to_str(val, tbl))
    end
end


function test_remove(t)
    local t1 = {}
    local cases = {
        {{},                nil, {},                nil},
        {{1, 2, 3},         2,   {1, 3},            2},
        {{1, 2, 3, x=4},    2,   {1, 3, x=4},       2},
        {{1, 2, 3},         3,   {1, 2},            3},
        {{1, 2, 3, x=4},    3,   {1, 2, x=4},       3},
        {{1, 2, 3, x=4},    4,   {1, 2, 3},         4},
        {{1, 2, 3, x=t1},   t1,  {1, 2, 3},         t1},

        {{1, 2, t1, x=t1}, t1,   {1, 2, x=t1}, t1},
    }
    
    for i, case in ipairs(cases) do

        local tbl, val, expected_tbl, expected_rst = case[1], case[2], case[3], case[4]

        local rst = tableutil.remove(tbl, val)

        t:eqdict(expected_tbl, tbl, i .. 'th tbl')
        t:eq(expected_rst, rst, i .. 'th rst')
    end
end



function test_extends(t)

    local cases = {

        {{1,2},     {3,4},       {1,2,3,4}},
        {{1,2},     {3},         {1,2,3}},
        {{1,2},     {nil},       {1,2}},
        {{1,2},     {},          {1,2}},
        {{},        {1,2},       {1,2}},
        {nil,       {1},         nil},
        {{1,{2,3}}, {4,5},       {1,{2,3},4,5}},
        {{1},       {{2,3},4,5}, {1,{2,3},4,5}},
        {{"xx",2},  {3,"yy"},    {"xx",2,3,"yy"}},
        {{1,2},     {3,nil,4},   {1,2,3}},
        {{1,nil,2}, {3,4},       {1,nil,2,3,4}},

    }

    for _, c in ipairs( cases ) do

        local exp   = c[3]
        local actul = tableutil.extends(c[1], c[2])

        local msg = "expect: " .. to_str( exp ) ..
                   ", actul: " .. to_str( actul )

        t:eqlist( actul, exp, msg )
    end
end
