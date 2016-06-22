local r = require( "acid.paxos.round" )

function test_new(t)
    t:err( function () r.new( { 0 } ) end )
    t:err( function () r.new( { 1 } ) end )
    t:err( function () r.new( { a=1 } ) end )
    t:err( function () r.new( { 1, 2, 3 } ) end )

    local a = r.new( { 1, '' } )
    t:eq( 1, a[ 1 ] )
    t:eq( '', a[ 2 ] )
end

function test_zero(t)
    local a = r.zero()
    t:eqlist( { 0, '' }, a, 'zero' )
end

function test_max(t)

    t:eqlist( r.zero(), r.max( {} ), 'max of empty' )

    t:eqlist( { 5, 'x' }, r.max( {{ 5, 'x' }} ), 'max of 1' )

    local three = { { 0, 'x' }, { 2, 'a' }, { 1, 'b' } }
    local a = r.max( three )
    t:eqlist( { 2, 'a' }, a, 'max of 3' )

    a[ 1 ] = 10
    t:eq( 10, three[ 2 ][ 1 ], 'max uses reference' )

end

function test_incr(t)
    local a = r.zero()
    a[ 2 ] = 'xxx'

    local b = r.incr( a )

    t:eqlist( { 0, 'xxx' }, a )
    t:eqlist( { 1, 'xxx' }, b )
end

function test_cmp(t)
    t:eq( 0, r.cmp( nil, nil ) )
    t:eq( 1, r.cmp( r.zero(), nil ) )
    t:eq( -1, r.cmp( nil, r.zero() ) )

    t:eq( 1, r.cmp( { 1, 'b' }, { 0 } ) )
    t:eq( 1, r.cmp( { 1 }, { 0, 'a' } ) )

    t:eq( 0, r.cmp( r.zero(), r.zero() ) )
    t:eq( 1, r.cmp( r.incr(r.zero()), r.zero() ) )
    t:eq( -1, r.cmp( { 1, 'b' }, { 2, 'a' } ) )
    t:eq( 1, r.cmp( { 1, 'b' }, { 1, 'a' } ) )
    t:eq( 1, r.cmp( { 2, 'a' }, { 1, 'b' } ) )
    t:eq( -1, r.cmp( { 1, 'a' }, { 1, 'b' } ) )
end
