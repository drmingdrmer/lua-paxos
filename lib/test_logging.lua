local l = require( "acid.logging" )

function test_tostr(t)
    t:eq( "1 nil", l.tostr( 1, nil ) )
    t:eq( "{1,2} {a=3,b={x=4}} nil", l.tostr( {1,2}, {a=3,b={x=4}}, nil ) )
end
