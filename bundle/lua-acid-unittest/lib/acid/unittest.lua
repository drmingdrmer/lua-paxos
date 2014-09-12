local _M = { _VERSION='0.1' }

local function tostr(x)
    return '[' .. tostring( x ) .. ']'
end

local function dd(...)
    local args = {...}
    local s = ''
    for _, mes in ipairs(args) do
        s = s .. tostring(mes)
    end
    _M.output( s )
end

function _M.output(s)
    print( s )
end

local function is_test_file( fn )
    return fn:sub( 1, 5 ) == 'test_' and fn:sub( -4, -1 ) == '.lua'
end

local function scandir(directory)
    local t = {}
    for filename in io.popen('ls "'..directory..'"'):lines() do
        table.insert( t, filename )
    end
    return t
end

local function keys(tbl)
    local n = 0
    local ks = {}
    for k, v in pairs( tbl ) do
        table.insert( ks, k )
        n = n + 1
    end
    table.sort( ks, function(a, b) return tostring(a)<tostring(b) end )
    return ks, n
end

local testfuncs = {

    ass= function (self, expr, expection, mes)
        mes = mes or ''

        local thisfile = debug.getinfo(1).short_src
        local info
        for i = 2, 10 do
            info = debug.getinfo(i)
            if info.short_src ~= thisfile then
                break
            end
        end

        local pos = 'Failure: \n'
        pos = pos .. '   in ' .. info.short_src .. '\n'
        pos = pos .. '   ' .. self._name .. '():' .. info.currentline .. '\n'

        assert( expr, pos .. '   expect ' .. expection .. ' (' .. mes .. ')' )
        self._suite.n_assert = self._suite.n_assert + 1
    end,

    eq= function( self, a, b, mes )
        self:ass( a==b, 'to be ' .. tostr(a) .. ' but is ' .. tostr(b), mes )
    end,

    neq= function( self, a, b, mes )
        self:ass( a~=b, 'not to be' .. tostr(a) .. ' but the same: ' .. tostr(b), mes )
    end,

    err= function ( self, func, mes )
        local ok, rst = pcall( func )
        self:eq( false, ok, mes )
    end,

    eqlist= function( self, a, b, mes )
        self:neq( nil, a, "left list is not nil" )
        self:neq( nil, b, "right list is not nil" )

        for i, e in ipairs(a) do
            self:ass( e==b[i], i .. 'th elt to be ' .. tostr(e) .. ' but is ' .. tostr(b[i]), mes )
        end
        -- check if b has more elements
        for i, e in ipairs(b) do
            self:ass( nil~=a[i], i .. 'th elt to be nil but is ' .. tostr(e), mes )
        end
    end,

    eqdict= function( self, a, b, mes )
        mes = mes or ''

        if a == b then
            return
        end

        self:neq( nil, a, "left table is not nil" .. mes )
        self:neq( nil, b, "right table is not nil" .. mes )
        local akeys, an = keys( a )
        local bkeys, bn = keys( b )

        for _, k in ipairs( akeys ) do
            self:ass( b[k] ~= nil, '["' .. k .. '"] in right but not. '.. mes )
        end
        for _, k in ipairs( bkeys ) do
            self:ass( a[k] ~= nil, '["' .. k .. '"] in left but not. '.. mes )
        end
        for _, k in ipairs( akeys ) do
            local av, bv = a[k], b[k]
            if type( av ) == 'table' and type( bv ) == 'table' then
                self:eqdict( av, bv, k .. '<' .. mes )
            else
                self:ass( a[k] == b[k],
                '["' .. k .. '"] to be ' .. tostr(a[k]) .. ' but is ' .. tostr(b[k]), mes )
            end
        end

    end,

    contain= function( self, a, b, mes )
        self:neq( nil, a, "left table is not nil" )
        self:neq( nil, b, "right table is not nil" )

        for k, e in pairs(a) do
            self:ass( e==b[k], '["' .. k .. '"] to be ' .. tostr(e) .. ' but is ' .. tostr(b[k]), mes )
        end
    end,

}

local _mt = { __index= testfuncs }

local function find_tests(tbl)
    local tests = {}
    for k, v in pairs(tbl) do
        if k:sub( 1, 5 ) == 'test_' and type( v ) == 'function' then
            tests[ k ] = v
        end
    end
    return tests
end

function _M.test_one( suite, name, func )

    dd( "* testing ", name, ' ...' )

    local tfuncs = {}
    setmetatable( tfuncs, _mt )
    tfuncs._name = name
    tfuncs._suite = suite

    local co = coroutine.create( func )
    local ok, rst = coroutine.resume( co, tfuncs )

    if not ok then
        dd( rst )
        dd( debug.traceback(co) )
        os.exit(1)
    end
    suite.n = suite.n + 1
end

function _M.testall( suite )

    local names = {}

    local tests = find_tests( _G )
    for k, v in pairs(tests) do
        table.insert( names, { k, v } )
    end

    table.sort( names, function(x, y) return x[ 1 ]<y[ 1 ] end  )

    for _, t in ipairs( names ) do
        local funcname, func = t[ 1 ], t[ 2 ]
        _M.test_one( suite, funcname, func )
    end
end

function _M.testdir( dir )

    package.path = package.path .. ';'..dir..'/?.lua'

    local suite = { n=0, n_assert=0 }
    local fns = scandir( dir )

    for _, fn in ipairs(fns) do

        if is_test_file( fn ) then

            dd( "---- ", fn, ' ----' )

            local tests0 = find_tests( _G )
            require( fn:sub( 1, -5 ) )
            local tests1 = find_tests( _G )

            _M.testall( suite )

            for k, v in pairs(tests1) do
                if tests0[ k ] == nil then
                    _G[ k ] = nil
                end
            end
        end
    end
    dd( suite.n, ' tests all passed. nr of assert: ', suite.n_assert )
    return true
end

function _M.t()
    if arg == nil then
        -- lua -l unittest
        _M.testdir( '.' )
        os.exit()
    else
        -- require( "unittest" )
    end
end
_M.t()

return _M
