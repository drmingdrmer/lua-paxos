local strutil = require("acid.strutil")


function test_split(t)

    local str ='/v1/get/video.vic.sina.com.cn%2fmt788%2f9e%2f1a%2f81403.jpg/%7b%22xACL%22%3a%20%7b%22GRPS000000ANONYMOUSE%22%3a%20%5b%22read%22%5d, %20%22SINA00000000000SALES%22%3a%20%5b%22read%22, %20%22write%22, %20%22read_acp%22, %20%22write_acp%22%5d%7d, %20%22Info%22%3a%20null, %20%22Type%22%3a%20%22image%5c%2fjpeg%22, %20%22ver%22%3a%201042410872, %20%22Get-Location%22%3a%20%5b%7b%22CheckNumber%22%3a%201042410872, %20%22GroupID%22%3a%20341476, %20%22Partitions%22%3a%20%5b%7b%22IPs%22%3a%20%5b%2258.63.236.89%22, %20%2210.71.5.89%22%5d, %20%22PartitionID%22%3a%20%22185c3e5700014004975f90b11c13fc5e%22, %20%22IDC%22%3a%20%22.dx.GZ%22%7d, %20%7b%22IPs%22%3a%20%5b%2258.63.236.184%22, %20%2210.71.5.184%22%5d, %20%22PartitionID%22%3a%20%225a56155500014009aaa590b11c148e88%22, %20%22IDC%22%3a%20%22.dx.GZ%22%7d, %20%7b%22IPs%22%3a%20%5b%2260.28.228.36%22, %20%22172.16.228.36%22%5d, %20%22PartitionID%22%3a%20%22a41d4006000140029595d4ae52b17fe1%22, %20%22IDC%22%3a%20%22.wt.TJ%22%7d, %20%7b%22IPs%22%3a%20%5b%22111.161.78.59%22, %20%22172.16.48.59%22%5d, %20%22PartitionID%22%3a%20%22d7fee54d00014006b58090b11c145321%22, %20%22IDC%22%3a%20%22.wt.TJ%22%7d%5d%7d%5d, %20%22Info-Int%22%3a%200, %20%22ts%22%3a%201356544141, %20%22ACL%22%3a%20%7b%22SINA00000000000SALES%22%3a%20%5b%22read%22, %20%22write%22, %20%22read_acp%22, %20%22write_acp%22%5d%7d, %20%22ETag2%22%3a%20%2235b4ec0bfd826ea609054ccca4976e4fc77f3a8b%22, %20%22ETag%22%3a%20%22e14526f8858e2e0f898e72f141f108e4%22, %20%22Key%22%3a%20%22mt788%5c%2f9e%5c%2f1a%5c%2f81403.jpg%22, %20%22Owner%22%3a%20%22SINA00000000000SALES%22, %20%22Origo%22%3a%20%220000000000000000000090b11c09b4d9%22, %20%22GroupClassID%22%3a%206, %20%22File-Meta%22%3a%20%7b%22Content-Type%22%3a%20%22image%5c%2fjpeg%22%7d, %20%22Size%22%3a%2095964%7d?n=1&r=1&w=1&expire=60&ver_key=ts'

    t:eqdict( { '' }, strutil.split( '', '/' ), 'empty string' )
    t:eqdict( { '', '' }, strutil.split( '/', '/' ), 'single pattern' )
    t:eqdict( { '', '', '' }, strutil.split( '//', '/' ), 'dual pattern' )
    t:eqdict( { 'a', '', '' }, strutil.split( 'a//', '/' ), '"a" and dual pattern' )
    t:eqdict( { '', 'a', '' }, strutil.split( '/a/', '/' ), '/a/' )
    t:eqdict( { '', '', 'a' }, strutil.split( '//a', '/' ), '//a' )

    t:eqdict( { 'abcdefg', '', '' }, strutil.split( 'abcdefg//', '/' ), '"abcdefg" and dual pattern' )
    t:eqdict( { '', 'abcdefg', '' }, strutil.split( '/abcdefg/', '/' ), '/abcdefg/' )
    t:eqdict( { '', '', 'abcdefg' }, strutil.split( '//abcdefg', '/' ), '//abcdefg' )

    t:eqdict( { 'abc', 'xyz', 'uvw' }, strutil.split( 'abc/xyz/uvw', '/' ), 'full' )
    t:eqdict( { '', 'abc', '' }, strutil.split( '/abc/', '/' ), '/abc/' )
    t:eqdict( { '', '', 'abc' }, strutil.split( '//abc', '/' ), '//abc' )

    t:eq( str, table.concat( strutil.split( str, '/' ), '/' ) )

end

function test_startswith(t)
    local s = strutil.startswith
    t:eq(true, s( '', '' ) )
    t:eq(true, s( 'a', '' ) )
    t:eq(false, s( 'a', 'b' ) )
    t:eq(true, s( 'ab', 'a' ) )
    t:eq(true, s( 'ab', 'ab' ) )
    t:eq(false, s( 'ab', 'abc' ) )
end

function test_endswith(t)
    local s = strutil.endswith
    t:eq(true, s( '', '' ) )
    t:eq(true, s( 'a', '' ) )
    t:eq(false, s( 'a', 'b' ) )
    t:eq(true, s( 'ab', 'b' ) )
    t:eq(true, s( 'ab', 'ab' ) )
    t:eq(false, s( 'ab', 'bc' ) )
end

function test_rjust(t)
    local f = strutil.rjust
    t:eq( '.......abc', f( 'abc', 10, '.' ) )
    t:eq( '       abc', f( 'abc', 10 ) )
end

function test_ljust(t)
    local f = strutil.ljust
    t:eq( 'abc.......', f( 'abc', 10, '.' ) )
    t:eq( 'abc       ', f( 'abc', 10 ) )
end

function test_fnmatch(t)

    function t_match(s, ptn, ok)
        t:eq(
        strutil.fnmatch(s, ptn), ok,
        s .. ' ' .. ptn .. ' ' .. tostring(ok)
        )
    end

    t_match('', '', true)
    t_match('a', '', false)
    t_match('a', 'a', true)
    t_match('a', 'b', false)

    t_match('', '?', false)
    t_match('?', '?', true)
    t_match('*', '?', true)
    t_match('.', '?', true)
    t_match('a', '?', true)

    t_match('', '*', true)
    t_match('a', '*', true)
    t_match('ab', '*', true)
    t_match('?', '*', true)
    t_match('??', '*', true)
    t_match('..', '*', true)

    t_match('a', '.', false)

    t_match('.', '*.*', true)
    t_match('a.', '*.*', true)
    t_match('.b', '*.*', true)
    t_match('a.b', '*.*', true)
    t_match('a.b.c', '*.*', true)
    t_match('.a.b.c', '*.*', true)
    t_match('.a.b.c.', '*.*', true)
    t_match('abc', '*.*', false)

    t_match('a.b', '.*', false)
    t_match('a.b', '*.', false)
    t_match('a.b', '*', true)
    t_match('a.b.c', '*', true)

    t_match('', '\\', false)
    t_match('\\', '\\', true)
    t_match('\\a', '\\', false)

    -- escaped
    t_match('*', '\\*', true)
    t_match('a', '\\*', false)

    t_match('?', '\\?', true)
    t_match('a', '\\?', false)
    t_match('ab', '\\?', false)

    -- non escaped
    t_match('a', '\\\\*', false)
    t_match('\\', '\\\\*', true)
    t_match('\\a', '\\\\*', true)
    t_match('\\abcd*', '\\\\*', true)

    t_match('?', '\\\\?', false)
    t_match('a', '\\\\?', false)
    t_match('\\?', '\\\\?', true)
    t_match('\\a', '\\\\?', true)
end


function bench_split()

    local str ='/v1/get/video.vic.sina.com.cn%2fmt788%2f9e%2f1a%2f81403.jpg/%7b%22xACL%22%3a%20%7b%22GRPS000000ANONYMOUSE%22%3a%20%5b%22read%22%5d, %20%22SINA00000000000SALES%22%3a%20%5b%22read%22, %20%22write%22, %20%22read_acp%22, %20%22write_acp%22%5d%7d, %20%22Info%22%3a%20null, %20%22Type%22%3a%20%22image%5c%2fjpeg%22, %20%22ver%22%3a%201042410872, %20%22Get-Location%22%3a%20%5b%7b%22CheckNumber%22%3a%201042410872, %20%22GroupID%22%3a%20341476, %20%22Partitions%22%3a%20%5b%7b%22IPs%22%3a%20%5b%2258.63.236.89%22, %20%2210.71.5.89%22%5d, %20%22PartitionID%22%3a%20%22185c3e5700014004975f90b11c13fc5e%22, %20%22IDC%22%3a%20%22.dx.GZ%22%7d, %20%7b%22IPs%22%3a%20%5b%2258.63.236.184%22, %20%2210.71.5.184%22%5d, %20%22PartitionID%22%3a%20%225a56155500014009aaa590b11c148e88%22, %20%22IDC%22%3a%20%22.dx.GZ%22%7d, %20%7b%22IPs%22%3a%20%5b%2260.28.228.36%22, %20%22172.16.228.36%22%5d, %20%22PartitionID%22%3a%20%22a41d4006000140029595d4ae52b17fe1%22, %20%22IDC%22%3a%20%22.wt.TJ%22%7d, %20%7b%22IPs%22%3a%20%5b%22111.161.78.59%22, %20%22172.16.48.59%22%5d, %20%22PartitionID%22%3a%20%22d7fee54d00014006b58090b11c145321%22, %20%22IDC%22%3a%20%22.wt.TJ%22%7d%5d%7d%5d, %20%22Info-Int%22%3a%200, %20%22ts%22%3a%201356544141, %20%22ACL%22%3a%20%7b%22SINA00000000000SALES%22%3a%20%5b%22read%22, %20%22write%22, %20%22read_acp%22, %20%22write_acp%22%5d%7d, %20%22ETag2%22%3a%20%2235b4ec0bfd826ea609054ccca4976e4fc77f3a8b%22, %20%22ETag%22%3a%20%22e14526f8858e2e0f898e72f141f108e4%22, %20%22Key%22%3a%20%22mt788%5c%2f9e%5c%2f1a%5c%2f81403.jpg%22, %20%22Owner%22%3a%20%22SINA00000000000SALES%22, %20%22Origo%22%3a%20%220000000000000000000090b11c09b4d9%22, %20%22GroupClassID%22%3a%206, %20%22File-Meta%22%3a%20%7b%22Content-Type%22%3a%20%22image%5c%2fjpeg%22%7d, %20%22Size%22%3a%2095964%7d?n=1&r=1&w=1&expire=60&ver_key=ts'

    local xx = strutil.split( str, '/' )
    for i, v in ipairs(xx) do
        print( v )
    end

    local i = 1024 * 1024
    while i > 0 do
        xx = strutil.split( str, '/')
        i = i - 1
    end

end

