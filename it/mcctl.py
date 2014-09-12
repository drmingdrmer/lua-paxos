
import telnetlib

def mc_flush():
    t = telnetlib.Telnet( '127.0.0.1', 8001 )
    t.write( 'flush_all\r\n' )

