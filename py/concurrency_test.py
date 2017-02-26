#!/usr/bin/env python
# coding: utf-8

import os
import sys
import datetime
import time
import random
import threading
import socket

from it.paxoscli import PaxosClient, PaxosError, ids_dic, init_view, request, request_ex
from it.ngxctl import ngx_start, ngx_stop, ngx_restart
from it.sto import init_sto

g1 = ids_dic("1")
g2 = ids_dic("2")
g3 = ids_dic("3")
g12 = ids_dic("12")
g23 = ids_dic("23")
g13 = ids_dic("13")
g123 = ids_dic("123")

def err_rst( code ):
    return { "err": { "Code": code } }

class ee( object ):
    NoChange = err_rst("NoChange")
    NoView = err_rst("NoView")
    DuringChange = err_rst("DuringChange")
    QuorumFailure = err_rst("QuorumFailure")
    VerNotExist = err_rst("VerNotExist")

def randsleep( ratio=1 ):
    time.sleep( random.random()*0.4*ratio )

def dd( args, *more_args ):
    dt = str(datetime.datetime.now())
    out( dt, *( args + list(more_args) ) )

def out( *args ):
    os.write( 1, " ".join( [str(x) for x in args] ) + "\n" )

def integration_test():
    cases = (
            # func, args, result, result_filter
            ( "set",
              (init_view,(1,(1,2,3)), {}),
              (init_view,(2,(1,2,3)), {}),
              (request,('get_view',2,),{"ver":1,"key":"view","val":[g123]}),
              (request,('get',1,{"key":"i"}),{"ver":1, "key":"i"}),
              (request,('set',1,{"key":"i", "val":100}),{"ver":2, "key":"i", "val":100}),
              (request,('get',2,{"key":"i"}),{"ver":2, "key":"i", "val":100}),

              # re-set does not change
              (request,('set',1,{"key":"i", "val":100}),{"ver":2, "key":"i", "val":100}),
              (ngx_stop,('2',), None),
              (ngx_stop,('3',), None),
              (time.sleep, (1,), None ),

              # set without changing require quorum too
              (request,('set',1,{"key":"i", "val":100}),ee.QuorumFailure),
              (ngx_start,('2',), None),

              (time.sleep, (1,), None ),
              (request,('set',1,{"key":"i", "val":{"foo":"bar"}}),{ "ver":3, "key":"i", "val":{ "foo":"bar" } }),

              # re-set table value
              (request,('set',1,{"key":"i", "val":{"foo":"bar"}}),{ "ver":3, "key":"i", "val":{ "foo":"bar" } }),

              # set with specific ver
              (request,('set',1,{"key":"i", "ver":2, "val":{"foo":"bar"}}),{ "err":{ "Code":"VerNotExist", "Message":3 } }),

              # set with different table value
              (request,('set',1,{"key":"i", "ver":3, "val":{"FOO":"bar"}}),{ "ver":4, "key":"i", "val":{ "FOO":"bar" } }),

            ),
            ( "get",
              (init_view,(1,(1,2,3)), {}),
              (init_view,(2,(1,2,3)), {}),
              (request,('get',1,{"key":"i"}),{"ver":1, "key":"i"}),
              (request,('set',1,{"key":"i", "val":100}),{"ver":2, "key":"i", "val":100}),
              (request,('get',2,{"key":"i", "ver":2}),{"ver":2, "key":"i", "val":100}),
              (request,('get',2,{"key":"i", "ver":0}),{ "err":{ "Code":"VerNotExist", "Message":2 } }),
              (request,('get',2,{"key":"i", "ver":1}),{ "err":{ "Code":"VerNotExist", "Message":2 } }),
              (request,('get',2,{"key":"i", "ver":3}),{ "err":{ "Code":"VerNotExist", "Message":2 } }),
            ),
            ( "unable to elect with only 1 member",
              (request,('get_view',1,),ee.NoView),
              (request,('get_view',2,),ee.NoView),
              (init_view,(1,(1,2,3)), {}),
              (request,('get_view',1,),{"ver":1,"key":"view","val":[g123]}),
              (request,('get_leader',1,),{"ver":1,"key":"leader"}),
              (request,('get_or_elect_leader',1,),ee.QuorumFailure),
            ),

            ( "able to elect with only 2 members",
              (init_view,(1,(1,2,3)), {}),
              (init_view,(2,(1,2,3)), {}),
              (request,('get_view',2,),{"ver":1,"key":"view","val":[g123]}),
              (request,('get_or_elect_leader',1,),{"ver":2, "key":"leader", "val":{"ident":"1", "__lease":1}}),
              (request,('get_or_elect_leader',1,),{"ver":2, "key":"leader", "val":{"ident":"1", "__lease":1}}),
              (request,('get_or_elect_leader',2,),{"ver":2, "key":"leader", "val":{"ident":"1", "__lease":1}}),
              (request,('get_leader',1,),{"ver":2,"key":"leader", "val":{"ident":"1", "__lease":1}}),
              (time.sleep, (1,), None ),
              (request,('get_leader',1,),{"ver":2,"key":"leader","val":{"ident":"1", "__lease":0}}),
              (time.sleep, (1,), None ),
              (request,('get_leader',1,),{"ver":2,"key":"leader"}),
              (request,('get_or_elect_leader',2,),{"ver":3,"key":"leader","val":{"ident":"2", "__lease":1}}),

              # get leader with version specified
              (request,('get',2,{"key":"leader", "ver":3}),{"ver":3,"key":"leader","val":{"ident":"2", "__lease":1}}),
              (request,('get',2,{"key":"leader", "ver":4}),{"err": { "Code": "VerNotExist", "Message":3 }}),
            ),

            ( "unable to elect with 2 members with different ver",
              (init_view,(1,(1,2,3)), {}),
              (request,('get_view',1,),{"ver":1,"key":"view","val":[g123]}),
              (init_view,(2,(1,2,3), 2), {}),
              (request,('get_view',2,),{"ver":2,"key":"view","val":[g123]}),
              # 1 will load latest version=2 from 2, and then try to elect
              # leader with version=2 and would found that it locally does not
              # have committed data with version=2
              (request,('get_or_elect_leader',1,),ee.QuorumFailure),
              (request,('get_or_elect_leader',2,),ee.QuorumFailure),
              (request,('get_leader',1,),{"ver":2, "key":"leader"}),
            ),

            ( "elect with dual view",
              (init_view,(1,((1,2,3), (1,2))), {}),
              (init_view,(2,((1,2,3), (1,2))), {}),
              (request,('get_view',2,),{"ver":1,"key":"view","val":[g123, g12]}),
              (request,('get_or_elect_leader',1,),{"ver":2, "key":"leader", "val":{"ident":"1", "__lease":1}}),
              (request,('get_or_elect_leader',1,),{"ver":2, "key":"leader", "val":{"ident":"1", "__lease":1}}),
              (request,('get_or_elect_leader',2,),{"ver":2, "key":"leader", "val":{"ident":"1", "__lease":1}}),
              (request,('get_leader',1,),{"ver":2,"key":"leader", "val":{"ident":"1", "__lease":1}}),
              (request,('read',1,),{"ver":2,"val":{"leader":{"ident":"1", "__lease":1}, "view":[g123, g12]}}),

              (time.sleep, (1,), None ),
              (request,('get_leader',1,),{"ver":2,"key":"leader","val":{"ident":"1", "__lease":0}}),
              (time.sleep, (1,), None ),
              (request,('get_leader',1,),{"ver":2,"key":"leader"}),
              (request,('get_or_elect_leader',2,),{"ver":3,"key":"leader","val":{"ident":"2", "__lease":1}}),
            ),

            ( "elect failure with dual view",
              (init_view,(1,((1,2,3), (1,3))), {}),
              (init_view,(2,((1,2,3), (1,3))), {}),
              (request,('get_view',2,),{"ver":1,"key":"view","val":[g123, g13]}),
              (request,('get_or_elect_leader',1,),ee.QuorumFailure),
            ),

            ( "change_view",
              (init_view,(1,(1,)), {}),
              (ngx_stop,(2,),None),
              (request,('get_view',1,),{"ver":1,"key":"view","val":[g1]}),
              (request,('change_view',1,{"add":g23}),{"ver":3,"key":"view","val":[g123]}),
              (request,('get_view',1,),{"ver":3,"key":"view","val":[g123]}),
              (request,('get_view',3,),{"ver":3,"key":"view","val":[g123]}),
              (request,('change_view',1,{"add":g23}),{"ver":3,"key":"view","val":[g123]}),
            ),

            ( "change_view without any change",
              (init_view,(1,(1,)), {}),
              (request,('get_view',1,),{"ver":1,"key":"view","val":[g1]}),
              (request,('change_view',1,{}), {"ver":1,"key":"view","val":[g1]}),
              (request,('get_view',1,),{"ver":1,"key":"view","val":[g1]}),
            ),
            ( "change_view in process, come to consistent state",
              (init_view,(1,((1,),(1,2)),2), {}),
              (request,('get_view',1,),{"ver":2,"key":"view","val":[g1, g12]}),
              (request,('get_view',2,),ee.NoView),
              (request,('change_view',1,{}), ee.DuringChange),
              (request,('get_view',1,),{"ver":3,"key":"view","val":[g12]}),
            ),
            ( "change_view with unmatched versions",
              (init_view,(1,(1,2,3),2), {}),
              (request,('get_view',1,),{"ver":2,"key":"view","val":[g123]}),
              (request,('get_view',2,),ee.NoView),
              (init_view,(3,(1,2,3),3), {}),
              (request,('get_view',3,),{"ver":3,"key":"view","val":[g123]}),

              # change_view fix unmatched versions
              (request,('change_view',1,{"del":g1}),{"ver":5,"key":"view","val":[g23]}),

              (request,('get_view',1,),{"ver":5,"key":"view","val":[g23]}),
              (request,('get_view',2,),{"ver":5,"key":"view","val":[g23]}),
              (request,('get_view',3,),{"ver":5,"key":"view","val":[g23]}),
            ),
    )

    for case in cases:
        ngx_restart('123')
        init_sto()

        mes = case[0]
        out( "" )
        out( "="*7, mes )

        for actions in case[1:]:
            f, args, rst = actions[:3]
            if len(actions) == 4:
                rst_filter = actions[3]
            else:
                rst_filter = lambda x:x

            r = f( *args ) or {}
            b = r.get('body')
            b = rst_filter(b)

            out( "" )
            out( f.__name__, args )
            import pprint
            pprint.pprint( rst )
            pprint.pprint( b )
            assert b == rst, "expect to be " +repr(rst) + " but: " +repr(b)

            out( 'OK: ', )
            out( r.get('status'), r.get('body') )

def incr_worker(incr_key, idents, n):

    cur_ver = 1

    for i in range( n ):

        for n_try in range( 1, 1024*1024 ):

            randsleep( 0.3 )

            to_ident = idents[ random.randint( 0, len(idents)-1 ) ]

            mes = [ "key-{0}".format( incr_key ),
                    "incr-{i} try-{n_try}".format( i=i, n_try=n_try ),
                    "req to:", to_ident,
                    "with ver:", cur_ver,
            ]

            try:
                b = request_ex( "get", to_ident, { "key":incr_key } )

                remote_ver, remote_val = b[ 'ver' ], b.get('val')
                if remote_ver < cur_ver:
                    # unfinished commit might be seen,
                    continue

                if remote_ver >= cur_ver:
                    # get might see uncommitted value. thus version might
                    # not be seen in future read

                    if remote_val == i + 1:
                        dd( mes, "unfinished done", "get", b )

                    elif remote_val != i:
                        dd( mes, "error: remote val is: {val}, i={i}, ver={ver}".format(val=remote_val, i=i, ver=remote_ver) )
                        sys.exit( 1 )


                b = request_ex("set", to_ident, {"key":incr_key, "ver":cur_ver, "val":i+1})
                dd( mes, "ok", "set", b )

                cur_ver = b['ver']


                b = request_ex( "read", to_ident, {"ver":b[ 'ver' ]} )

                ver = b['ver']
                vals = [ b['val'].get( x, 0 ) for x in idents ]
                total = sum(vals)

                dd( mes, "ver=", b['ver'], "total=", total, "vals=", *vals )
                assert total == ver - 1, 'total == ver - 1: %d, %d' %( total, ver )

                break

            except socket.error as e:
                pass

            except PaxosError as e:
                dd( mes, "err", e.Code, e.Message )

                if e.Code == 'VerNotExist' and cur_ver < e.Message:
                    cur_ver = e.Message
                    dd( mes, 'refreshed ver to', cur_ver )

                randsleep()

            except Exception as e:

                dd( mes, "err", repr(e) )

monkeysess = { 'enabled': True }

def monkey(sess):

    if not monkeysess[ 'enabled' ]:
        return

    stat = dict( [ (x, True) for x in sess['idents'] ] )

    while sess[ 'running' ]:

        ident = sess['idents'][ random.randint( 0, len(sess['idents'])-1 ) ]

        try:
            if stat[ ident ]:
                ngx_stop( ident )
                os.write( 1, 'nginx stopped: ' + ident + '\n' )
                stat[ ident ] = False
            else:
                ngx_start( ident )
                os.write( 1, 'nginx started: ' + ident + '\n' )
                stat[ ident ] = True

            randsleep()

        except Exception as e:
            os.write( 1, repr( e ) + ' while nginx operation: ' + ident + '\n' )


def concurrency_test():
    ngx_restart('123')
    init_sto()

    idents = [ x for x in '123' ]
    nthread = 5
    nincr = 500
    nmonkey = 1

    for ident in idents:
        body = { "ver":1,
                 "val": {
                         "1":0,
                         "2":0,
                         "3":0,
                         "view": [ {
                                 "1":"1",
                                 "2":"2",
                                 "3":"3", }, ],
                 }
        }
        request( 'phase3', ident, body )

    ths = []
    for ident in idents:
        th = threading.Thread( target=incr_worker, args=( ident, idents, nincr ) )
        th.daemon = True
        th.start()

        ths.append( th )

    sess = { 'running':True, "idents":idents,
             'locks': dict( [ (x, threading.RLock()) for x in idents ] )
    }

    monkeys = []
    for ii in range( nmonkey ):
        monkey_th = threading.Thread( target=monkey, args=( sess, ) )
        monkey_th.daemon = True
        monkey_th.start()
        monkeys.append( monkey_th )

    for th in ths:
        while th.is_alive():
            th.join(0.1)

    sess[ 'running' ] = False
    for th in monkeys:
        th.join()

if __name__ == "__main__":
    import it.ngxconf
    it.ngxconf.make_conf(3)

    integration_test()

    monkeysess[ 'enabled' ] = True
    concurrency_test()

    monkeysess[ 'enabled' ] = False
    concurrency_test()
