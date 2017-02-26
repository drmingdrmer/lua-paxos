#!/usr/bin/env python
# coding: utf-8

import os
import sys
import datetime
import time
import random
import threading
import socket
import pprint

from it.paxoscli import PaxosClient, PaxosError, ids_dic, init_view, request, request_ex
from it.ngxctl import ngx_start, ngx_stop, ngx_restart
from it.sto import init_sto

def err_rst( code ):
    return { "err": { "Code": code } }

class ee( object ):
    NoChange = err_rst("NoChange")
    NoView = err_rst("NoView")
    DuringChange = err_rst("DuringChange")
    QuorumFailure = err_rst("QuorumFailure")
    VerNotExist = err_rst("VerNotExist")

g1 = ids_dic("1")
g2 = ids_dic("2")
g3 = ids_dic("3")
g12 = ids_dic("12")
g23 = ids_dic("23")
g13 = ids_dic("13")
g123 = ids_dic("123")
g234 = ids_dic("234")

def out( *args ):
    os.write( 1, " ".join( [str(x) for x in args] ) + "\n" )

def check_test():
    cases = (
            # func, args, result
            ( "view change after dead detected",
              (init_view,(1,(1,2,3)), {}),
              (init_view,(2,(1,2,3)), {}),
              (init_view,(3,(1,2,3)), {}),
              (request,('get_view',1,),{"ver":1,"key":"view","val":[g123]}),
              (request,('get_view',2,),{"ver":1,"key":"view","val":[g123]}),
              (request,('get_view',3,),{"ver":1,"key":"view","val":[g123]}),
              (request,('get',1,{"key":"i"}),{"ver":1, "key":"i"}),

              (request,('isalive',2,),{}),
              (request,('isalive',4,),{"err":{"Code": "NoView"}}),

              (init_view,(4,(1,2,3)), {}),
              (request,('isalive',4,),{"err":{"Code": "NotMember"}}),

              # should have been destoried
              (time.sleep, (2,), None),
              (request,('isalive',4,),{"err":{"Code": "NoView"}}),

              # # after shut down 1, 4 become a member
              # (ngx_stop, ("1", ), None),
              # (time.sleep, (10,), None),
              # (request,('get_view',2,),{"ver":6,"key":"view","val":[g234]}),
              # (request,('get_view',3,),{"ver":6,"key":"view","val":[g234]}),
              # (request,('get_view',4,),{"ver":6,"key":"view","val":[g234]}),

              # (ngx_stop, ("2", ), None),
              # (time.sleep, (3,), None),
              # (request,('get_view',3,),{"ver":10,"key":"view","val":[g234]}),
              # (request,('get_view',4,),{"ver":10,"key":"view","val":[g234]}),
            ),
    )

    for case in cases:
        ngx_restart('1234')
        init_sto()

        mes = case[0]
        out( "" )
        out( "="*7, mes )

        for actions in case[1:]:
            f, args, rst = actions[:3]

            r = f( *args ) or {}
            b = r.get('body')

            out( "" )
            out( f.__name__, args )
            pprint.pprint( rst )
            pprint.pprint( b )
            assert b == rst, "expect to be " +repr(rst) + " but: " +repr(b)

            out( 'OK: ', )
            out( r.get('status'), r.get('body') )

    # nondeterministic test

    ngx_stop("1")
    time.sleep(10)

    # after shutting down 1. 4 should become a member
    for mid in (2, 3, 4):
        b = request('get_view', mid)['body']
        assert b['val'] == [g234]

    # after shutting down 2. only 3, 4 is alive.
    ngx_stop("2")
    time.sleep(10)

    vers = {'3':0, '4':0}
    while vers['3'] < 60 and vers['4'] < 60:

        for mid in vers:

            r = request('get_view', mid)
            b = r['body']
            _ver = b['ver']
            assert _ver >= vers[mid], "expect to get a greater version than: " + repr(vers[mid]) + ' but: ' + repr(b)
            vers[mid] = _ver

            assert '1' not in b['val'][0] or '2' not in b['val'][0]
            assert '3' in b['val'][0]
            assert '4' in b['val'][0]

            if len(b['val']) == 2:
                assert '1' not in b['val'][1] or '2' not in b['val'][1]
                assert '3' in b['val'][1]
                assert '4' in b['val'][1]

            time.sleep(1)


if __name__ == "__main__":
    import it.ngxconf
    it.ngxconf.make_conf(4, 'true')

    check_test()
