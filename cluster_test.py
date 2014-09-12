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

from it.paxoscli import PaxosClient, PaxosError, ids_dic, init_view, request, req_ex
from it.ngxctl import ngx_start, ngx_stop, ngx_restart
from it.mcctl import mc_flush

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

              # after shut down 1, 4 become a member
              (ngx_stop, ("1", ), None),
              (time.sleep, (10,), None),
              (request,('get_view',2,),{"ver":6,"key":"view","val":[g234]}),
              (request,('get_view',3,),{"ver":6,"key":"view","val":[g234]}),
              (request,('get_view',4,),{"ver":6,"key":"view","val":[g234]}),

              (ngx_stop, ("2", ), None),
              (time.sleep, (10,), None),
              (request,('get_view',3,),{"ver":10,"key":"view","val":[g234]}),
              (request,('get_view',4,),{"ver":10,"key":"view","val":[g234]}),
            ),
    )

    for case in cases:
        ngx_restart('1234')
        mc_flush()

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


if __name__ == "__main__":
    import it.ngxconf
    it.ngxconf.make_conf(4, 'true')

    check_test()
