#!/usr/bin/env python2.6
# coding: utf-8

import sys
import subprocess

ngxpath = 'srv/nginx/sbin/nginx'

def ngx_restart(idents):
    for ident in idents:
        ngx_stop(ident)
        ngx_start(ident)

def ngx_stop(ident):
    subprocess.call( [
            ngxpath,
            "-c", "conf/" + str(ident),
            "-s", "stop",
    ] )

def ngx_start(ident):
    subprocess.call( [
            ngxpath,
            "-c", "conf/" + str(ident),
    ] )

if __name__ == "__main__":

    args = sys.argv[1:]
    cmd = args[0]
    ident = args[1]

    if cmd == 'start':
        ngx_start(ident)
    elif cmd == 'stop':
        ngx_stop(ident)
    elif cmd == 'restart':
        ngx_restart(ident)
    else:
        raise ValueError("Invalid cmd:", cmd)
