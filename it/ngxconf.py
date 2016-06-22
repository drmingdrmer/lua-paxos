#!/usr/bin/env python
# coding: utf-8

import string

tmpl = '''
worker_processes  1;
error_log  logs/err-$ident.log  error;
pid        logs/$ident.pid;

events {
    worker_connections  256;
}

http {
    log_format accfmt '$$remote_addr [$$time_local]'
                       ' "$$request" $$status $$bytes_sent $$request_time'
                       ' "$$paxos_log"'
                       ;

    access_log logs/acc-$ident.log accfmt;

    lua_package_path  '$$prefix/../../lib/?.lua;;';
    lua_package_cpath  '$$prefix/../../clib/?.so;;';

    lua_shared_dict paxos_lock 10m;
    lua_socket_log_errors off;
    init_worker_by_lua 'local e=require("sample"); e.members_on_this_node={"$ident"}; e.init_cluster_check($enabled)';

    server {
        listen       908$ident;

        location /api/ {
            set $$paxos_log "";
            content_by_lua 'require("sample").cluster.server:handle_req()';
        }

        location /user_api/get_leader {
            set $$paxos_log "";
            content_by_lua '

            local function output( code, ... )
                ngx.status = code
                ngx.print( ... )
                ngx.eof()
                ngx.exit( ngx.HTTP_OK )
            end

            local s = require("sample").cluster.server
            local paxos, err, errmes = s:new_paxos({cluster_id=ngx.var.arg_cluster_id, ident=ngx.var.arg_ident})
            if err then
                output( 500, err )
            end

            local _l, err, errmes = paxos:local_get("leader")
            if err then
                output( 500, err )
            end

            local _m, err, errmes = paxos:local_get_members()
            if err then
                output( 500, err )
            end

            local ids = {}

            for k, _ in pairs( _m.val or {} ) do
                table.insert( ids, k )
            end
            table.sort(ids)
            local ids = table.concat( ids, "," )

            local leader, ver = _l.val, _l.ver
            if leader then
                output( 200,
                        "ver:", _l.ver,
                        " leader:", leader.ident,
                        " lease:", leader.__lease,
                        " members:", ids
                )
            else
                output( 404, "- -" )
            end
            ';
        }
    }
}
# vim: ft=ngx
'''

pref = 'srv/nginx/conf/'

def make_conf(n=3, enable_cluster_check = 'false'):
    for i in range( 1, n+1 ):
        data = { 'ident': str(i), 'enabled': enable_cluster_check }
        cont = string.Template( tmpl ).substitute( data )
        with open( pref + str(i), 'w' ) as f:
            f.write( cont )
            f.close()

if __name__ == "__main__":
    make_conf(3, 'false')
