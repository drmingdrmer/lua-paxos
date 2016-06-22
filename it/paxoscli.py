
import json
import urllib

import it.http
import it.paxosclient

PaxosError = it.paxosclient.PaxosError

class PaxosClient( it.paxosclient.PaxosClient ):

    api_uri_prefix = '/api'

    def __init__( self, ident ):
        ip, port, cluster_id = ip_port_cid(ident)
        super(PaxosClient, self).__init__(ip, port, cluster_id, ident)

def ip_port_cid(ident):
    ip = '127.0.0.1'
    port = 9080+int(ident)
    cluster_id = 'x'
    return ip, port, cluster_id

def ids_dic(ids):
    return dict([ (str(x),str(x))
                  for x in ids ])

def init_view( ident, view_ids, ver=1 ):

    if type(view_ids[0]) not in (type(()), type([])):
        view_ids = [view_ids]

    view = [ ids_dic(x) for x in view_ids ]

    return request( 'phase3', ident,
                    { 'ver':ver,
                      'val': {
                              'view': view,
                      } } )

def request( cmd, ident, body=None ):
    if cmd == 'get_leader':
        cmd, body = 'get', {"key":"leader"}

    elif cmd == 'get_view':
        cmd, body = 'get', {"key":"view"}

    ip, port, cluster_id = ip_port_cid(ident)
    return it.paxosclient.request(ip, port, cluster_id, ident, cmd, body=body)

def request_ex( cmd, to_ident, body=None ):
    ip, port, cluster_id = ip_port_cid(to_ident)
    return it.paxosclient.request_ex(ip, port, cluster_id, to_ident, cmd, body=body)
