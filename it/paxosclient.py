
import sys
import json
import urllib

import it.http

_http = it.http

class PaxosError( Exception ):
    def __init__( self, *args, **argkv ):
        self.Code = argkv.get( 'Code' )
        self.Message = argkv.get( 'Message' )

class PaxosClient( object ):

    api_uri_prefix = '/api'

    def __init__( self, ip, port, cluster_id, ident ):
        self.ip = ip
        self.port = port
        self.cluster_id = cluster_id
        self.ident = ident

    def send_cmd( self, cmd, reqbody=None ):

        req = { 'cmd':cmd }

        if reqbody is not None:
            req.update( reqbody )

        uri = self.make_uri( req )

        if reqbody is not None:
            reqbody = json.dumps(reqbody)

        return self.http( uri, reqbody=reqbody )

    def http( self, uri, reqbody=None ):

        reqbody = reqbody or ''

        h = _http.Http( self.ip, self.port, timeout=3 )
        h.send_request( uri, headers={ 'Content-Length': len( reqbody ) } )
        h.send_body( reqbody )
        h.finish_request()

        body = h.read_body( 1024*1024 )
        try:
            body = json.loads( body )
        except:
            pass
        return { 'status': h.status,
                 'headers': h.headers,
                 'body': body, }

    def make_uri( self, req ):

        uri = self.api_uri_prefix \
                + ('/{cluster_id}/{ident}/{cmd}'.format(
                        cluster_id=self.cluster_id,
                        ident=self.ident,
                        cmd=req[ 'cmd' ],
                ))

        query_keys = [ 'ver' ]
        q = {}
        for k in query_keys:
            if k in req:
                q[ k ] = req[ k ]

        uri += '?' + urllib.urlencode( q )
        return uri

def init_view( ip, port, cluster_id, ident, members ):

    view = [ members ]

    return request_ex( ip, port, cluster_id, ident, 'phase3', {
            'ver': 1,
            'val': {
                    'view': [ members ],
            }
    } )

def request( ip, port, cluster_id, ident, cmd, body=None ):

    p = PaxosClient( ip, port, cluster_id, ident )
    rst = p.send_cmd(cmd, reqbody=body)
    return rst

def request_ex( ip, port, cluster_id, ident, cmd, body=None ):

    rst = request( ip, port, cluster_id, ident, cmd, body=body )
    b = rst[ 'body' ]
    if 'err' in b:
        e = b[ 'err' ]
        raise PaxosError( **e )

    return b

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == 'init':
        ip, port, cluster_id, ident = sys.argv[2:6]
        port = int(port)

        members = sys.argv[6:]
        members = dict([(x, i+1) for i, x in enumerate(members)])

        init_view(ip, port, cluster_id, ident, members)
    else:
        raise
