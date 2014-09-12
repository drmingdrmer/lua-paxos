
import json
import urllib

import it.http

class PaxosError( Exception ):
    def __init__( self, *args, **argkv ):
        self.Code = argkv.get( 'Code' )
        self.Message = argkv.get( 'Message' )

class PaxosClient( object ):

    api_uri_prefix = '/api'

    def __init__( self, ident ):
        self.ident = int( ident )
        self.cluster_id = 'x'

    def send_cmd( self, cmd, reqbody=None ):

        req = { 'cmd':cmd }

        if reqbody is not None:
            req.update( reqbody )

        uri = self.make_uri( req )

        if reqbody is not None:
            reqbody = json.dumps(reqbody)

        return self.http( uri, req_body=reqbody )

    def http( self, uri, req_body=None ):

        req_body = req_body or ''

        h = it.http.Http( '127.0.0.1', 9080+int(self.ident), timeout=3 )
        h.send_request( uri, headers={ 'Content-Length': len( req_body ) } )
        h.send_body( req_body )
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

def ids_dic(ids):
    return dict([ (str(x),str(x))
                  for x in ids ])

def init_view( ident, view_ids, ver=1 ):

    if type(view_ids[0]) not in (type(()), type([])):
        view_ids = [view_ids]

    view = [ ids_dic(x) for x in view_ids ]

    return request( 'phase3', ident, { 'ver':ver, 'val': {
            'view': view,
    } } )

def request( cmd, ident, body=None ):
    if cmd == 'get_leader':
        cmd, body = 'get', {"key":"leader"}

    elif cmd == 'get_view':
        cmd, body = 'get', {"key":"view"}

    p = PaxosClient( ident )
    rst = p.send_cmd(cmd, reqbody=body)
    return rst

def req_ex( cmd, to_ident, body=None ):

    rst = request( cmd, to_ident, body )
    b = rst[ 'body' ]
    if 'err' in b:
        e = b[ 'err' ]
        raise PaxosError( **e )

    return b
