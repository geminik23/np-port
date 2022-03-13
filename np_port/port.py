import pika
from queue import Queue
import threading
import time
import json
import msgpack
import msgpack_numpy as m
import numpy as np



def int_to_bytes(v:int):
    return v.to_bytes(4, byteorder='big')

def int_from_bytes(b:bytes):
    return int.from_bytes(b, byteorder='big')

def pack_msg(sender, op, ptype, data, pcount=0, uid=None):
    return b'O||' + op.encode('utf-8') + b'I||'+ (b'' if uid is None else uid) + b'F||' + sender.encode('utf-8')+b'P||'+ptype.encode('utf-8')+ b'C||'+int_to_bytes(pcount)+b'D||'+(data if data is not None else b' ')

def unpack_msg(body):
    try:
        i = 0
        j = body.find(b'O||', 0)

        i = j
        j = body.find(b'I||', i)
        op = body[i+3:j].decode('utf-8')
        i = j
        j = body.find(b'F||', i)
        uid = body[i+3:j].decode('utf-8')
        i = j
        j = body.find(b'P||', i)
        sender = body[i+3:j].decode('utf-8')
        i = j
        j = body.find(b'C||', i)
        ptype = body[i+3:j].decode('utf-8')
        i = j
        j = body.find(b'D||', i)
        pcount = int_from_bytes(body[i+3:j])
        i = j
        data = body[i+3:]

        # packet type
        if ptype == 'n':
            data = None
        elif ptype == 'j':
            data = json.loads(data.decode('utf-8'))
        elif ptype == 'o':
            data = msgpack.unpackb(data, object_hook=m.decode)

        return (op, uid, sender, ptype, pcount, data)
    except:
        return None
    

class Port:
    def __init__(self, inq:Queue, outq:Queue, backend_handle):
        self._inq = inq
        self._outq = outq
        self._handle = backend_handle
        self._handle.start()
        pass

    ## receive the OpMessage type
    def receive(self):
        try:
            data = self._inq.get_nowait()
            return data
        except: pass

    # send OpMessage type
    def _send(self, msg):
        try:
            data = self._outq.put_nowait(msg)
        except: pass

    def _encode(self, data):
        if data is None: return 'n', None
        t = type(data)
        if t == dict:
            t = 'j'
            data = json.dumps(data).encode('utf-8')
        elif t == np.ndarray:
            t = 'o'
            data = msgpack.packb(data, default=m.encode)
        else: # throw?
            return None, None
        return t, data


    def request(self, to:str, op:str, data):
        t, data = self._encode(data)
        if t is None: return
        self._send(('q', to, op, (t, data)))

    def reply(self, to:str, op:str, data):
        t, data = self._encode(data)
        if t is None: return
        self._send(('r', to, op, (t, data)))

    def publish(self, group, op:str, data):
        t, data = self._encode(data)
        if t is None: return
        self._send(('p', group, op, (t, data)))

class _port():
    def __init__(self, self_id, group_name, url, inq:Queue, outq:Queue):
        self._name = self_id 
        self._group = group_name
        self._url = url

        self._inq = inq
        self._outq = outq

        self._sys_id = 0
        self._conn = None

        pass

    def system_message_id(self):
        msgid = "{}:sys{}".format(self._name, self._sys_id)
        self._sys_id += 1
        return msgid



    def _init(self):
        if self._conn != None: self._conn.close()
        # log.info("connecting to url %s"%self._url)
        conn = pika.BlockingConnection( pika.URLParameters(self._url))
        ch = conn.channel()
        ch.exchange_declare('state', exchange_type='topic')


        # init state
        result = ch.queue_declare('', exclusive=True)
        queue_name = result.method.queue

        ch.queue_bind( exchange='state', queue=queue_name, routing_key='All.State')
        ch.queue_bind( exchange='state', queue=queue_name, routing_key='{}.State'.format(self._group))

        self._state_qname = queue_name

        # task
        ch.queue_declare('{}.Task'.format(self._name))
        ch.queue_declare('{}.Reply'.format(self._name))
        # ch.queue_declare('{}.Reply'.format(self._name), exclusive=True)


        # hb
        self._ch = ch
        self._conn = conn
        pass


    def run(self):
        self._init()
        ch = self._ch
        stateq = self._state_qname
        taskq = '{}.Task'.format(self._name)
        resq = '{}.Reply'.format(self._name)
        # hbmsg = {'id':None, 'src':self._name, 'dst':'MainService'}


        while True:
            last = time.perf_counter()
            while True:
                #### inq
                t = None
                try:
                    t, to, op, data = self._inq.get_nowait()
                    ptype, data = data
                except Exception as e:
                    pass
                if t is not None:
                    if t == 'q': 
                        qname = '{}.Task'.format(to)
                        body = pack_msg(self._name, op, ptype, data, pcount=0)
                        ch.basic_publish(exchange='', routing_key=qname, body=body, properties=pika.BasicProperties(reply_to=resq, expiration=str(6000)),)
                    elif t == 'r':
                        qname = '{}.Reply'.format(to)
                        body = pack_msg(self._name, op, ptype, data, pcount=0)
                        ch.basic_publish(exchange='', routing_key=qname, properties=pika.BasicProperties(expiration=str(6000)), body=body)
                    elif t == 'p': # publish
                        qname = 'All.State' if to is None else '{}.State'.format(to)
                        body = pack_msg(self._name, op, ptype, data, pcount=0)
                        ch.basic_publish('state', qname, body=body,properties=pika.BasicProperties(expiration=str(6000)),)

                ## TODO use uid and pcount later for division of packet. ignore here

                # subscribe
                method, props, body = ch.basic_get(queue=stateq, auto_ack=False)
                if body != None:
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                    unpacked = unpack_msg(body)
                    if unpacked is not None:
                        op, uid, sender, packet_type, pcount, data = unpacked 

                    msg = ('Pub', (sender, op, data))
                    self._outq.put(msg)
                    pass

                # Request
                method, props, body = ch.basic_get(queue=taskq, auto_ack=False)
                if body != None:
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                    unpacked = unpack_msg(body)
                    if unpacked is not None:
                        op, uid, sender, packet_type, pcount, data = unpacked
                    msg = ('Req', (sender, op, data))
                    self._outq.put(msg)

                # Reply
                method, props, body = ch.basic_get(queue=resq, auto_ack=False)
                if body != None:
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                    unpacked = unpack_msg(body)
                    if unpacked is not None:
                        op, uid, sender, packet_type, pcount, data = unpacked 
                    msg = ('Rep', (sender, op, data))
                    self._outq.put(msg)



def _run_port(self_id, group_name, url, inq, outq):
    p = _port(self_id, group_name, url, inq, outq)
    p.run()

def create_port(self_id:str, group_name:str, url)->Port:
    '''
    current node id / group name / rabbitmq url 
    '''

    inq = Queue()
    outq = Queue()

    p = threading.Thread(target=_run_port, args=(self_id, group_name, url, outq, inq), daemon=True)
    port = Port(inq, outq, p)


    return port
