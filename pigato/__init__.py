# -*- encoding: utf-8 -*-

# The MIT License (MIT)
#
# Copyright (c) 2015 Adriano Ribeiro
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Sof# -*- coding: utf-8 -*-tware, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
from abc import abstractmethod
from datetime import timedelta, timezone, datetime
from uuid import uuid4
import zmq
from tornado.log import gen_log
from zmq.eventloop import ioloop, zmqstream
from zmq.utils.monitor import recv_monitor_message

import mdp

HEARTBEAT_LIVENESS = 3


class Client():
    def __init__(self, broker, conf=None, context=None, io_loop = None):

        if zmq.zmq_version_info() < (4, 0):
            raise RuntimeError("monitoring in libzmq version < 4.0 is not supported")

        conf = conf or {}
        self._conf = dict(
            autostart=conf.get('autostart') or False,
            reconnect=conf.get('reconnect') or 1000,
            heartbeat=conf.get('heartbeat') or 2500,
            timeout=conf.get('timeout') or 60000,
            retry=0,
            prefix='C{0}'.format(uuid4())
        )

        print('client id {0}'.format( self._conf.get('prefix')))
        self._liveness = HEARTBEAT_LIVENESS
        self._broker = broker
        self._loop = io_loop or ioloop.IOLoop.instance()
        self._ctx = context or zmq.Context.instance()
        self._reqs = dict()

        if self._conf['autostart']:
            self.start()

    def start(self):

        print( self._conf.get('prefix'))

        self.stop()
        self._mcnt = 0
        self._socketId = '{0}-{1}'.format(self._conf['prefix'], uuid4())

        self._sock = self._ctx.socket(zmq.DEALER)
        self._monitor = self._sock.get_monitor_socket()
        self._sock.setsockopt_string(zmq.IDENTITY, self._socketId)
        self._sock.setsockopt(zmq.LINGER,1)
        self._stream = zmqstream.ZMQStream(self._sock, self._loop)
        self._stream.on_recv_stream(self.on_data_recv)

        try:
            self._stream.connect(self._broker)
            self._liveness = HEARTBEAT_LIVENESS
        except Exception as ex:
            self.on_error(['error', ex])
            gen_log.exception(ex)

        self._loop.spawn_callback(self.on_socket_event,self._monitor)
        # self.hbeater = ioloop.PeriodicCallback(self.beat, period, self._loop)
        # self.hbeater.start()
        self.on_start()
        self._loop.start()

    def stop(self):
        self.on_stop()

    def send(self, msg):
        print(msg)
        self._stream.send_multipart(msg)

    def heartbeat(self, rid):
        msg = [mdp.CLIENT, mdp.W_HEARTBEAT]

        if rid:
            msg.append(rid)

        self.send(msg)

    def request(self, serviceName, data, opts=None):
        rid = str(uuid4())
        req_opts = opts or {}

        for prop in  ['timeout', 'retry']:
            req_opts[prop] = opts.get(prop) or self._conf[prop]

        req = dict(
            rid=rid,
            timeout=opts['timeout'],
            # POSIX time in milliseconds
            ts= (datetime.now().replace(tzinfo=timezone.utc) - datetime(1970,1,1, tzinfo=timezone.utc)) // timedelta(seconds=1) * 1e3,
            opts=req_opts,
            heartbeat=self.heartbeat,
            _finalMsg=None,
            ended=False
        )

        req['lts'] = req['ts']

        self._reqs[rid]=req

        self.send([mdp.CLIENT, mdp.W_REQUEST, serviceName, rid, json.dumps(data), json.dumps(opts)])

        return req

    def on_socket_event(self, monitor):
        # Event names:
        # EVENT_ACCEPTED :   32
        # EVENT_ACCEPT_FAILED :   64
        # EVENT_ALL : 65535
        # EVENT_BIND_FAILED :   16
        # EVENT_CLOSED :  128
        # EVENT_CLOSE_FAILED :  256
        # EVENT_CONNECTED :    1
        # EVENT_CONNECT_DELAYED :    2
        # EVENT_CONNECT_RETRIED :    4
        # EVENT_DISCONNECTED :  512
        # EVENT_LISTENING :    8
        # EVENT_MONITOR_STOPPED : 1024

        EVENT_MAP = {}
        print("Event names:")
        for name in dir(zmq):
            if name.startswith('EVENT_'):
                value = getattr(zmq, name)
                print("%21s : %4i" % (name, value))
                EVENT_MAP[value] = name

        while monitor.poll():
            evt = recv_monitor_message(monitor)
            evt.update({'description': EVENT_MAP[evt['event']]})
            print("Event: {}".format(evt))

            if evt['event'] == zmq.EVENT_MONITOR_STOPPED:
                break
            elif evt['event'] == zmq.EVENT_CONNECTED:
                self.on_connect()
            elif evt['event'] == zmq.EVENT_DISCONNECTED:
                self.on_disconnect()

            # elif evt['event'] == zmq.EVENT_CONNECT_DELAYED:
            #
            # elif evt['event'] == zmq.EVENT_CLOSED:
            #
            # elif evt['event'] == zmq.EVENT_CONNECT_RETRIED:

        monitor.close()
        print()
        print("event monitor thread done!")

    def on_error(self, error):
        print('error: {0}.'.format(error))

    def on_data_recv(self, stream, msg):

        header = msg[0]
        type = msg[1]

        self._liveness = HEARTBEAT_LIVENESS

        if header != mdp.CLIENT:
            self.on_error('ERR_MSG_HEADER')
            return

        self._mcnt+=1

        if type == mdp.W_HEARTBEAT:
            return

        if len(msg) < 3:
            self.on_error('ERR_MSG_LENGTH')
            return

        rid = msg[3]

        req = self._reqs.get(rid)

        if not req:
            self.on_error('ERR_REQ_INVALID')
            return

        err = +msg[4] or 0
        data = msg[5] or None

        if data:
            data = json.dump(data)

        if err == -1:
            err = data
            data = None

        if type == mdp.W_REPLY or type == mdp.W_REPLY_PARTIAL:
            req.lts = self.get_time

            if type == mdp.W_REPLY:
                req['_finalMsg'] = [err,data]
                req['ended'] = True
                del self._reqs[rid]

            if err:
                self.on_error(err)

            if type == mdp.W_REPLY:
                self.on_msg(data)

    # @abstractmethod
    def on_msg(self, data):
        print('i am receive data {0}.'.format(data))

    def on_end(self):
        print('i am finished.')

    def on_start(self):
        print('i am started.')

    def on_stop(self):
        print('i am stopped.')

    def on_connect(self):
        print('i am connected.')
        client.request('echo', 'foo-stream', { 'timeout': 10000 })
        print('i made a request.')

    def on_disconnect(self):
        print('i was disconnected.')

    @property
    def get_time(self):
        return (datetime.now().replace(tzinfo=timezone.utc) - datetime(1970,1,1, tzinfo=timezone.utc)) // timedelta(seconds=1) * 1e3


client = Client('tcp://127.0.0.1:55555')
client.start()
ioloop.IOLoop.current().start()