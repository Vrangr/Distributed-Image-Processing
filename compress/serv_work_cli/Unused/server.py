import random
import sys
import threading
import time
import zmq
import signal,sys
import socket
# import heartbeat_server as sh


NBR_CLIENTS = 2
NBR_WORKERS = 5
fe_addr = "tcp://0.0.0.0:5565"
be_addr = "tcp://0.0.0.0:5566"
monitor_addr = "tcp://0.0.0.0:5567"
fe_client_addr = "tcp://192.168.1.11:5565"
be_worker_addr = "tcp://192.168.1.11:5566"
fe_monitor_addr = "tcp://192.168.1.11:5567"

def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


def asbytes(obj):
    s = str(obj)
    if str is not bytes:
        # Python 3
        s = s.encode('ascii')
    return s


def add_to_queue(msg):
    pass


if __name__ == '__main__':
    print("Serving on {} ".format(socket.gethostbyname(socket.gethostname())))
    ctx = zmq.Context()

    # Front end clients
    fe_client = ctx.socket(zmq.ROUTER)
    fe_client.bind(fe_addr)

    # Backend end clients
    be_worker = ctx.socket(zmq.ROUTER)
    be_worker.bind(be_addr)

    # For monitor messages
    monitor_client = ctx.socket(zmq.PULL)
    monitor_client.bind(monitor_addr)

    # Capacity measurement
    be_cap = 0

    # Available workers
    workers = []

    # Initiate worker queue
    # worker_queue = sh.WorkerQueue()

    # Poller for front end and backend
    fe_poller = zmq.Poller()
    be_poller = zmq.Poller()

    print("Polling initialization")

    # Register client connections and worker connections on the poller
    fe_poller.register(fe_client, zmq.POLLIN)
    be_poller.register(be_worker, zmq.POLLIN)
    be_poller.register(monitor_client, zmq.POLLIN)

    while True:
        # POLL FROM WORKERS TO CHECK WHO ARE DONE
        # print(workers)
        try:
            events = dict(be_poller.poll(1000 if be_cap else None))
            print('{}    {}'.format( events, be_worker))
        except zmq.ZMQError:
            print("Error bro")
            break  # interrupted

        previous = be_cap
        # Handle reply from local worker
        msg = None
        # GET WORKER ADDRESS IF A WORKER IS FREE AND NUMBER OF WORKERS IS LESS THAN MAX
        if be_worker in events and be_cap != NBR_WORKERS:
            # get ready msg from the worker and add worker address to the address pool
            msg = be_worker.recv_multipart()
            (address, empty), msg = msg[:2], msg[2:]
            workers.append(address.decode('ascii'))
            be_cap += 1

            # If it's READY, don't route the message any further
            if msg[-1] == b'READY':
                msg = None
            elif msg[-1] == b'HEARTBEAT':
                pass

        if msg is not None:
            fe_client.send_multipart(msg)

        # handle monitor message
        if monitor_client in events:
            print("Monitor : %s" % monitor_client.recv_string())

            # Now route as many clients requests as we can handle
            # - If we have local capacity we poll both localfe
        while be_cap:
            secondary = zmq.Poller()
            secondary.register(fe_client, zmq.POLLIN)

            events = dict(secondary.poll(0))

            # We'll do peer brokers first, to prevent starvation
            if fe_client in events:
                msg = fe_client.recv_multipart()
                add_to_queue(msg)
            else:
                break  # No work, go back to backends

            if be_cap:
                t = str(workers.pop(0))
                msg = [t.encode('ascii'), b''] + msg
                # print("Worker {} Handle client {} with work {}".format(msg[0].decode('ascii'), msg[2].decode('ascii'),
                # msg[4].decode('ascii')))
                be_worker.send_multipart(msg)
                be_cap -= 1
