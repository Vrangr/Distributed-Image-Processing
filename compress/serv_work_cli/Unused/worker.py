import os
import random
import sys
import multiprocessing as mp
import time
import zmq
import signal,sys
import socket


def task_thread(_msg_queue, _done_queue, _worker_task_soc):
    while True:
        if not _msg_queue.empty():
            work = _msg_queue.get()
            # do work
            # push the done work into done_queue
            _done_queue.put(work)
            _worker_task_soc.send_multipart(work)



def asbytes(obj):
    s = str(obj)
    if str is not bytes:
        # Python 3
        s = s.encode('ascii')
    return s


def worker_task( msg_queue, done_queue, worker_task_soc):
    """Worker using REQ socket to do LRU routing"""
    # Tell broker we're ready for work
    worker_task_soc.send(b"READY")

    # Process messages as they arrive
    while True:
        try:
            msg = worker_task_soc.recv_multipart()
        except zmq.ZMQError:
            # interrupted
            print("Exited {}".format(worker_task_soc.identity))
            return
        # Put recieved msg into the queue
        msg_queue.put(msg)
        time.sleep(random.randint(0, 1))
        print("Worker {} done with {}".format(worker_task_soc.identity, msg[2].decode('ascii')))
        worker_task_soc.send_multipart(msg)


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        myserver = asbytes(sys.argv[1])
        be_worker_addr = "tcp://{}:5566".format(myserver.decode('ascii'))

        # Create socket for queueing the image msgs
        ctx = zmq.Context()
        worker_task_soc = ctx.socket(zmq.REQ)
        worker_task_soc.identity = ("%s" % (12000 + os.getpid())).encode('ascii')
        worker_task_soc.connect(be_worker_addr)

        host_name = socket.gethostname()
        proc_queue = mp.Queue()
        done_queue = mp.Queue()

        worker_task(proc_queue, done_queue, worker_task_soc)
