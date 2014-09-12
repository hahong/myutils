#!/usr/bin/env python

import os
import time
import subprocess
import sys
import socket

JOB_LEVEL_CMD = 'squeue -u hahong'
N_THRESHOLD = 50
N_REPS = N_THRESHOLD
HOST = None
PORT = None


def is_server_alive(host, port, retry=5):
    if host is None or port is None:
        return True    # indefinite running

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        sock.close()
        return True
    except:
        # host is down
        if retry <= 0:
            return False
        print '* Retrying:', retry
        return is_server_alive(host, port, retry=retry - 1)


def get_job_level(job_level_cmd=JOB_LEVEL_CMD, screen=['om_']):
    output = subprocess.Popen(job_level_cmd.split(),
            stdout=subprocess.PIPE).communicate()[0]
    return len([None for e in output.split('\n') if
        any([s in e for s in screen])])


def main(argv, n_threshold=N_THRESHOLD, n_reps=N_REPS,
        host=HOST, port=PORT):
    if 'NTHR' in os.environ:
        n_threshold = int(os.environ['NTHR'])
        print '* n_threshold =', n_threshold
    if 'NREPS' in os.environ:
        n_reps = int(os.environ['NREPS'])
        print '* n_reps =', n_reps
    if 'HOST' in os.environ:
        host = os.environ['HOST']
        print '* host =', host
    if 'PORT' in os.environ:
        port = int(os.environ['PORT'])
        print '* port =', port

    while True:
        if not is_server_alive(host, port):
            print '* Host is down.  Exiting...'
            break
        if get_job_level() <= n_threshold:
            print '* Host %s:%s is up' % (host, str(port))
            print '* Below level %d' % n_threshold
            for _ in xrange(n_reps):
                os.system(' '.join(argv[1:]))
            print '* Done exec %d' % n_reps
            print
        time.sleep(1)

if __name__ == '__main__':
    main(sys.argv)
