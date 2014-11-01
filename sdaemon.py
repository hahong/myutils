#!/usr/bin/env python

import os
import time
import subprocess
import sys
import socket

# defaults
N_THRESHOLD = int(os.environ.get('SD_NTHR', 50))
N_THRESHOLD_LOW = int(os.environ.get('SD_NTHRLOW', 0))
N_THRESHOLD_SYSLOAD = int(os.environ.get('SD_NTHRSYSLOAD', 12))
N_RESERVED_MIN = int(os.environ.get('SD_NRESMIN', 40))
N_REPS = int(os.environ.get('SD_NREPS', 20))
HOST = os.environ.get('SD_HOST')
PORT = os.environ.get('SD_PORT')
PORT = None if PORT is None else int(PORT)
USER = os.environ.get('USER', 'hahong')
NCORES_CMD = 'sinfo -lNe'.split()
PARTITION = os.environ.get('SD_PARTITION', 'om_all_nodes')
SVERBOSE = int(os.environ.get('SD_VERBOSE', 0))
GET_USAGE_SCR_DEF = [PARTITION[:3]]
GET_USAGE_SCR_RUN = [' R ', ' node0']
GET_USAGE_SCR_SUS = [' S ', ' node0']
JOB_LEVEL_CMD = ['squeue', '-o',
                 '%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R   %C']


def get_total_cpus(ncores_cmd=NCORES_CMD, screen=[PARTITION],
                   idx_cpu=4, idx_node=1):
    output = subprocess.Popen(ncores_cmd,
                              stdout=subprocess.PIPE).communicate()[0]
    threads = [int(e.split()[idx_node]) * int(e.split()[idx_cpu])
               for e in output.split('\n') if
               all([s in e for s in screen])]
    return sum(threads)
N_TOTAL_CPUS = get_total_cpus()


def get_usage(job_level_cmd=JOB_LEVEL_CMD,
              screen=GET_USAGE_SCR_DEF,
              screen_run=GET_USAGE_SCR_RUN,
              screen_sus=GET_USAGE_SCR_SUS,
              user=USER, idx_cpu=-1):
    output = subprocess.Popen(job_level_cmd,
                              stdout=subprocess.PIPE).communicate()[0]
    alljs = [int(e.split()[idx_cpu]) for e in output.split('\n') if
             all([s in e for s in screen])]
    scr = screen + [user]
    allujs = [int(e.split()[idx_cpu]) for e in output.split('\n') if
              all([s in e for s in scr])]
    scr_run = scr + screen_run
    allRs = [int(e.split()[idx_cpu]) for e in output.split('\n') if
             all([s in e for s in scr_run])]
    scr_sus = scr + screen_sus
    allSs = [int(e.split()[idx_cpu]) for e in output.split('\n') if
             all([s in e for s in scr_sus])]

    n_thr_all = sum(allujs)
    n_thr_alloc = sum(allRs) + sum(allSs)
    n_thr_notrun = n_thr_all - sum(allRs)
    n_thr_all_everyone = sum(alljs)
    # print n_thr_all, n_thr_alloc, n_thr_notrun
    return n_thr_all, n_thr_alloc, n_thr_notrun, n_thr_all_everyone


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
        time.sleep(5)
        return is_server_alive(host, port, retry=retry - 1)


def main(argv, n_threshold=N_THRESHOLD,
         n_threshold_low=N_THRESHOLD_LOW,
         n_threshold_sysload=N_THRESHOLD_SYSLOAD,
         n_reps=N_REPS,
         n_reserved_min=N_RESERVED_MIN,
         n_cpus=N_TOTAL_CPUS,
         host=HOST, port=PORT,
         verbose=SVERBOSE):

    if n_threshold < n_threshold_low:
        n_threshold = n_threshold_low

    print '* n_threshold:', n_threshold
    print '* n_thr_low  :', n_threshold_low
    print '* n_thr_sysld:', n_threshold_sysload
    print '* n_resv_min :', n_reserved_min
    print '* n_reps     :', n_reps
    print '* n_cpus     :', n_cpus
    print '* host       :', host
    print '* port       :', port
    print

    while True:
        if not is_server_alive(host, port):
            print '* Host is down.  Exiting...'
            break

        q_jobs = False
        try:
            n_threads_all, _, n_threads_notrun, n_threads_everyone = \
                get_usage()

            if n_threads_all < n_threshold_low:
                msg = '* Below min level %d' % n_threshold_low
                q_jobs = True
            elif (n_threads_all < n_threshold and
                  n_threads_notrun < n_threshold_sysload and
                  n_threads_everyone < n_cpus - n_reserved_min):
                msg = '* Below level %d' % n_threshold
                q_jobs = True

            nmsg = ''
            if q_jobs:
                print
                print '* Host %s:%s is up' % (host, str(port))
                print msg
                for _ in xrange(n_reps):
                    os.system(' '.join(argv[1:]))
                print '* Done submitting %d' % n_reps
            else:
                if n_threads_all >= n_threshold:
                    nmsg = ' | too many user threads (%d >= %d)' % \
                        (n_threads_all, n_threshold)
                elif n_threads_notrun >= n_threshold_sysload:
                    nmsg = ' | too many waiting threads (%d >= %d)' % \
                        (n_threads_notrun, n_threshold_sysload)
                elif n_threads_everyone >= n_cpus - n_reserved_min:
                    nmsg = ' | high system load (%d >= %d)' % \
                        (n_threads_everyone, n_cpus - n_reserved_min)

            if q_jobs or verbose > 0:
                p_user = 100. * n_threads_all / n_cpus
                p_everyone = 100. * n_threads_everyone / n_cpus
                print '* System load: user=%d/%d=%5.2f%% ' \
                    '(norun=%d) ' \
                    'total=%d/%d=%5.2f%%%s' % (
                        n_threads_all, n_cpus, p_user,
                        n_threads_notrun,
                        n_threads_everyone, n_cpus, p_everyone,
                        nmsg)

        except Exception as e:
            print '*** Unknown error:', e
        time.sleep(3)

if __name__ == '__main__':
    main(sys.argv)
