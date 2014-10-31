#!/usr/bin/env python

import os
import time
import subprocess
import sys
import socket

SCREEN_DEF = ['om_', ' R ', ' node0']
JOB_LEVEL_CMD = ['squeue', '-o',
                 '%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R   %C']
NCORES_CMD = 'sinfo -lNe'.split()

def get_total_threads(ncores_cmd=NCORES_CMD, screen=['om_all_nodes'],
                      idx_cpu=4, idx_node=1):
    output = subprocess.Popen(ncores_cmd,
                              stdout=subprocess.PIPE
                             ).communicate()[0]
    threads = [int(e.split()[idx_node]) * int(e.split()[idx_cpu])
               for e in output.split('\n') if
               all([s in e for s in screen])]
    return sum(threads)

def get_usage(job_level_cmd=JOB_LEVEL_CMD,
              screen=SCREEN_DEF, idx_cpu=-1):
    output = subprocess.Popen(job_level_cmd,
                              stdout=subprocess.PIPE
                             ).communicate()[0]
    cores = [int(e.split()[idx_cpu]) for e in output.split('\n') if
             all([s in e for s in screen])]
    return sum(cores)


def main(argv, def_user=os.environ.get('USER', ''), def_opt='-p'):
    user = def_user
    opt = def_opt

    if len(argv) == 1:
        pass
    elif len(argv) == 2:
        if argv[1] == '-n':
            opt = '-n'
            user = ''
        else:
            user = argv[1]
    elif len(argv) == 3:
        opt = argv[1]
        user = argv[2]
    else:
        print 'Incorrect options given.'
        return

    if opt == '-p':
        u_all = get_usage(screen=SCREEN_DEF)
        u_user = get_usage(screen=SCREEN_DEF + [user])
        n_total_thr = get_total_threads()
        print '%5d%5d%5d : %6.2f %6.2f' % (
            n_total_thr, u_all, u_user,
            100. * u_all / n_total_thr, 100. * u_user / n_total_thr)
    elif opt == '-n':
        print get_usage(screen=SCREEN_DEF + [user])
    else:
        print 'Option "%s" not understood.' % opt
        return

if __name__ == '__main__':
    main(sys.argv)
