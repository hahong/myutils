#!/usr/bin/env python

import os
import time
import subprocess
import sys
import socket

SCREEN_DEF = ['om_', ' node0']
JOB_LEVEL_CMD = ['squeue', '-o',
                 '%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R   %C']

def get_total_cores(job_level_cmd=JOB_LEVEL_CMD,
                    screen=SCREEN_DEF, idx_cpu=-1):
    output = subprocess.Popen(job_level_cmd,
                              stdout=subprocess.PIPE
                             ).communicate()[0]
    cores = [int(e.split()[idx_cpu]) for e in output.split('\n') if
             all([s in e for s in screen])]
    return sum(cores)


def main(argv, def_user=''):
    user = def_user
    opt = None
    if len(argv) == 2:
        user = argv[1]
    elif len(argv) == 3:
        opt = argv[1]
        user = argv[2]
    else:
        print 'Incorrect options given.'
        return

    if opt == '-p':
        p_all = get_total_cores(screen=SCREEN_DEF)
        p_user = get_total_cores(screen=SCREEN_DEF + [user])
        print '%5d %5d %6.2f' % (p_all, p_user, 100. * p_user / p_all)
    else:
        print get_total_cores(screen=SCREEN_DEF + [user])


if __name__ == '__main__':
    main(sys.argv)
