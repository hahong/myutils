#!/usr/bin/env python

import os
import time
import subprocess
import sys

JOB_LEVEL_CMD = 'squeue -u hahong'
N_THRESHOLD = 50
N_REPS = N_THRESHOLD


def get_job_level(job_level_cmd=JOB_LEVEL_CMD, screen=['om_']):
    output = subprocess.Popen(job_level_cmd.split(),
            stdout=subprocess.PIPE).communicate()[0]
    return len([None for e in output.split('\n') if
        any([s in e for s in screen])])


def main(argv):
    n_threshold = N_THRESHOLD
    n_reps = N_REPS

    if 'NTHR' in os.environ:
        n_threshold = int(os.environ['NTHR'])
        print '* n_threshold =', n_threshold
    if 'NREPS' in os.environ:
        n_reps = int(os.environ['NREPS'])
        print '* n_reps =', n_reps

    while True:
        if get_job_level() <= n_threshold:
            print '* Below level'
            for _ in xrange(n_reps):
                os.system(' '.join(argv[1:]))
            print '* Done exec %d' % n_reps
        time.sleep(1)

if __name__ == '__main__':
    main(sys.argv)
