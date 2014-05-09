#!/usr/bin/env python

import os
import sys
from joblib import Parallel, delayed

N_JOBS = -1
THEANO_CPU = '${theano_flags_cpu}'


def worker(job, wid=0):
    theano_flags_cpu = 'THEANO_FLAGS="base_compiledir=~/.theano%02d"' % wid
    job = job.replace(THEANO_CPU, theano_flags_cpu)
    os.system(job)


def parrun(jobs, n_jobs=N_JOBS):
    Parallel(n_jobs=n_jobs, verbose=1)(delayed(worker)(j, i)
            for i, j in enumerate(jobs))


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print 'parrun: no joblist file given. reading from stdin.'
        f = sys.stdin
    else:
        f = open(sys.argv[1])

    n_jobs = N_JOBS
    if 'NJOBS' in os.environ:
        n_jobs = int(os.environ['NJOBS'])
        print '* n_jobs =', n_jobs

    jobs = []
    cmd = ''
    for line0 in f.readlines():
        line = line0.strip()
        if line[-1] == '\\':
            cmd += line[:-1] + ' '
        else:
            jobs.append(cmd + line)
            cmd = ''

    parrun(jobs, n_jobs=n_jobs)
