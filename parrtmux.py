#!/usr/bin/env python

import os
import sys
import subprocess
import time

N_JOBS = int(os.environ.get('NJOBS', -1))
N_GPU = int(os.environ.get('NGPU', 2))
SLEEP = 0.6


def get_unique_cores():
    out = subprocess.check_output('cat /proc/cpuinfo | '
            'grep -e "core id" -e "physical id"', shell=True)
    out = out.split('\n')
    phyid = [int(e.split()[-1]) for e in out[0::2] if e.strip() != '']
    coreid = [int(e.split()[-1]) for e in out[1::2] if e.strip() != '']
    uid = zip(phyid, coreid)
    n = len(set(uid))
    return n


def get_tmux_windows():
    out = subprocess.check_output('tmux list-windows', shell=True)
    out = out.split('\n')
    wids = [int(e.split(':')[0]) for e in out if e.strip() != '']
    return max(wids), wids


def parrun_tmux(args, n_gpu=N_GPU):
    if len(args) == 1:
        print 'No commands given.'
        return
    cmd = ' '.join(args[1:])

    n_jobs = N_JOBS
    if n_jobs < 0:
        n_cores = get_unique_cores()
        n_jobs = n_cores + (n_jobs + 1)
    wid_max, _ = get_tmux_windows()

    print 'With: n_jobs=%d, n_gpu=%d, wid_max=%d' % (n_jobs, n_gpu, wid_max)
    print '-' * 50

    jobs = []
    for jid in xrange(n_jobs):
        wid = wid_max + jid + 1
        devgpu = 'device=gpu%d' % jid if jid < n_gpu else ''
        devgpu_comma = devgpu + ',' if devgpu != '' else ''
        theano_flags_cpu = 'THEANO_FLAGS="base_compiledir=~/.theano{jid}"'
        theano_flags_gpu = 'THEANO_FLAGS="***base_compiledir=~/.theano{jid}"'
        theano_flags_gpu = theano_flags_gpu.replace('***', devgpu_comma)
        # --
        job = cmd
        job = job.replace('{theano_flags_cpu}', theano_flags_cpu)
        job = job.replace('{theano_flags_gpu}', theano_flags_gpu)
        job = job.replace('{jid}', '%02d' % jid)
        job = job.replace('{devgpu}', devgpu)
        jobs.append((job, wid))
        print job

    print
    while True:
        ans = str(raw_input('Proceed? (y/n) ')).lower()
        if ans == 'n':
            print 'Aborted.'
            return
        elif ans == 'y':
            break

    for job, wid in jobs:
        os.system('tmux new-window -t :%d' % wid)
        time.sleep(SLEEP)
        os.system("tmux send-keys -t :%d '%s' C-m" % (wid, job))


if __name__ == '__main__':
    parrun_tmux(sys.argv)
