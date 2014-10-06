#!/usr/bin/env python

import os
import sys
import SocketServer
import socket
import time
import Queue
import signal
import cPickle as pk
import hashlib
import random
from math import floor
try:
    from psutil import phymem_usage
except ImportError:
    print 'mparrun: psutil unavailable. disabling pmemlimit'

    # dummy function
    def phymem_usage():                   # noqa
        return [0.]

NPROC = -1
PORT = 32288
HOST = ''                                 # all available interfaces
BDIR = os.path.expanduser('~/.mparrun')
FTOK = BDIR + '/ftok'
SLIST = BDIR + '/default_spawn'
RLOCK = BDIR + '/mparrun_run_%s_%s'       # file-based run-lock mechanism
PMEMLIMIT = 80.
MAXDELAY = 3.
MAXCNT = 0                                # run jobs indefinitely
THEANO_CPU = '${theano_flags_cpu}'

# ---------------------------------------------------------------------------
Q = Queue.Queue()                         # job list holder
QD = Queue.Queue()                        # for estimated time
SEP = '+xx ||*-1)'
SELF_SHUTDOWN = True
EOJ = 'end-of-job'
PREFIX = 'mparrun'
ALARM = 10
TFMT = '%Y%m%d_%H%M%S'


# Server Part ----------------------------------------------------------------
class MyTCPHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        req = self.data = self.rfile.readline().strip()
        tokens = req.split(SEP)
        if len(tokens) <= 1:
            return
        t0, cmd = tokens[:2]

        # {{ command get:
        if cmd == 'get':
            # if all jobs are finished,
            if Q.empty():
                self.wfile.write(t0 + SEP + EOJ + SEP + '-9999')
                if server._end_notified:
                    return
                # end of job tidbits..
                print 'mparrun: end of job'
                server._end_notified = True
                if server._self_shutdown:
                    signal.alarm(ALARM)
                return

            # some diag messages
            n = Q.qsize()
            if n % 20 == 0:
                dt = time.time() - server._t0
                nd = QD.qsize()              # done tasks
                if nd == 0:
                    nd = 1
                v = float(dt) / float(nd)
                ls = v * n                   # left time in secs
                H = int(floor(ls / 3600.))
                lhs = ls % 3600
                M = int(floor(lhs / 60.))
                S = int(ls % 60)

                H0 = int(floor(dt / 3600.))
                lhs0 = dt % 3600
                M0 = int(floor(lhs0 / 60.))
                S0 = int(dt % 60)
                print 'mparrun: %d jobs left' \
                    ' (%d:%02d:%02d left, %d:%02d:%02d passed)' % \
                    (n, H, M, S, H0, M0, S0)

            # send the job
            jid, job = Q.get()
            self.wfile.write(t0 + SEP + job + SEP + str(jid))
            self.request.close()
            QD.put(0)   # for diagnostic
        # end of get }}


def handler(signum, frame):
    cleanup()


def cleanup():
    if server._server is None or server._closed:
        return
    print 'mparrun: shutting down...'
    server._server.server_close()
    print 'mparrun: all done.'
    server._closed = True


def server(jobs, port=PORT):
    for jid, job in enumerate(jobs):
        Q.put((jid, job))

    s = SocketServer.TCPServer((HOST, port), MyTCPHandler)
    server._server = s
    server._t0 = time.time()
    signal.signal(signal.SIGALRM, handler)

    check_basedir()
    hostname = os.uname()[1].replace(' ', '')
    ftok = FTOK
    # writedown the host address
    open(ftok, 'wt').write(hostname + '\n' + str(port))

    n = len(jobs)
    print 'mparrun: server started. ^C to stop.'
    print 'mparrun: total %d jobs.' % n
    try:
        s.serve_forever(poll_interval=1)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print 'mparrun: other exception:', e
    finally:
        print
        cleanup()

server._self_shutdown = SELF_SHUTDOWN
server._end_notified = False
server._closed = False
server._server = None
server._t0 = None


# Client Part -----------------------------------------------------------------
def client_worker(addr, port, active, rseed,
                  pmemlimit=PMEMLIMIT, maxcnt=MAXCNT, maxdelay=MAXDELAY):
    errs = []
    stats = []
    cnt = 0
    client_worker.active = active
    pid = os.getpid()
    s_pid = str(pid)
    hostname = os.uname()[1].replace(' ', '')
    if os.getenv('MPARRUN_THEANO_FLAGS') is not None:
        theano_flags_cpu = os.getenv('MPARRUN_THEANO_FLAGS')
    else:
        theano_flags_cpu = \
            'THEANO_FLAGS="base_compiledir=~/.theano%02d"' % rseed
    random.seed(rseed)

    signal.signal(signal.SIGINT, handler_client)
    signal.signal(signal.SIGTERM, handler_client)

    while True:
        if maxdelay > 0:
            # staggered start
            time.sleep(random.random() * maxdelay)
        terminated = False
        # check system %memory first
        while True:
            if phymem_usage()[-1] < pmemlimit:
                break
            if not os.path.exists(active):
                terminated = True
                break
            print 'mparrun: waiting memory...'
            time.sleep(5)

        if terminated or not os.path.exists(active):
            print 'mparrun: terminated.'
            break

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((addr, port))
        except:
            print 'mparrun: no response. job finished?'
            break

        # make request
        t0 = str(time.time())
        req = t0 + SEP + 'get\n'      # request
        xsend(req, sock)              # send

        # get the new job from the server
        try:
            received = xrecv(sock)
            sock.close()
        except:    # some lingering transactions. close the connection
            break

        # parse...
        tokens = received.split(SEP)
        if len(tokens) != 3:          # potentially corrupted.
            errs.append(('Not Enough Arguments', tokens))
            continue

        t, job, jid = tokens
        if job == EOJ:
            break
        if t != t0:                  # not matching time stamp. corrupted.
            print >>sys.stderr, 'mparrun: corrupted message:', tokens
            errs.append(('Invalid Time Stamp', tokens))
            continue

        # all parsed. run it!
        try:
            print 'mparrun: running (jid=%s): %s' % (jid, job)
            job = job.replace(THEANO_CPU, theano_flags_cpu)
            job = job.replace('${pid}', s_pid)
            job = job.replace('${node}', hostname)
            job = job.replace('${workerid}', str(rseed))
            print 'mparrun: final cmd:        %s' % job
            # v this is needed to ensure the stdout order
            sys.stdout.flush()
            errcode = os.system(job)
            hash = hashlib.md5(job).digest()
            stats.append((jid, errcode, hash))
        except Exception as e:
            errs.append(('Run-time Error', tokens, e))

        cnt += 1
        if maxcnt > 0 and cnt >= maxcnt:
            break

    return errs, stats
client_worker.active = None


def xsend(w, s):
    while len(w) > 0:
        ns = s.send(w)
        w = w[ns:]


def xrecv(s):
    res = ''
    while True:
        r = s.recv(4)
        if r == '':
            return res
        res += r


def handler_client(signum, frame):
    print 'Wait until all the threads terminated...'
    cleanup_client()


def cleanup_client():
    active = client_worker.active
    if active is not None and os.path.exists(active):
        try:
            os.unlink(active)
        except OSError:
            # ignore when multiple threads attempted to del the file
            print 'mparrun: cleaned up.'
            pass


def client(addr, port=PORT, nproc=NPROC, prefix=PREFIX,
           pmemlimit=PMEMLIMIT, maxcnt=MAXCNT, maxdelay=MAXDELAY):
    from joblib import Parallel, delayed
    print 'mparrun: server =', addr

    signal.signal(signal.SIGINT, handler_client)
    signal.signal(signal.SIGTERM, handler_client)

    check_basedir()
    hostname = os.uname()[1].replace(' ', '')
    starttime = time.strftime(TFMT)
    active = RLOCK % (hostname, starttime)
    open(active, 'wt').close()    # create a run-lock file.
    client_worker.active = active

    if nproc < 0:
        nproc = detectCPUs()
    res = Parallel(n_jobs=nproc, verbose=0)(delayed(client_worker)(
        addr, port, active, i,
        pmemlimit=pmemlimit, maxcnt=maxcnt, maxdelay=maxdelay)
        for i in xrange(nproc))
    endtime = time.strftime(TFMT)

    # extract some error info
    err_found = False
    for r in res:
        if len(r[0]) > 0:
            err_found = True
            break
    if err_found:
        dmpfn = prefix + '_dmp_' + endtime + '.pk'
        dmp = open(dmpfn, 'wb')
        print 'mparrun: there were warnings/errors. dumping...', dmpfn
        pk.dump([r[0] for r in res], dmp)
        dmp.close()

    # dump stats: extract other task info
    dmpfn = prefix + '_stat_' + hostname + '_' + endtime + '.pk'
    dmp = open(dmpfn, 'wb')
    pk.dump([r[1] for r in res], dmp)
    dmp.close()

    # cleanup and exit
    cleanup_client()
    print 'mparrun: finished.'

    return int(err_found)


# Verification Part -----------------------------------------------------------
def verify(src_files, stat_files, cmdprn=False):
    print 'mparrun: verify stat files.'

    n = 0
    # if cmdprn:
    #     jobs = []
    #     for f in src_files:
    #         jobs.extend([j.strip() for j in open(f).readlines()])
    #     n = len(jobs)
    # else:
    #     for f in src_files:
    #         n += len(open(f).readlines())
    jobs = []
    for f in src_files:
        cmd = ''
        for line0 in open(f).readlines():
            line = line0.strip()
            if line[-1] == '\\':
                cmd += line[:-1] + ' '
            else:
                jobs.append(cmd + line)
                cmd = ''
    n = len(jobs)
    jid_all = set(range(n))

    badfound = False
    for f in stat_files:
        stat = pk.load(open(f))
        for worker in stat:
            for jid0, errcode, hash in worker:
                jid = int(jid0)
                if errcode != 0:
                    print '* %s: jid=%d, errcode=%d' % (f, jid, errcode)
                    if cmdprn:
                        print >>sys.stderr, jobs[jid]
                    badfound = True
                if jid not in jid_all:
                    print '* %s: duplicated jid=%d' % (f, jid)
                    badfound = True
                jid_all.remove(jid)

    jid_left = list(jid_all)
    if len(jid_left) > 0:
        print 'mparrun: unfinished jobs:', jid_left
        if cmdprn:
            for jid in jid_left:
                print >>sys.stderr, jobs[jid]
        badfound = True

    if not badfound:
        print 'mparrun: all seems okay.'
    else:
        print
        print 'mparrun: done.'


# Client Spawing Part --------------------------------------------------------
def spawn(all_args, slist=SLIST, prefix=PREFIX, dry=False, exclude=None,
          runcmd=False, redir=True):
    from joblib import Parallel, delayed
    import string as ss

    cwd = os.getcwd()
    home = os.path.expanduser('~')
    wd = os.path.relpath(cwd, home)
    jobs = []

    if runcmd:
        nopass = ['r', '--n', '--redir']
    else:
        nopass = ['cs', '--n']
    nopass_opts = ['--slist=', '--exclude=']
    passargs = [a for a in all_args if a.lower() not in nopass and
                all([a[:len(n)] != n for n in nopass_opts])]

    print 'mparrun: creating jobs:'
    for node_info0 in open(slist).readlines():
        ninfo = node_info0.strip().split()
        node = ninfo[0]
        # if node is in the excluded list, skip it.
        if exclude is not None and any([e in node for e in exclude]):
            continue

        # -- process commands
        nodeargs, nodeopts = parse_opts(ninfo[1:], optpx='++')
        # arguemnts to be passed
        if runcmd:
            args = ss.join(passargs)
        else:
            args = ss.join(nodeargs + passargs)

        # process options
        cd = wd
        if 'cd' in nodeopts:
            cd = nodeopts['cd']
        pre_cmd = ''
        if 'rhome' in nodeopts:
            pre_cmd += 'cd %s && ' % nodeopts['rhome']

        # make cmd
        starttime = time.strftime(TFMT)
        stdout = prefix + '_stat_' + node + '_' + starttime + '.stdout.log'
        stderr = prefix + '_stat_' + node + '_' + starttime + '.stderr.log'
        after_cmd = ' && echo "mparrun: finished: %s"' % node

        if runcmd:
            cmd_run = args
            if redir:
                cmd_body = "ssh -i ~/.ssh/id_rsa_ba %s '. ~/.bash_profile" + \
                    " && %s cd %s && %s 2> %s > %s %s'"
                cmd = cmd_body % (node, pre_cmd, cd, cmd_run,
                                  stderr, stdout, after_cmd)
            else:
                cmd_body = "ssh -i ~/.ssh/id_rsa_ba %s '. ~/.bash_profile" + \
                    " && %s cd %s && %s'"
                cmd = cmd_body % (node, pre_cmd, cd, cmd_run)
        else:
            cmd_run = 'mparrun.py c ' + args
            cmd_body = "ssh -i ~/.ssh/id_rsa_ba %s '. ~/.bash_profile" + \
                " && %s cd %s && %s 2> %s > %s %s'"
            cmd = cmd_body % (node, pre_cmd, cd, cmd_run,
                              stderr, stdout, after_cmd)
        print '  ', cmd
        jobs.append(cmd)

    if not dry:
        print 'mparrun: running jobs...'
        Parallel(n_jobs=len(jobs), verbose=0)(delayed(os.system)(j)
                                              for j in jobs)
    else:
        print 'mparrun: DRY-RUN'
    print 'mparrun: all finished.'


# Housekeeping Part ----------------------------------------------------------
def detectCPUs():
    # Linux, Unix and MacOS:
    if hasattr(os, "sysconf"):
        if 'SC_NPROCESSORS_ONLN' in os.sysconf_names:
            # Linux & Unix:
            ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
            if isinstance(ncpus, int) and ncpus > 0:
                return ncpus
    else:  # OSX
        return int(os.popen2("sysctl -n hw.ncpu")[1].read())
    # Windows:
    if 'NUMBER_OF_PROCESSORS' in os.environ:
        ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
        if ncpus > 0:
            return ncpus
    return 1


def check_basedir(bdir=BDIR):
    if bdir == '':
        return
    if not os.path.exists(bdir):
        os.makedirs(bdir)


def parse_opts(tokens, optpx='--'):
    opts0 = []
    args = []
    opts = {}
    n = len(optpx)

    for token in tokens:
        if token[:2] == optpx:
            opts0.append(token[n:])
        else:
            args.append(token)

    for opt in opts0:
        parsed = opt.split('=')
        key = parsed[0].strip()
        if len(parsed) > 1:
            cmd = parsed[1].strip()
        else:
            cmd = ''
        opts[key] = cmd

    return args, opts


def main():
    exitcode = 0
    if len(sys.argv) < 2:
        print 'Server mode:'
        print 'mparrun.py s <joblist file> [options]'
        print '--port=#       set the port number'
        print
        print 'Client mode:'
        print 'mparrun.py c [server address] [options]'
        print '--nproc=#      set the number of worker processes'
        print '--port=#       set the port number'
        print '--pmemlimit=#  set the %memory limit (0 < # < 100)'
        print '--prefix=str   output info file prefix (= --px)'
        print '--maxdelay=#   maximum delay in staggered job starting'
        print '--maxcnt=#     exit after running # jobs'
        print
        print 'Spawn mode:'
        print 'mparrun.py cs [options]'
        print '--slist=str    path to the spawning list'
        print '--exclude=str  comma separted server list to exlcuded'
        print '--n            dry-run'
        print '(all client options are supported/passed)'
        print
        print 'Simple spawn running mode:'
        print 'mparrun.py r <command>'
        print '--slist=str    path to the spawning list'
        print '--exclude=str  comma separted server list to exlcuded'
        print '--redir        redirect stdout and stderr'
        print '--n            dry-run'
        print
        print 'Verify mode:'
        print 'mparrun.py v <joblist.sh> <stat.1.pk> [stat.2.pk] ...'
        print '--cmdprn       print the failed commands to stderr'
        return 0

    # parse options and arguments
    args, opts = parse_opts(sys.argv[1:])

    # -- do the work
    # detect the options
    if 'chdir' in opts:
        cd = opts['chdir']
        os.chdir(cd)
        print 'mparrun: cd to', cd

    nproc = NPROC
    if 'nproc' in opts:
        nproc = int(opts['nproc'])
        print 'mparrun: nproc =', nproc

    pmemlimit = PMEMLIMIT
    if 'pmemlimit' in opts:
        pmemlimit = float(opts['pmemlimit'])
        print 'mparrun: pmemlimit =', pmemlimit

    port = PORT
    if 'port' in opts:
        port = int(opts['port'])
        print 'mparrun: port =', port

    maxcnt = MAXCNT
    if 'maxcnt' in opts:
        maxcnt = int(opts['maxcnt'])
        print 'mparrun: maxcnt =', maxcnt

    maxdelay = MAXDELAY
    if 'maxdelay' in opts:
        maxdelay = float(opts['maxdelay'])
        print 'mparrun: maxdelay =', maxdelay

    prefix = PREFIX
    if 'prefix' in opts:
        prefix = os.path.expanduser(opts['prefix'])
        print 'mparrun: prefix =', prefix
    elif 'px' in opts:
        prefix = os.path.expanduser(opts['px'])
        print 'mparrun: prefix =', prefix

    slist = SLIST
    if 'slist' in opts:
        slist = os.path.expanduser(opts['slist'])
        print 'mparrun: slist =', slist

    exclude = None
    if 'exclude' in opts:
        exclude = opts['exclude'].split(',')
        print 'mparrun: exclude =', exclude

    dry = False
    if 'n' in opts:
        dry = True

    redir = False
    if 'redir' in opts:
        redir = True

    cmdprn = False
    if 'cmdprn' in opts:
        cmdprn = True

    # detect the mode
    if len(args) == 0:
        print 'mparrun: assuming client mode'
        args.append('c')
    mode = args[0]

    # invoke server
    if mode.lower() in ['s', 'server']:
        if len(args) < 2:
            print 'mparrun: joblist file required.'
            return 1
        f = open(args[1])
        # jobs = [line.strip() for line in f.readlines()]
        jobs = []
        cmd = ''
        for line0 in f.readlines():
            line = line0.strip()
            if line[-1] == '\\':
                cmd += line[:-1] + ' '
            else:
                jobs.append(cmd + line)
                cmd = ''
        server(jobs, port=port)

    # or, invoke client
    elif mode.lower() in ['c', 'client']:
        if len(args) > 1:
            addr = args[1]
        else:
            ftok = FTOK
            f = open(ftok)
            addr = f.readline().strip()
            port = int(f.readline().strip())
        exitcode = client(addr, port, nproc, prefix,
                          pmemlimit=pmemlimit, maxcnt=maxcnt,
                          maxdelay=maxdelay)

    # or, spawn clients
    elif mode.lower() == 'cs':
        spawn(sys.argv[1:], slist=slist, prefix=prefix, dry=dry,
              exclude=exclude)

    # or, spawn running mode
    elif mode.lower() == 'r':
        spawn(sys.argv[1:], slist=slist, prefix=prefix, dry=dry,
              exclude=exclude, runcmd=True, redir=redir)

    # or, verify
    elif mode.lower() in ['v', 'verify']:
        verify(args[1:2], args[2:], cmdprn=cmdprn)

    # unknown mode
    else:
        print 'mparrun: unknown mode.'

    return exitcode


if __name__ == '__main__':
    exitcode = main()
    sys.exit(exitcode)
