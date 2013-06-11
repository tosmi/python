#!/bin/env python

""":Synopsis: expire postgresql backups and wals

**Source Code:** `expirebackup.py`

---------------

"""
import sys
import os
import logging
import logging.handlers
import fnmatch
import re

from datetime import datetime
from optparse import OptionParser

logger = logging.getLogger('expirebackup')

class PgsqlBackup(object):
    """parses a wal.backup file

    example input:

    START WAL LOCATION: 2/54000020 (file 000000010000000200000054)
    STOP WAL LOCATION: 2/540000A8 (file 000000010000000200000054)
    CHECKPOINT LOCATION: 2/54000020
    BACKUP METHOD: pg_start_backup
    BACKUP FROM: master
    START TIME: 2013-06-11 08:35:29 CEST
    LABEL: data/bck_1370932528
    STOP TIME: 2013-06-11 08:35:46 CEST
    """
    def __init__(self, data=None):
        self.label = None
        self.stop_wal = None
        self.stop_time = None

        [ self.parse_data( l.strip() ) for l in data]

        logger.debug('found wal data, stop_wal=%s, stop_time=%s, label=%s' % (self.stop_wal, self.stop_time, self.label))

    def __str__(self,):
        str = ""
        for key in self.__dict__:
            str += "%s: %s\n" % (key, self.__dict__[key])
        return str

    def parse_data(self, line):
        if 'STOP WAL' in line:
            m = re.search('file\s(?P<file>\d+)',line)
            self.stop_wal = m.group('file')
        elif 'STOP TIME' in line:
            m = re.search('TIME:\s(?P<datetime>.*$)', line)
            self.stop_time = datetime.strptime(m.group('datetime'), "%Y-%m-%d %H:%M:%S %Z")
        elif 'LABEL' in line:
            m = re.search('LABEL:\s(?P<label>.*$)', line)
            self.label = m.group('label')


def setup_logging(options=None):
    """setup our log infrastructure, of logfile is None or we are not
    able to open the log, revert to stdout
    """
    MAXLOGFILESIZE = 2097152
    BACKUPCOUNT    = 1

    defaultLogHandler = None
    logFormat         = logging.Formatter("%(asctime)s: %(name)s: %(levelname)s - %(message)s")

    streamLogHandler = logging.StreamHandler(sys.stdout)
    streamLogHandler.setFormatter(logFormat)
    logger.addHandler(streamLogHandler)

    if not options.stdout:
        logfile = '/'.join([sys.path[0], '../log/expirebackup.log'])
        try:
            defaultLogHandler = logging.handlers.RotatingFileHandler(logfile, maxBytes=MAXLOGFILESIZE, backupCount=BACKUPCOUNT)
            defaultLogHandler.setFormatter(logFormat)
        except IOError, (errno, strerror):
            logger.warning("Could not openlogfile %s, using STDOUT " % (logfile) )
        else:
            logger.removeHandler(streamLogHandler)
            logger.addHandler(defaultLogHandler)

    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


def parse_args():
    """parse any given command line arguments.
    """
    parser = OptionParser()
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                      help="enable debug output")
    parser.add_option("--stdout", dest="stdout", action="store_true")
    parser.add_option("-w","--warch", dest="warch",
                      help="archived wal logs location")
    parser.add_option("-k", "--keepdays", dest="keepdays",
                      help="how long should we keep backups in days")
    (options, args) = parser.parse_args()
    return options

def get_warch_location(options):
    warch_location = None
    if options.warch:
        warch_location = options.warch
    elif 'PGBCK' in os.environ:
        warch_location = os.environ['PGBCK']

    if warch_location is None:
        logger.error('Could not determine warch location, please set PGPBCK or use -w!')
        sys.exit(1)
    else:
        logger.debug("warch location is %s" % (warch_location))

    return warch_location

def find_files(directory, pattern):
    """return a generator of all files in directory matching pattern
    stolen from http://www.dabeaz.com/generators/
    """
    for path, dirlist, filelist in os.walk(directory):
        for name in fnmatch.filter(filelist,pattern):
            yield os.path.join(path,name)

def open_files(filenames):
    """open filenames an return a file object.
    stolen from http://www.dabeaz.com/generators/
    """
    for name in filenames:
        yield open(name)

def gen_backups(files):
    for f in files:
        data = f.readlines()
        b = PgsqlBackup(data)
        yield b

def find_expire(backups, keep_days):
    from datetime import timedelta

    expire = None
    delta  = timedelta( int(keep_days) )
    now    = datetime.now()

    tmp = None
    ptmp = None
    for b in backups:
        logger.debug('is the backup (%s) smaller as now - delta (%s)' % (b.stop_time, now - delta))
        if b.stop_time < now - delta:
            tmp = b
        if tmp is not None:
            if ptmp is not None:
                if tmp.stop_time < ptmp.stop_time:
                    tmp = ptmp
                else:
                    ptmp = tmp

    print expire
    return expire

def remove_backups(expire):
    print expire

def expire_backups(options):
    """search for .backup files in the wal log archive folder. expires
    (deletes) all backups and archived wals older than n days.
    """
    warch_dir    = get_warch_location(options)
    arch_wal_dir = '/'.join([warch_dir, 'wal'])
    bck_dir      = '/'.join([warch_dir, 'data'])
    keep_days    = None

    if 'PGBCK_KEEP_DAYS' in os.environ:
        keep_days = os.environ['PGBCK_KEEP_DAYS']
    elif options.keepdays:
        keep_days = options.keepdays
    else:
        logger.error('You must specify how long we should keep backups, either via PGBCK_KEEP_DAYS or -k!')
        sys.exit(1)

    logger.debug("archived wal location is %s, backup dir is %s" % (arch_wal_dir, bck_dir))
    logger.debug("will keep backups older than %s days" % (keep_days))

    bck_files_names = find_files(arch_wal_dir, '*.backup')
    bck_files       = open_files(bck_files_names)
    backups         = gen_backups(bck_files)
    expire          = find_expire(backups, keep_days)

    remove_backups(expire)


if __name__ == '__main__':
    options = parse_args()
    setup_logging(options=options)

    logger.debug('expirebackup startet')
    expire_backups(options)
