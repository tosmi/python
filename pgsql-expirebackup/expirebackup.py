#!/bin/env python
"""expire postgresql backups and wals

This script searches for postgresql backup files and removes all files
older than PGBCK_KEEP_DAYS or the number of days specified with -k.

We search for all *.backup files in the archived wal directory and
create PgsqlBackup objects from the metadata in these files. Than we
search for the youngest PgsqlBackup object that is older than
PGBCK_KEEP_DAYS. finally we remove all full backups and archived wals
older than this PgsqlBackup object.
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
    """parses a wal.backup file and creates a PgsqlBackup object from
    the metadata. we store the following data:

    - `label`: the label of this backup
    - `stop_wal`: the wal file name when the backup stopped
    - `stop_time`: the time the backup stopped

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

        for line in data:
            self.parse_data( line.strip() )

        logger.debug('found wal data, stop_wal=%s, stop_time=%s, label=%s'
                     % (self.stop_wal, self.stop_time, self.label))

    def __str__(self,):
        str = ""
        for key in self.__dict__:
            str += "%s: %s\n" % (key, self.__dict__[key])
        return str

    def parse_data(self, line):
        """parses the line and set PgsqlBackup attribute accordingly.
        """
        if 'STOP WAL' in line:
            m = re.search(r'file\s(?P<file>\d+)', line)
            self.stop_wal = m.group('file')
        elif 'STOP TIME' in line:
            m = re.search(r'TIME:\s(?P<datetime>.*$)', line)
            self.stop_time = datetime.strptime(m.group('datetime'),
                                               "%Y-%m-%d %H:%M:%S %Z")
        elif 'LABEL' in line:
            m = re.search(r'LABEL:\s(?P<label>.*$)', line)
            self.label = m.group('label')


def setup_logging(options=None):
    """setup our log infrastructure, if --stdout is specified log to
    stdout, otherwise log to ../log/expirebackup.log. if opening the
    logfile fails log to stdout.
    """
    MAXLOGFILESIZE = 2097152
    BACKUPCOUNT    = 1

    defaultLogHandler = None
    logFormat         = logging.Formatter("%(asctime)s: %(name)s:%(levelname)s - %(message)s")

    streamLogHandler = logging.StreamHandler(sys.stdout)
    streamLogHandler.setFormatter(logFormat)
    logger.addHandler(streamLogHandler)

    if not options.stdout:
        logfile = '/'.join([sys.path[0], '../log/expirebackup.log'])
        try:
            defaultLogHandler = logging.handlers.RotatingFileHandler(logfile,
                                                                     maxBytes=MAXLOGFILESIZE,
                                                                     backupCount=BACKUPCOUNT)
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
    parser.add_option("-w", "--warch", dest="warch",
                      help="archived wal logs location")
    parser.add_option("-k", "--keepdays", dest="keepdays",
                      help="how long should we keep backups in days. overwrites PGBCK_KEEP_DAYS if set.")
    (options, args) = parser.parse_args()
    return options

def get_warch_location(options):
    """ return the archived wal location. if -w is specified check
    this directory, else use PGBCK. if both are missing exit 1. if the
    warch location is not a directory exit 1.
    """
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

    if not os.path.isdir(warch_location):
        logger.error('warch location %s is not a directory or inaccessible, exit!' % (warch_location) )
        sys.exit(1)

    return warch_location

def find_files(directory, pattern):
    """return a generator of all files in directory matching pattern.
    stolen from http://www.dabeaz.com/generators/
    """
    for path, dirlist, filelist in os.walk(directory):
        print (dirlist, pattern)
        for name in fnmatch.filter(filelist, pattern):
            yield os.path.join(path, name)

def find_directories(directory, pattern):
    """return a generator of all subdirectories in directory matching
    pattern.  stolen from http://www.dabeaz.com/generators/
    """

    for path, dirlist, filelist in os.walk(directory):
        for name in fnmatch.filter(dirlist, pattern):
            yield os.path.join(path, name)

def find_files_older(files, datetime):
    """return files older than datetime
    """
    for f in files:
        ftime = datetime.fromtimestamp(os.stat(f).st_mtime)
        if ftime < datetime:
            yield (f, ftime)


def open_files(filenames):
    """open filenames an return a file object.
    stolen from http://www.dabeaz.com/generators/
    """
    for name in filenames:
        yield open(name)

def gen_backups(files):
    """generates backup object from wal backup file content
    """
    for f in files:
        data = f.readlines()
        b = PgsqlBackup(data)
        yield b

def find_expire(backups, keep_days):
    """searches all backups found for the newest backup we
    should expire.

    the algorithm is as follows:

    1. iterate over all backups
    2. if the backup stop time is older than now() - delta then this is
       a candiate to expire
    3. if there is a previous backup to expire, check if the current backup
       is older than the previous
    4. if YES keep the previous backup
    5. if NO keep the current backup

    finally return the backup to expire
    """
    from datetime import timedelta

    expire = None
    delta  = timedelta( int(keep_days) )
    now    = datetime.now()

    tmp = None
    for b in backups:
        logger.debug('is the backup with label %s (%s) smaller as now - delta (%s)' % (b.label, b.stop_time, now - delta))
        if b.stop_time < now - delta:
            logger.debug('found backup %s to expire (%s)' % (b.label, b.stop_time, ))
            expire = b
        if tmp is not None:
            logger.debug('there is a previous backup %s to expire (%s)' % (b.label, b.stop_time, ))
            if expire.stop_time < tmp.stop_time:
                logger.debug('the current backup %s (%s) is older than the previous backup %s (%s), so we keep the previous one' % (expire.label, expire.stop_time, tmp.label, tmp.stop_time ))
                expire = tmp
        else:
            tmp = expire
    logger.debug('going to expire all backup data older than %s' % (expire.stop_time) )
    return expire

def remove_backups(backup, arch_wal_dir, bck_dir):
    """search for all files older than backup.stop_time in arch_wal_dir and bck_dir
    and remove them.
    """
    wal_files    = find_files(arch_wal_dir, '*')
    rm_wal_files = find_files_older(wal_files, backup.stop_time)
    for f, datetime in rm_wal_files:
        logger.debug('going to remove wal %s with date %s' % (f, datetime) )
        # os.unlink(f)

    bck_files = find_directories(bck_dir, '*bck*')
    rm_bck_files = find_files_older(bck_files, datetime )
    for f, datetime in rm_bck_files:
        logger.debug('going to remove full backup %s with date %s' % (f, datetime) )

def expire_backups(options):
    """search for .backup files in the wal log archive folder. expires
    (deletes) all backups and archived wals older than n days.
    """
    warch_dir    = get_warch_location(options)
    arch_wal_dir = '/'.join([warch_dir, 'wal'])
    bck_dir      = '/'.join([warch_dir, 'data'])
    keep_days    = None

    if options.keepdays:
        keep_days = options.keepdays
    elif 'PGBCK_KEEP_DAYS' in os.environ:
        keep_days = os.environ['PGBCK_KEEP_DAYS']
    else:
        logger.error('You must specify how long we should keep backups, either via PGBCK_KEEP_DAYS or -k!')
        sys.exit(1)

    logger.debug("archived wal location is %s, backup dir is %s" % (arch_wal_dir, bck_dir))
    logger.debug("will keep backups older than %s days" % (keep_days))

    bck_files_names = find_files(arch_wal_dir, '*.backup')
    bck_files       = open_files(bck_files_names)
    backups         = gen_backups(bck_files)
    expire          = find_expire(backups, keep_days)

    remove_backups(expire, arch_wal_dir, bck_dir)

if __name__ == '__main__':
    options = parse_args()
    setup_logging(options=options)

    logger.debug('expirebackup startet')
    expire_backups(options)
