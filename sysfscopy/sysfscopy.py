""":Synopsis: Copy a given sysfs tree


**Source Code:** `sysfscopy.py`

---------------

This is the :mod:`sysfscopy` module. It copies a given sysfs tree without
following links. Links are actually converted to simple directories or
file.

"""
import shutil
import os

class SysfsCopy(object):
    """copy files an directories up to a certain depth. it's main use is
    to copy parts of sysfs as there are filesystem loop within sysfs.
    """
    def __init__(self):
       self.maxdepth = 1
       self._currdepth = 0

    def sysfscopy(self,source, destination, maxdepth=1):
        """this is a recursive function. self._copy_directory
        calls sysfscopy with a sub directory of source and increments
        self._currdepth.

        we have to pass source and destination around because of the
        recursion. do not make source and destination attributes of
        this class!
        """
        self.maxdepth = maxdepth

        if not os.path.isdir(destination):
            self._makedir(destination)
        for filename in os.listdir(source):
            self._copy(source, destination, filename)
        self._currdepth -= 1

    def _makedir(self, destination):
        os.makedirs(destination)

    def _copy(self, source, destination, filename):
        fqfilename = os.path.join(source, filename)
        if os.path.isdir(fqfilename):
            self._copy_directory(source, destination, filename)
        elif os.path.isfile(fqfilename):
            self._copy_file(source, destination, fqfilename)

    def _copy_directory(self, source, destination, dirname):
        src_dir = os.path.join(source, dirname)
        dest_dir = os.path.join(destination, dirname)
        os.mkdir(dest_dir)
        if self._currdepth < self.maxdepth:
            self._currdepth+=1
            self.sysfscopy(src_dir, dest_dir)

    def _copy_file(self, source, destination, filename):
        try:
            shutil.copy(filename, destination)
        except IOError, e:
            filename = os.path.basename(source)
            dest_filename = os.path.join(destination, filename)
            self._writefile(dest_filename, e)

    def _writefile(self, destination, e):
        fhandle = open(destination, 'w')
        fhandle.write(e.strerror)
        fhandle.close()
