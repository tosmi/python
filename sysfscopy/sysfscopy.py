""":Synopsis: copy a given sys tree without following links. Links
are actually converted to simple directories or file.

**Source Code:** `sysfscopy.py`

---------------

This is the :mod:`sysfscopy` module.
"""

import shutil
import os

class SysfsCopyFailedException(Exception):
    def __init__(self):
        pass

class SysfsCopy(object):
    def __init__(self):
       self.maxdepth = 1
       self._currdepth = 0

    def sysfscopy(self,source, destination):
        self._makedir(destination)

        for filename in os.listdir(source):
            fqfilename = os.path.join(source, filename)
            if os.path.isdir(fqfilename):
                    src_dir = os.path.join(source, filename)
                    dest_dir = os.path.join(destination, filename)
                    os.mkdir(dest_dir)
                    self._copy_directory(source, destination)
                    if self._currdepth < self.maxdepth:
                        self._currdepth+=1
                        self.sysfscopy(src_dir, dest_dir)
            elif os.path.isfile(fqfilename):
                self._copyfile(fqfilename, destination)

        self._currdepth -= 1

    def _makedir(self, destination):
        try:
            os.makedirs(destination)
        except OSError, e:
            print e

    def _copy_directory(self, source, destination):
        pass

    def _copyfile(self, source,destination):
        try:
            shutil.copy(source, destination)
        except IOError, e:
            filename = os.path.basename(source)
            dest_filename = os.path.join(destination, filename)
            print dest_filename
            self._writefile(dest_filename, e)

    def _writefile(self, destination, e):
        fhandle = open(destination, 'w')
        fhandle.write(e.strerror)
        fhandle.close()
