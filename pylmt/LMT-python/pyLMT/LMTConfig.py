"""
#*****************************************************************************\
#*  $Id: Makefile,v 1.9 2013/04/22 auselton Exp $
#*****************************************************************************
#*  Copyright (C) 2013 The Regents of the University of California.
#*  Produced at Lawrence Berkeley National Laboratory (cf, DISCLAIMER).
#*  Written by Andrew Uselton <acuselton@lbl.gov> as part of LMT:
#*  Copyright (C) 2007-2010 Lawrence Livermore National Security, LLC.
#*  This module (re)written by Jim Garlick <garlick@llnl.gov>.
#*  UCRL-CODE-232438
#*
#*  This file is part of Lustre Monitoring Tool, version 2.
#*  Authors: H. Wartens, P. Spencer, N. O'Neill, J. Long, J. Garlick
#*  For details, see http://github.com/chaos/lmt.
#*
#*  LMT is free software; you can redistribute it and/or modify it under
#*  the terms of the GNU General Public License (as published by the Free
#*  Software Foundation) version 2, dated June 1991.
#*
#*  LMT is distributed in the hope that it will be useful, but WITHOUT
#*  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#*  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
#*  for more details.
#*
#*  You should have received a copy of the GNU General Public License along
#*  with LMT; if not, write to the Free Software Foundation, Inc.,
#*  59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.
#*****************************************************************************/
"""

import MySQLdb
from pyLMT import DEFAULT_CONFIG, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class LMTConfigError(Error):
    """
    Generic Error for problems with LMTConfig objects.
    """

class LMTConfigNoIndexError(LMTConfigError):
    """
    If we can't figure out the index and it was not provided then we
    don't know what to do. No default is appropriate, since connecting
    successfully to something other than inteded woulfd be a bad thing.
    """

class LMTConfigBadIndexError(LMTConfigError):
    """
    The index has to be with the range of file system configurations available.
    """

class LMTConfigMissingFieldError(LMTConfigError):
    """
    There is a missing field in the file system record being identified.
    """

#*******************************************************************************
def process_configuration(args):
    """
    args should have a config field that is an open file handle with
    info about connecting to LMT DBs for a variety of file systems.
    The args.fs specifies the 'dbname' as it appears in the config file.
    If there is no args.fs then args.index can identify the file
    system of interest.
    On success return a dict with info for connecting to the
    specific file system of interest.
    """
    if args.config is None:
        args.config = open(DEFAULT_CONFIG)
    config = LMTConfig(args.config)
    return(config.filesystem(index=args.index, name=args.fs))

#*******************************************************************************
# Begin class LMTConfig
class LMTConfig:
    """
    Container class for configuration info for the LMT DB.
    """
    def __init__(self, cfgFile):
        self.Debug = False
        self.DebugMessages = None
        self.ErrorMessages = None
        self.Filesystems = []
        self.conn = None
        if type(cfgFile) is str:
            cfgFile = open(cfgFile)
        for line in cfgFile:
            eq = line.find('=')
            if eq >= 0:
                db_string = line[:eq]
                val = line[eq+1:]
                if val[-1:] == '\n':
                    val = val[:-1]
                db_tup = db_string.split('.')
                if (len(db_tup) == 3) and (db_tup[0] == 'filesys'):
                    try:
                        db_num = int(db_tup[1])
                    except:
                        break
                    while len(self.Filesystems) < db_num + 1:
                        self.Filesystems.append({'num' : len(self.Filesystems)})
                    self.Filesystems[db_num][db_tup[2]] = val
        # In some cases the config file gets parsed more than once, eg.
        # if we're looking at Bulk data and Metadata in the same applicaiton.
        cfgFile.seek(0)

    def debug(self, module=None):
        if (module is None) or (module == "LMTConfig"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def filesystem(self, index=1, name=None):
        if not name is None:
            index = None
            for i in range(len(self.Filesystems)):
                if ('dbname' in self.Filesystems[i]) and (name == self.Filesystems[i]['dbname']):
                    index = i
                    break
            if index == None:
                handleError(self,
                            LMTConfigNoIndexError,
                            "LMTConfig.filesystem(): ERROR - unable to identify filesystem %s among %d filesystem entries" % (name, len(self.Filesystems)))
                # not reached
        if index is None:
            # i.e. neither a name nor an index was provided, no defaults
            handleError(self,
                        LMTConfigNoIndexError,
                        "LMTConfig.filesystem(): ERROR - No filesystem specified")
            # not reached
        if (index < 0) or (index >= len(self.Filesystems)):
            handleError(self,
                        LMTConfigBadIndexError,
                        "LMTConfig.filesystem(): Error - No filesystem in config file at index %d" % index)
            # not reached
        fields = ['name', 'mountname', 'dbhost', 'dbport', 'dbuser', 'dbauth', 'dbname', 'host']
        fs = self.Filesystems[index]
        for field in fields:
            if not field in fs:
                handleError(self,
                            LMTConfigMissingFieldError,
                            "LMTConfig.filesystem(): Error - %s value missing " % field)
                # not reached
        try:
            fs['conn'] = MySQLdb.connect (host = fs['dbhost'],
                                          port = int(fs['dbport']),
                                          user = fs['dbuser'],
                                          passwd = fs['dbauth'],
                                          db = fs['dbname'])
        except MySQLdb.Error, e:
            handleError(self,
                        MySQLdb.Error,
                        "LMTConfig.filesystem(): Error - %d: %s" % (e.args[0], e.args[1]))
            # not reached
        return(fs)


    def show(self):
        for fs in self.Filesystems:
            self.show_fs(fs)

    def show_fs(self, fs):
        if fs == None:
            return
        for key,val in fs.iteritems():
            if type(val) == str:
                print "%s=%s" % (key, val)

# End of class LMTConfig
#*******************************************************************************

