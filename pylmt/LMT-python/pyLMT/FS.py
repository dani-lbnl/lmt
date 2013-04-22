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

from pyLMT import Timestamp, TimeSteps, Bulk, MDS, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class FSError(Error):
    """
    Generic Error for problems with FS objects.
    """

class FSNoConnectionError(FSError):
    """
    You need a MySQL connection to get any data
    """

class FSMissingParamsError(FSError):
    """
    Begin and end are Timestamp object, and must be present to
    specify the interval of interest.
    """

#*******************************************************************************
# Begin class FS
class FS():
    """
    Container class for FS I/O data from all OSSs for a file system. There will
    be an OSS object for each OSS in the file system as well as summary data.
    Hold the Steps object here as well.
    """

    def __init__(self, name):
        """
        OST and timestamp values all come in from the same query,
        and include many OSTs on many OSSs at once. We'll initialize empty data
        structures and fill them in incrementally.
        """
        self.name = name
        self.Debug = False
        self.DebugMessages = None
        self.DebugModules = {"Bulk":False, "MDS":False, "OSS":False, "OST":False,
                             "Timestamp":False, "TimeSteps":False}
        self.ErrorMessages = None
        self.conn = None
        # Here, and below can get clear_data()ed
        self.begin = None
        self.end = None
        self.Steps = None
        self.Bulk = Bulk.Bulk(name)
        self.MDS = MDS.MDS(fs=name)
        self.haveData = False
        self.total = 0

    def clear_data(self):
        self.begin = None
        self.end = None
        self.Steps = None
        self.Bulk = Bulk.Bulk(name)
        self.MDS = MDS.MDS(fs=name)
        self.haveData = False
        self.total = 0

    def debug(self, module=None):
        if (module == None) or (module == "FS"):
            self.Debug = not self.Debug
        if module == "Bulk":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "MDS":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "OSS":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "OST":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "Timestamp":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "TimeSteps":
            self.DebugModules[module] = not self.DebugModules[module]

    def getInfo(self, conn=None):
        """
        Get the list of OSSs and the metadata operations using the provided MySQL
        connection 'conn'.
        """
        if conn == None:
            if self.conn == None:
                handleError(self,
                            FSNoConnectionError,
                            "FS.getInfo(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        self.Bulk.getOSSs(conn)
        self.MDS.opsFromDB(conn)
        return

    def setSteps(self, Steps):
        self.Steps = Steps
        self.begin = self.Steps.begin
        self.end = self.Steps.end
        self.Bulk.setSteps(Steps)
        self.MDS.setSteps(Steps)

    def getData(self):
        """
        Get data from the MySQL connection 'conn' for the interval
        from Steps.begin to Steps.end. begin and end are themselves already
        Timestamp objects, and are required.
        """
        if self.Steps is None:
            print "FS.getData(): Error - You must supply a TimeSteps oject first"
            return
        if self.conn == None:
            handleError(self,
                        FSNoConnectionError,
                        "FS.getData(): Error - Please provide a MySQL connection")
            # not reached
        if self.Debug == True:
            self.DebugMessages += "FS.getData(): get data from %d/%d to %d/%d" % (self.Steps.begin.sie, self.Steps.begin.ts_id, self.Steps.end.sie, self.Steps.end.ts_id)
        self.Bulk.getData()
        self.MDS.getData()
        return

    def getQuickData(self, conn=None):
        """
        Get data from the MySQL connection 'conn' for the interval
        from begin to end. begin and end are themselves already
        Timestamp objects, and are required.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            FSNoConnectionError,
                            "FS.getQuickData(): Error - Please provide a MySQL connection")
                # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "FS.getQuickData(): get data from %d/%d to %d/%d" % (self.Steps.begin.sie, self.Steps.begin.ts_id, self.Steps.end.sie, self.Steps.end.ts_id)
        self.Bulk.getQuickData(conn=conn)
        self.MDS.getQuickData(conn=conn)
        self.MDS.getCPU()
        return

    def getCPU(self):
        """
        The MDS objects get its CPU uitlization by inclding 'cpu' in the ops
        list in MDS.getData()
        """
        self.Bulk.getCPU()
        self.MDS.getCPU()
        self.haveData = True


# End of class FS
#*******************************************************************************
