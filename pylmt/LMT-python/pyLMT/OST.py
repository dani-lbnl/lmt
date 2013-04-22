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
import numpy as np
import numpy.ma as ma

from pyLMT import Counter, Timestamp, TimeSteps, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class OSTError(Error):
    """
    Generic Error for problems with OST objects.
    """

class OSTQueryError(OSTError):
    """
    Something went wrong while trying to query the DB.
    """

class OSTNoDataError(OSTError):
    """
    We didn't get anything back from the query.
    """

class OSTNoStepsError(OSTError):
    """
    There should always be a Steps object in place.
    """

#*******************************************************************************
# Begin class OST
class OST():
    """
    Container class for OST_INFO table rows. There will be a TimeSeries
    object for read, write, and both. Hold the Steps object here as well.
    """

    def __init__(self, name):
        """
        Read write and timestamp values all come in from the same query,
        and include many OSTs at once. We'll initialize empty data
        structures and fill them in incrementally.
        """
        self.name = name
        self.Debug = False
        self.DebugMessages = None
        self.DebugModules = {"Timestamp":False, "TimeSteps":False}
        self.ErrorMessages = None
        self.conn = None
        # Here, and below can get clear_data()ed
        self.begin = None
        self.end = None
        self.Read = Counter.Counter("read", "count/sec")
        self.Write = Counter.Counter("write", "count/sec")
        self.OST = Counter.Counter("OST", "count/sec")
        self.Steps = None
        self.Missing = None
        self.haveData = False
        self.total = 0

    def clear_data(self):
        self.begin = None
        self.end = None
        self.Read = Counter.Counter(self.name, "count/sec")
        self.Write = Counter.Counter(self.name, "count/sec")
        self.OST = Counter.Counter(self.name, "count/sec")
        self.Steps = None
        self.haveData = False
        self.total = 0

    def debug(self, module=None):
        if (module == None) or (module == "OST"):
            self.Debug = not self.Debug
            self.Read.debug()
            self.Write.debug()
            self.OST.debug()
        if module == "Timestamp":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "TimeSteps":
            self.DebugModules[module] = not self.DebugModules[module]
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def showStep(self, step):
        self.Read.showStep(step)
        self.Write.showStep(step)
        self.OST.showStep(step)

    def register(self, sie, read, write):
        """
        All the sie values should alredy be registered with self.Steps
        """
        if self.Debug == True:
            print "OST %s" % self.name
        if self.Steps == None:
            print "OST.register(): Warning - registering OST %s sie %d without a TimeSteps object" % (self.name, sie)
            return
        if self.Steps.steps() == 0:
            print "OST.register(): Warning - registering OST %s sie %d with zero length TimeSteps object" % (sel.name, sie)
            return
        self.Read.register(sie, read)
        self.Write.register(sie, write)
        self.OST.register(sie, read+write)

    def setSteps(self, Steps):
        self.Steps = Steps
        self.begin = self.Steps.begin
        self.end = self.Steps.end
        for data in (self.Read, self.Write, self.OST):
            data.setSteps(self.Steps)

    def getData(self, conn=None):
        """
        This supports accessing just the one OST, which is really
        only going to happen when this module is called with __main__().

        Get data from the MySQL connection 'conn' for the interval
        from begin to end. begin and end are themselves already
        Timestamp objects, and are required.
        """
        if self.Steps is None:
            print "OST.getData(): Error - You must supply a TimeSteps oject first"
            return
        if conn == None:
            if self.conn == None:
                print "OST.getData(): Error - Please provide a MySQL connection"
                return
        else:
            self.conn = conn
        if self.Debug == True:
            print "OST.getData(): get data from %d/%d to %d/%d" % (self.Steps.begin.sie, self.Steps.begin.ts_id, self.Steps.end.sie, self.Steps.end.ts_id)
        query = "SELECT * FROM OST_INFO,TIMESTAMP_INFO,OST_DATA WHERE "
        query += "OST_INFO.OST_ID=OST_DATA.OST_ID AND "
        query += "OST_INFO.OST_NAME='" + self.name + "' AND "
        query += "TIMESTAMP_INFO.TS_ID=OST_DATA.TS_ID AND TIMESTAMP_INFO.TIMESTAMP >= '"
        query += self.begin.timestr
        query += "' AND TIMESTAMP_INFO.TIMESTAMP <= '"
        query += self.end.timestr
        query += "' order by TIMESTAMP_INFO.TS_ID"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                print "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        OSTQueryError,
                        "OST.getData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        OSTNoDataError,
                        "OST.getData(): WARNING - No data")
            # not reached
        # Build up the arrays.
        for row in rows:
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            self.register(sie, float(row['READ_BYTES']), float(row['WRITE_BYTES']))
        cursor.close()
        self.interpolate()
        self.stats()
        self.haveData = True
        return

    def setData(self, begin, end):
        self.begin = begin
        self.end = end

    def interpolate(self):
        # N.B. The counter interpolate() fucntion also divides by
        # the step intervals, so the resulting series is a set of
        # observations of the true rate in MB/s.
        masked_vals = np.where(self.Read.Values.mask == True)
        self.Missing = np.zeros(len(self.Read.Values), dtype=np.int32)
        if ma.count_masked(self.Read.Values) != 0:
            self.Missing[masked_vals] = 1
        n = self.Read.interpolate()
        if n == 0:
            handleError(self,
                        OSTNoDataError,
                        "OST.interpolate(): Warning - No Read data")
            # not reached
        n = self.Write.interpolate()
        if n == 0:
            handleError(self,
                        OSTNoDataError,
                        "OST.interpolate(): Warning - No Write data")
            # not reached
        n = self.OST.interpolate()
        if n == 0:
            handleError(self,
                        OSTNoDataError,
                        "OST.interpolate(): Warning - No combined data")
            # not reached

    def stats(self):
        self.Read.stats()
        self.Write.stats()
        self.OST.stats()

    def header1(self):
        #print "%12s\t%6d\t%5.3e\t%6d\t%5.3e\t%5.3e\t%6d (%5.3e)\t%5.3e (%5.3e)"
        print "%12s\t%6s\t%8s\t%6s\t%8s\t%8s\t%6s (%8s)\t%8s (%8s)" % ("i/o",
                                                                       "#steps",
                                                                       "total",
                                                                       "max",
                                                                       "ave",
                                                                       "stdev",
                                                                       "#>ave",
                                                                       "frac",
                                                                       "tot>ave",
                                                                       "frac")
        print "%12s\t%6s\t%8s\t%6s\t%8s\t%8s\t%6s %8s \t%8s %8s " % ("",
                                                                     "count",
                                                                     "GiB",
                                                                     "MiB/sec",
                                                                     "MiB/sec",
                                                                     "MiB/sec",
                                                                     "count",
                                                                     "",
                                                                     "GiBytes>ave",
                                                                     "")

    def header2(self):
        print "%12s\t%6s\t%8s\t%6s\t%8s\t%8s\t%6s\t%8s" % ("------",
                                                           "------",
                                                           "------",
                                                           "------",
                                                           "------",
                                                           "------",
                                                           "-----------------",
                                                           "-----------------")

    def report(self):
        if self.Steps == None:
            handleError(self,
                        OSTNoStepsError,
                        "OST.report(): - Warning - No steps")
            # not reached
        print self.name
        self.header1()
        self.header2()
        for data in (self.Read, self.Write, self.OST):
            if data == None:
                handleError(self,
                            OSTNoDataError,
                            "OST.report(): Warning - No data")
                # not reached
            if data.Stats.total == None:
                handleError(self,
                            OSTNoStatsError,
                            "OST.report(): - Warning - %s Stats not calculated" % data.name)
                # not reached
            format = "%12s\t%6d\t%5.3e\t%6d\t%5.3e\t%5.3e\t%6d (%5.3e)\t%5.3e (%5.3e)"
            print format % (data.name,
                            data.Steps.steps(),
                            data.Stats.total/(1024*1024*1024),
                            data.Stats.max/(1024*1024),
                            data.Stats.ave/(1024*1024),
                            data.Stats.stdev/(1024*1024),
                            data.Stats.above,
                            float(data.Stats.above)/float(data.Steps.steps()),
                            data.Stats.totAbv/(1024*1024*1024),
                            data.Stats.totAbvFrac)


# End of class OST
#*******************************************************************************
