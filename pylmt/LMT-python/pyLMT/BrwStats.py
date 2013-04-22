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

from pyLMT import Timestamp, HistBins, TimeSteps, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class BrwStatsError(Error):
    """
    Generic Error for problems with BrwStats objects.
    """

class BrwStatsQueryError(BrwStatsError):
    """
    Something went wrong while trying to query the DB.
    """

class BrwStatsNoConnectionError(BrwStatsError):
    """
    We don't seem to have a MySQL conneciton.
    """

class BrwStatsQueryBrwStatsError(BrwStatsError):
    """
    We didn't get anything back from the query.
    """


class BrwStatsNoDataError(BrwStatsError):
    """
    We didn't get anything back from the query.
    """

class BrwStatsNoStepsError(BrwStatsError):
    """
    There should always be a Steps object in place.
    """

class BrwStatsNoBinsError(BrwStatsError):
    """
    There should always be a Bins object in place for each histogram type.
    """

BrwStatsNames = [
    "BRW_RPC",
    "BRW_DISPAGES",
    "BRW_DISBLOCKS",
    "BRW_FRAG",
    "BRW_FLIGHT",
    "BRW_IOTIME",
    "BRW_IOSIZE"
    ]

#*******************************************************************************
# Begin class BrwStats
class BrwStats():
    """
    The BRW_STATS_DATA table has rows that gather histogram values
    for seven different histograms for each of read RPCs and write
    RPCs. They are also broken out by <ost name>.
    """

    def __init__(self, name):
        """
        OST, histogram type, bin, read, write, and timestamp values all
        come in from the same query,and include many BrwStats at once.
        We'll initialize empty data structures and fill them in
        incrementally.
        The 'name' is the name of the OST for which we are getting
        histogram stats.
        """
        self.name = name
        self.Debug = False
        self.DebugMessages = None
        self.DebugModules = {"Timestamp":False,
                             "TimeSteps":False,
                             "HistBins":False,
                             "HistSeries":False}
        self.ErrorMessages = None
        self.conn = None
        # Here, and below can get clear_data()ed
        self.begin = None
        self.end = None
        # There will be a separate Bins entry (HistBins object) for each
        # histogram type, and they are identified by the BrwStatsDict.
        self.Bins = []
        self.BrwStatsDict = {}
        # There are read and write histograms for each BrwStatsNames
        # entry and indexed by the string representation of the STATS_ID
        self.Read = {}
        self.Write = {}
        self.Steps = None
        self.haveData = False


    def clear_data(self):
        self.begin = None
        self.end = None
        # There will be a separate Bins entry (HistBins object) for each
        # histogram type, and they are identified by the BrwStatsDict.
        self.Bins = []
        self.BrwStatsDict = {}
        # There are read and write histograms for each BrwStatsNames
        # entry and indexed by the string representation of the STATS_ID
        self.Read = {}
        self.Write = {}
        self.Steps = None
        self.haveData = False

    def debug(self, module=None):
        if (module == None) or (module == "BrwStats"):
            self.Debug = not self.Debug
            self.Read.debug()
            self.Write.debug()
            self.BrwStats.debug()
        if module == "Timestamp":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "TimeSteps":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "HistBins":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "HistSeries":
            self.DebugModules[module] = not self.DebugModules[module]
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def showStep(self, hist, step):
        self.Read[hist].showStep(step)
        self.Write[hist].showStep(step)

    def register(self, hist, bin, sie, read, write):
        """
        All the sie values should alredy be registered with self.Steps
        """
        if self.Debug == True:
            print "BrwStats %s" % self.name
        if self.Steps == None:
            print "BrwStats.register(): Warning - registering BrwStats %s sie %d without a TimeSteps object" % (self.name, sie)
            return
        if self.Steps.steps() == 0:
            print "BrwStats.register(): Warning - registering BrwStats %s sie %d with zero length TimeSteps object" % (sel.name, sie)
            return
        self.Read[hist].register(bin, sie, read)
        self.Write[hist].register(bin, sie, write)

    def setBinsAndSteps(self, hist, Bins, Steps):
        # The steps are a universal sequence for the data, but each
        # histogram will have its own Bins sequence./
        self.Steps = Steps
        for data in (self.Read, self.Write):
            data[hist].setBinsAndSteps(Bins, Steps)

    def getBrwStats(self, conn=None):
        """
        Get the list of BrwStats using the provided MySQL
        connection 'conn'.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BrwStatsNoConnectionError,
                            "BrwStats.getBrwStats(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "BrwStats.getBrwStats(): get the list of BrwStats on %s" % self.name
        query = "SELECT * FROM BRW_STATS_INFO order by STATS_ID"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BrwStatsQueryBrwStatsError,
                        "BrwStats.getBrwStats: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwStatsQueryBrwStatsError,
                        "BrwStats.getBrwStats(): WARNING - No data")
            # not reached
        for row in rows:
            id = row["STATS_ID"]
            name = row["STATS_NAME"]
            description = row["DESCRIPTION"]
            self.BrwStatsDict[id] = len(self.Bins)
            Bins = HistBins.Histbins(id, name, description)
            if self.DebugModules["HistBins"] == True:
                Bins.debug()
            self.Bins.append(Bins)
            self.Read[id] = HistSeries.HistSeries(name, "count")
            self.Write[id] = HistSeries.HistSeries(name, "count")
        cursor.close()
        return

    def getData(self, begin, end, conn=None):
        """
        Get data from the MySQL connection 'conn' for the interval
        from begin to end. begin and end are themselves already
        Timestamp objects, and are required.
        """
        if (begin == None) or (end == None):
            print "BrwStats.getData(): Error - You must supply Timestamp ojects 'begin' and 'end'"
            return
        if conn == None:
            if self.conn == None:
                print "BrwStats.getData(): Error - Please provide a MySQL connection"
                return
        else:
            self.conn = conn
        if self.Debug == True:
            print "BrwStats.getData(): get data from %d/%d to %d/%d" % (begin.sie, begin.ts_id, end.sie, end.ts_id)
        self.end = end
        self.begin = begin
        query = "SELECT * FROM TIMESTAMP_INFO,BRW_STATS_DATA WHERE "
        query += "TIMESTAMP_INFO.TS_ID=BRW_STATS_DATA.TS_ID "
        query += "AND TIMESTAMP_INFO.TIMESTAMP >= '"
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
                        BrwStatsQueryError,
                        "BrwStats.getData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwStatsNoDataError,
                        "BrwStats.getData(): WARNING - No data")
            # not reached
        # First establish the set of bins for the various histograms
        # and the sequence of time steps involved
        # then distribute the observations to the various TimeSeries
        # All the BrwStats objects share this one TimeSteps object.
        self.Steps = TimeSteps.TimeSteps()
        if self.Debug == True:
            self.Steps.debug()
        for row in rows:
            self.Steps.examine(row['TIMESTAMP'], Timestamp.calc_sie(row['TIMESTAMP']), row['TS_ID'])
            self.Bins[self.BrwStatsDict[row['STATS_ID']]].examine(row['BIN'])
        self.Steps.register()
        if (self.Steps.steps() == 0):
            handleError(self,
                        BrwStatsNoStepsError,
                        "BrwStats.getData(): Warning - No steps")
            # not reached
        for Bins in self.Bins:
            Bins.register()
            if (Bins.bins() == 0):
                handleError(self,
                            BrwStatsNoBinsError,
                            "BrwStats.getData(): Warning - No bins for %s" % Bins.name)
                # not reached
            self.Read[Bins.id].setBinsAndSteps(Bins, Self.Steps)
            self.Write[Bins.id].setBinsAndSteps(Bins, Self.Steps)
        # Okay, now we have all the Steps and Bins organized
        # In the second phase we actually build up the arrays.
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
        n = self.Read.interpolate()
        if n == 0:
            handleError(self,
                        BrwStatsNoDataError,
                        "BrwStats.interpolate(): Warning - No Read data")
            # not reached
        n = self.Write.interpolate()
        if n == 0:
            handleError(self,
                        BrwStatsNoDataError,
                        "BrwStats.interpolate(): Warning - No Write data")
            # not reached
        n = self.BrwStats.interpolate()
        if n == 0:
            handleError(self,
                        BrwStatsNoDataError,
                        "BrwStats.interpolate(): Warning - No combined data")
            # not reached

    def stats(self):
        self.Read.stats()
        self.Write.stats()
        self.BrwStats.stats()

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
                        BrwStatsNoStepsError,
                        "BrwStats.report(): - Warning - No steps")
            # not reached
        print self.name
        self.header1()
        self.header2()
        for data in (self.Read, self.Write, self.BrwStats):
            if data == None:
                handleError(self,
                            BrwStatsNoDataError,
                            "BrwStats.report(): Warning - No data")
                # not reached
            if data.Stats.total == None:
                handleError(self,
                            BrwStatsNoStatsError,
                            "BrwStats.report(): - Warning - %s Stats not calculated" % data.name)
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


# End of class BrwStats
#*******************************************************************************
