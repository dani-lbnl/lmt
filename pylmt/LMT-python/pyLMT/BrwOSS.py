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

from pyLMT import Counter, HistBins, HistSeries, BrwOST, Timestamp, TimeSteps, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class BrwOSSError(Error):
    """
    Generic Error for problems with BrwOSS objects.
    """

class BrwOSSQueryOSTsError(BrwOSSError):
    """
    Something went wrong while trying to query the DB for the OSTs
    on this BrwOSS.
    """

class BrwOSSNoSuchOSTError(BrwOSSError):
    """
    Atempt to act on non-existent OST.
    """

class BrwOSSMissingParamsError(BrwOSSError):
    """
    You must suply a begin and end pair of Timestamp objects for the
    data query.
    """

class BrwOSSNoConnectionError(BrwOSSError):
    """
    You have to have a MySQLdb connectioon to get any data.
    """

class BrwOSSQueryError(BrwOSSError):
    """
    Something went wrong while trying to query the DB for the OST
    data.
    """

class BrwOSSQueryBrwStatsError(BrwOSSError):
    """
    Something went wrong while trying to query the DB for the OST
    data.
    """

class BrwOSSNoStepsError(BrwOSSError):
    """
    In most cases you can't do anything without a TimeSteps object.
    """

class BrwOSSNoBinsError(BrwOSSError):
    """
    In most cases you can't do anything without a HistBins object.
    """

class BrwOSSNoDataError(BrwOSSError):
    """
    There is no data.
    """

class BrwOSSNoStatsError(BrwOSSError):
    """
    The statistics summary has not been calculated.
    """

class BrwOSSSummaryDataError(BrwOSSError):
    """
    Something happened while trying to insert summary values
    back into the DB. Do you have write privilages?
    """

class BrwOSSBadModeError(BrwOSSError):
    """
    The mode passed around should be one of 'Read', 'Write', or 'Both'.
    """

#*******************************************************************************
# Begin class BrwOSS
class BrwOSS():
    """
    Container class for OSS_INFO table rows. There will be an OST
    object for each OST on the OSS as well as summary data. Hold the Steps object
    here as well.
    """

    def __init__(self, fs, name):
        """
        OST and timestamp values all come in from the same query,
        and include many OSTs on many OSSs at once. We'll initialize empty data
        structures and fill them in incrementally.
        """
        self.fs = fs
        self.name = name
        self.Debug = False
        self.DebugMessages = None
        self.DebugModules = {"BrwOST":False,
                             "Timestamp":False,
                             "TimeSteps":False,
                             "HistBins":False,
                             "HistSeries":False}
        self.ErrorMessages = None
        self.conn = None
        # Here, and below can get clear_data()ed
        self.begin = None
        self.end = None
        self.OSTs = []
        self.OSTDict = {}
        # There will be a separate Bins entry (HistBins object) for each
        # histogram type, and they are identified by the BrwIdDict and/or
        # BrwNameDict.
        self.Bins = []
        self.BrwIdDict = {}
        self.BrwNameDict = {}
        # There are read and write histograms for each BrwOSTNames
        # entry and indexed by the string representation of the STATS_ID
        self.Read = {}
        self.Write = {}
        self.Steps = None
        self.haveData = False
        self.total = 0

    def clear_data(self):
        self.begin = None
        self.end = None
        self.OSTs = []
        self.OSTDict = {}
        # There will be a separate Bins entry (HistBins object) for each
        # histogram type, and they are identified by the BrwIdDict and/or
        # BrwNameDict.
        self.Bins = []
        self.BrwIdDict = {}
        self.BrwNameDict = {}
        # There are read and write histograms for each BrwOSTNames
        # entry and indexed by the string representation of the STATS_ID
        self.Read = {}
        self.Write = {}
        self.Steps = None
        self.haveData = False
        self.total = 0

    def debug(self, module=None):
        if (module is None) or (module == "BrwOSS"):
            self.Debug = not self.Debug
            self.Read.debug()
            self.Write.debug()
            self.BrwOSS.debug()
        if module == "OST":
            self.DebugModules[module] = not self.DebugModules[module]
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
        for ost in self.OSTs:
            ost.showStep(hist, step)

    def register(self, ost, hist, bin, sie, read, write):
        """
        All the sie values should alredy be registered with self.Steps
        """
        if self.Debug == True:
            self.DebugMessages += "OSS %s" % self.name
        if self.Steps is None:
            handleError(self,
                        BrwOSSNoStepsError,
                        "BrwOSS.register(): Warning - registering OST %s hist %d bin %d sie %d without a TimeSteps object" % (ost.name, hist, bin, sie))
            # not reached
        if self.Steps.steps() == 0:
            handleError(self,
                        BrwOSSNoStepsError,
                        "BrwOSS.register(): Warning - registering OST %s hist %d bin %d sie %d with zero length TimeSteps object" % (ost.name, sie))
            # not reached
        ost.register(hist, bin, sie, read, write)

    def getBrwStats(self, conn=None, stat=None, bins=None):
        """
        Get the list of BrwStats using the provided MySQL
        connection 'conn'.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BrwOSSNoConnectionError,
                            "BrwOSS.getBrwStats(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "BrwOSS.getBrwStats(): get the list of BRW Stats on %s" % self.name
        self.end = self.Steps.end
        self.begin = self.Steps.begin
        query = "SELECT * FROM BRW_STATS_INFO"
        if not stat is None:
            query += " where STATS_NAME='" + stat + "'"
        query += " order by STATS_ID"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BrwOSSQueryBrwStatsError,
                        "BrwOSS.getBrwStats: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwOSSQueryBrwStatsError,
                        "BrwOSS.getBrwStats(): WARNING - No data")
            # not reached
        for row in rows:
            id = row["STATS_ID"]
            name = row["STATS_NAME"]
            description = row["DESCRIPTION"]
            units = row["UNITS"]
            if self.Debug == True:
                print "brw stats histogram name for %s: (%s, %s, %s, %s)" % (self.name,
                                                                             id,
                                                                             name,
                                                                             description,
                                                                             units)
            self.BrwIdDict[id] = len(self.Bins)
            self.BrwNameDict[name] = len(self.Bins)
            for o in self.OSTs:
                o.BrwIdDict[id] = len(self.Bins)
                o.BrwNameDict[name] = len(self.Bins)
            Bins = HistBins.HistBins(id, name, description, units)
            if self.DebugModules["HistBins"] == True:
                Bins.debug()
            self.Bins.append(Bins)
            self.Read[id] = HistSeries.HistSeries(name, "count")
            self.Read[id].setSteps(self.Steps)
            self.Write[id] = HistSeries.HistSeries(name, "count")
            self.Write[id].setSteps(self.Steps)
            for o in self.OSTs:
                o.Read[id] = HistSeries.HistSeries(name, "count")
                o.Read[id].setSteps(self.Steps)
                o.Write[id] = HistSeries.HistSeries(name, "count")
                o.Write[id].setSteps(self.Steps)
        # If we were passed a list of bins, preload the Bins object with them
        # Presumably we won't see any new ones in the examination of rows.
        if (not stat is None) and (not bins is None):
            stats_id = self.Bins[self.BrwNameDict[stat]].id
            for bin in bins:
                self.Bins[self.BrwIdDict[stats_id]].examine(bin)
        cursor.close()
        query = "select distinct STATS_ID, BIN from TIMESTAMP_INFO,BRW_STATS_DATA where "
        if not stat is None:
            stats_id = self.Bins[self.BrwNameDict[stat]].id
            query += "STATS_ID=" + str(stats_id) + " AND "
        query += "TIMESTAMP_INFO.TS_ID=BRW_STATS_DATA.TS_ID "
        query += "AND TIMESTAMP_INFO.TIMESTAMP >= '"
        query += self.begin.timestr
        query += "' AND TIMESTAMP_INFO.TIMESTAMP <= '"
        query += self.end.timestr
        query += "' order by STATS_ID, BIN"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BrwOSSQueryBrwOSSError,
                        "BrwOSS.getBrwStats: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwOSSQueryBrwOSSError,
                        "BrwOSS.getBrwStats(): WARNING - No bins data")
            # not reached
        for row in rows:
            id = row["STATS_ID"]
            bin = row["BIN"]
            self.Bins[self.BrwIdDict[stats_id]].examine(bin)
        for Bins in self.Bins:
            Bins.register()
            if (Bins.bins() == 0):
                handleError(self,
                            BrwOSSNoBinsError,
                            "BrwOSS.getBrwStats(): Warning - No bins for %s" % Bins.name)
                # not reached
        self.setBins()
        return

    def getOSTs(self, conn=None):
        """
        This supports accessing just the one BrwOSS.

        Get the list of OSTs that are on this BrwOSS using the provided MySQL
        connection 'conn'.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BrwOSSNoConnectionError,
                            "BrwOSS.getOSTs(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "BrwOSS.getOSTs(): get the list of OSTs on OSS %s" % self.name
        query = "SELECT * FROM OSS_INFO,OST_INFO WHERE "
        query += "OSS_INFO.OSS_ID=OST_INFO.OSS_ID AND "
        query += "OSS_INFO.HOSTNAME='" + self.name + "'"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BrwOSSQueryOSTsError,
                        "BrwOSS.getOSTs: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwOSSQueryOSTsError,
                        "BrwOSS.getOSTs(): WARNING - No data")
            # not reached
        for row in rows:
            name = row["OST_NAME"]
            self.OSTDict[name] = len(self.OSTs)
            brwost = BrwOST.BrwOST(name=name)
            if self.DebugModules["BrwOST"] == True:
                brwost.debug()
                brwost.debug("HistSeries")
            self.OSTs.append(brwost)
        cursor.close()
        return

    def getOST(self, ost=None):
        if ost is None:
            handleError(self,
                        BrwOSSNoSuchOSTError,
                        "BrwOSS.getOST(): Warning - Nothing requested")
        if ost in self.OSTDict:
            return(self.OSTs[self.OSTDict[ost]])
        handleError(self,
                    BrwOSSNoSuchOSTError,
                    "BrwOSS.getOST(): Warning - No OST %s on OSS %s" % (oss, self.name))

    def setSteps(self, Steps):
        self.Steps = Steps
        for o in self.OSTs:
            o.setSteps(self.Steps)

    def setBins(self):
        for Bins in self.Bins:
            self.Read[Bins.id].setBins(Bins)
            self.Write[Bins.id].setBins(Bins)
            for o in self.OSTs:
                o.Bins = self.Bins
                o.setBins()

    def getData(self, stat=None):
        """
        This supports accessing just the one OSS.

        Get data from the MySQL connection 'conn' for the interval
        from begin to end. begin and end are themselves already
        Timestamp objects, and are required.
        """
        if self.Steps is None:
            print "BrwOSS.getData(): Error - You must supply a TimeSteps oject first"
            return
        if self.conn is None:
            handleError(self,
                        BrwOSSNoConnectionError,
                        "BrwOSS.getData(): Error - Please provide a MySQL connection")
            # not reached
        if self.Debug == True:
            self.DebugMessages += "BrwOSS.getData(): get data from %d/%d to %d/%d" % (begin.sie, begin.ts_id, end.sie, end.ts_id)
        self.end = self.Steps.end
        self.begin = self.Steps.begin
        query = "SELECT * FROM OSS_INFO,OST_INFO,TIMESTAMP_INFO,BRW_STATS_DATA WHERE "
        if not stat is None:
            stats_id = self.Bins[self.BrwNameDict[stat]].id
            query += "STATS_ID=" + str(stats_id) + " AND "
        query += "OSS_INFO.OSS_ID=OST_INFO.OSS_ID AND "
        query += "OST_INFO.OST_ID=BRW_STATS_DATA.OST_ID AND "
        query += "OSS_INFO.HOSTNAME='" + self.name + "' AND "
        query += "TIMESTAMP_INFO.TS_ID=BRW_STATS_DATA.TS_ID "
        query += "AND TIMESTAMP_INFO.TIMESTAMP >= '"
        query += self.begin.timestr
        query += "' AND TIMESTAMP_INFO.TIMESTAMP <= '"
        query += self.end.timestr
        query += "' order by TIMESTAMP_INFO.TS_ID"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BrwOSSQueryError,
                        "BrwOSS.getData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwOSSQueryError,
                        "BrwOSS.getData(): WARNING - No data")
            # not reached
        # Build up the arrays.
        for row in rows:
            name = row['OST_NAME']
            if not name in self.OSTDict:
                handleError(self,
                            BrwOSSNoSuchOSTError,
                            "BrwOSS.getData(): Warning - No OST %s on BrwOSS %s" % (name, self.name))
                # not reached
            o = self.OSTs[self.OSTDict[name]]
            id = row['STATS_ID']
            bin = row['BIN']
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            read = int(row['READ_COUNT'])
            write = int(row['WRITE_COUNT'])
            self.register(o, id, bin, sie, read, write)
        cursor.close()
        self.setData()
        return

    def getDataSlice(self, conn=None, stat=None):
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BrwFSNoConnectionError,
                            "BrwFS.getDataSlice(): Error - Please provide a MySQL connection")
                # not reached
            else:
                conn = self.conn
        for ost in self.OSTs:
            ost.getDataSlice(conn=conn, stat=stat)

    def setData(self, begin=None, end=None):
        """
        If data is acquired elsewhere and not from the getData() above, this is also
        called from elsewhere (as from Bulk.setData()). We need to initialize the
        (begin, end) pair. For the same reason we need to pass those values to the OSTs.
        """
        if not begin is None:
            self.begin = begin
        if not end is None:
            self.end = end
        for ost in self.OSTs:
            ost.setData(self.begin, self.end)
        self.interpolate()
        self.setRead()
        self.setWrite()
        self.haveData = True

    def interpolate(self):
        for o in self.OSTs:
            o.interpolate()

    def setRead(self):
        for Bins in self.Bins:
            Read = self.Read[Bins.id]
            for ost in self.OSTs:
                ostRead = ost.Read[Bins.id]
                if Read.count == 0:
                    Read.copy(ostRead)
                else:
                    Read.add(ostRead)

    def setWrite(self):
        for Bins in self.Bins:
            Write = self.Write[Bins.id]
            for ost in self.OSTs:
                ostWrite = ost.Write[Bins.id]
                if Write.count == 0:
                    Write.copy(ostWrite)
                else:
                    Write.add(ostWrite)

    def report(self, mode):
        if (self.Bins is None) or (len(self.Bins) == 0):
            handleError(self,
                        BrwOSSNoBinsError,
                        "BrwOSS.report(): Error - No Bins for OSS %s" % self.name)
        for i, Bins in enumerate(self.Bins):
            if (Bins is None) or (Bins.bins() == 0):
                handleError(self,
                            BrwOSSNoBinsError,
                            "BrwOSS.report(): Error - No Bins for OSS %s histogram" % (self.name, i))
        if (self.Steps is None) or (self.Steps.steps() == 0):
            handleError(self,
                        BrwOSSNoStepsError,
                        "BrwOSS.report(): - Error - No steps for OSS %s" % self.name)
            # not reached
        print "%s (count = %d)" % (self.name, self.Read[self.Bins[0].id].count)
        if (mode is None) or (mode == 'read'):
            print "Read BRW Stats Histograms:"
            for key, Hist in self.Read.iteritems():
                print "name = %s, key = %s" % (Hist.name, key)
                print self.Bins[self.BrwIdDict[key]].Bins
                print Hist.Values[:,-1] - Hist.Values[:,0]
        if (mode is None) or (mode == 'write'):
            print "Write BRW Stats Histograms:"
            for key, Hist in self.Write.iteritems():
                print "name = %s, key = %s" % (Hist.name, key)
                print self.Bins[self.BrwIdDict[key]].Bins
                print Hist.Values[:,-1] - Hist.Values[:,0]

# End of class BrwOSS
#*******************************************************************************
