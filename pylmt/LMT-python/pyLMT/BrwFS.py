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

from pyLMT import Counter, HistBins, HistSeries, BrwOSS, Timestamp, TimeSteps, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class BrwFSError(Error):
    """
    Generic Error for problems with BrwFS objects.
    """

class BrwFSQueryOSSsError(BrwFSError):
    """
    Something went wrong while trying to query the DB for the OSSs
    on this BrwFS.
    """

class BrwFSNoSuchOSSError(BrwFSError):
    """
    Atempt to act on non-existent OSS.
    """

class BrwFSNoSuchOSTError(BrwFSError):
    """
    Atempt to act on non-existent OST.
    """

class BrwFSNoSuchStatError(BrwFSError):
    """
    Atempt to act on non-existent Stat.
    """

class BrwFSMissingParamsError(BrwFSError):
    """
    You must suply a begin and end pair of Timestamp objects for the
    data query.
    """

class BrwFSNoConnectionError(BrwFSError):
    """
    You have to have a MySQLdb connectioon to get any data.
    """

class BrwFSQueryError(BrwFSError):
    """
    Something went wrong while trying to query the DB for the OSS
    data.
    """

class BrwFSQueryBrwStatsError(BrwFSError):
    """
    Something went wrong while trying to query the DB for the OSS
    data.
    """

class BrwFSNoStepsError(BrwFSError):
    """
    In most cases you can't do anything without a TimeSteps object.
    """

class BrwFSNoBinsError(BrwFSError):
    """
    In most cases you can't do anything without a HistBins object.
    """

class BrwFSNoDataError(BrwFSError):
    """
    There is no data.
    """

class BrwFSNoStatsError(BrwFSError):
    """
    The statistics summary has not been calculated.
    """

class BrwFSSummaryDataError(BrwFSError):
    """
    Something happened while trying to insert summary values
    back into the DB. Do you have write privilages?
    """

class BrwFSBadModeError(BrwFSError):
    """
    The mode passed around should be one of 'Read', 'Write', or 'Both'.
    """

#*******************************************************************************
# Begin class BrwFS
class BrwFS():
    """
    Container class for OSS_INFO table rows. There will be an OSS
    object for each server on the FS as well as summary data. Hold the Steps object
    here as well.
    """

    def __init__(self, name):
        """
        OSS and timestamp values all come in from the same query,
        and include many OSSs and OSTs at once. We'll initialize empty data
        structures and fill them in incrementally.
        """
        self.name = name
        self.Debug = False
        self.DebugMessages = None
        self.DebugModules = {"BrwOSS":False,
                             "Timestamp":False,
                             "TimeSteps":False,
                             "HistBins":False,
                             "HistSeries":False}
        self.ErrorMessages = None
        self.conn = None
        # Here, and below can get clear_data()ed
        self.begin = None
        self.end = None
        self.OSSs = []
        self.OSSDict = {}
        # There will be a separate Bins entry (HistBins object) for each
        # histogram type, and they are identified by the BrwIdDict and/or
        # BrwNameDict.
        self.Bins = []
        self.BrwIdDict = {}
        self.BrwNameDict = {}
        # There are read and write histograms for each BrwOSSNames
        # entry and indexed by the string representation of the STATS_ID
        self.Read = {}
        self.Write = {}
        self.Steps = None
        self.haveData = False
        self.total = 0

    def clear_data(self):
        self.begin = None
        self.end = None
        self.OSSs = []
        self.OSSDict = {}
        # There will be a separate Bins entry (HistBins object) for each
        # histogram type, and they are identified by the BrwIdDict and/or
        # BrwNameDict.
        self.Bins = []
        self.BrwIdDict = {}
        self.BrwNameDict = {}
        # There are read and write histograms for each BrwOSSNames
        # entry and indexed by the string representation of the STATS_ID
        self.Read = {}
        self.Write = {}
        self.Steps = None
        self.haveData = False
        self.total = 0

    def debug(self, module=None):
        if (module is None) or (module == "BrwFS"):
            self.Debug = not self.Debug
            self.Read.debug()
            self.Write.debug()
            self.BrwFS.debug()
        if module == "OSS":
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
        for oss in self.OSSs:
            oss.showStep(hist, step)

    def register(self, oss, ost, hist, bin, sie, read, write):
        """
        All the sie values should alredy be registered with self.Steps
        """
        if self.Debug == True:
            self.DebugMessages += "FS %s" % self.name
        if self.Steps is None:
            handleError(self,
                        BrwFSNoStepsError,
                        "BrwFS.register(): Warning - registering OSS %s OST %s hist %d bin %d sie %d without a TimeSteps object" % (oss.name, ost.name, hist, bin, sie))
            # not reached
        if self.Steps.steps() == 0:
            handleError(self,
                        BrwFSNoStepsError,
                        "BrwFS.register(): Warning - registering OSS %s OST %s hist %d bin %d sie %d with zero length TimeSteps object" % (oss.name, ost.name, sie))
            # not reached
        oss.register(ost, hist, bin, sie, read, write)

    def getBrwStats(self, conn=None, stat=None, bins=None):
        """
        Get the list of BrwStats using the provided MySQL
        connection 'conn'.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BrwFSNoConnectionError,
                            "BrwFS.getBrwStats(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "BrwFS.getBrwStats(): get the list of BRW Stats on %s" % self.name
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
                        BrwFSQueryBrwStatsError,
                        "BrwFS.getBrwStats: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwFSQueryBrwStatsError,
                        "BrwFS.getBrwStats(): WARNING - No data")
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
            for oss in self.OSSs:
                oss.BrwIdDict[id] = len(self.Bins)
                oss.BrwNameDict[name] = len(self.Bins)
                for ost in oss.OSTs:
                    ost.BrwIdDict[id] = len(self.Bins)
                    ost.BrwNameDict[name] = len(self.Bins)
            Bins = HistBins.HistBins(id, name, description, units)
            if self.DebugModules["HistBins"] == True:
                Bins.debug()
            self.Bins.append(Bins)
            self.Read[id] = HistSeries.HistSeries(name, "count")
            self.Read[id].setSteps(self.Steps)
            self.Write[id] = HistSeries.HistSeries(name, "count")
            self.Write[id].setSteps(self.Steps)
            for oss in self.OSSs:
                oss.Read[id] = HistSeries.HistSeries(name, "count")
                oss.Read[id].setSteps(self.Steps)
                oss.Write[id] = HistSeries.HistSeries(name, "count")
                oss.Write[id].setSteps(self.Steps)
                for ost in oss.OSTs:
                    ost.Read[id] = HistSeries.HistSeries(name, "count")
                    ost.Read[id].setSteps(self.Steps)
                    ost.Write[id] = HistSeries.HistSeries(name, "count")
                    ost.Write[id].setSteps(self.Steps)
        cursor.close()
        # If we were passed a list of bins, preload the Bins object with them
        # Presumably we won't see any new ones in the examination of rows,
        # so skip that and just return
        if (not stat is None) and (not bins is None):
            stats_id = self.Bins[self.BrwNameDict[stat]].id
            for bin in bins:
                self.Bins[self.BrwIdDict[stats_id]].examine(bin)
            self.Bins[self.BrwIdDict[stats_id]].register()
            self.setBins()
            return
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
                        BrwFSQueryBrwFSError,
                        "BrwFS.getBrwStats: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwFSQueryBrwFSError,
                        "BrwFS.getBrwStats(): WARNING - No bins data")
            # not reached
        for row in rows:
            id = row["STATS_ID"]
            bin = row["BIN"]
            self.Bins[self.BrwIdDict[id]].examine(bin)
        for Bins in self.Bins:
            Bins.register()
            if (Bins.bins() == 0):
                handleError(self,
                            BrwFSNoBinsError,
                            "BrwFS.getBrwStats(): Warning - No bins for %s" % Bins.name)
                # not reached
        self.setBins()
        cursor.close()
        return

    def getStatIndex(self, stat=None):
        if stat is None:
            handleError(self,
                        BrwFSNoSuchStatError,
                        "BrwFS.getStatIndex(): Warning - Nothing requested")
        if stat in self.BrwNameDict:
            return(self.BrwNameDict[stat])
        handleError(self,
                    BrwFSNoSuchStatError,
                    "BrwFS.getStatIndex(): Warning - No Stat %s on FS %s" % (stat, self.name))

    def getStatId(self, stat=None):
        if stat is None:
            handleError(self,
                        BrwFSNoSuchStatError,
                        "BrwFS.getStatId(): Warning - Nothing requested")
        index = self.getStatIndex(stat)
        if (index < 0) or (index >= len(self.Bins)):
            handleError(self,
                        BrwFSNoSuchStatError,
                        "BrwFS.getStatId(): Warning - Index out of range")
        return(self.Bins[index].id)

    def getBins(self, stat=None):
        if stat is None:
            handleError(self,
                        BrwFSNoSuchStatError,
                        "BrwFS.getBins(): Warning - Nothing requested")
        index = self.getStatIndex(stat)
        if (index < 0) or (index >= len(self.Bins)):
            handleError(self,
                        BrwFSNoSuchStatError,
                        "BrwFS.getBins(): Warning - Index out of range")
        # This should really be a call to a HistBins method getBins()
        return(self.Bins[index].Bins)

    def getUnits(self, stat=None):
        if stat is None:
            handleError(self,
                        BrwFSNoSuchStatError,
                        "BrwFS.getUnits(): Warning - Nothing requested")
        index = self.getStatIndex(stat)
        if (index < 0) or (index >= len(self.Bins)):
            handleError(self,
                        BrwFSNoSuchStatError,
                        "BrwFS.getUnits(): Warning - Index out of range")
        # This should really be a call to a HistBins method getUnits()
        return(self.Bins[index].units)

    def getOSSs(self, conn=None):
        """
        This supports accessing just the one BrwFS.

        Get the list of OSSs that are on this BrwFS using the provided MySQL
        connection 'conn'.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BrwFSNoConnectionError,
                            "BrwFS.getOSSs(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "BrwFS.getOSSs(): get the list of OSSs on %s" % self.name
        query = "SELECT * FROM OSS_INFO"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BrwFSQueryOSSsError,
                        "BrwFS.getOSSs: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwFSQueryOSSsError,
                        "BrwFS.getOSSs(): WARNING - No data")
            # not reached
        for row in rows:
            name = row["HOSTNAME"]
            self.OSSDict[name] = len(self.OSSs)
            brwoss = BrwOSS.BrwOSS(fs=self.name, name=name)
            if self.DebugModules["BrwOSS"] == True:
                brwoss.debug()
                brwoss.debug("HistSeries")
            self.OSSs.append(brwoss)
            brwoss.getOSTs(self.conn)
        cursor.close()
        return

    # You can ask for the OSS by name or by the name of one of its OSTs
    def getOSS(self, oss=None, ost=None):
        if not oss is none:
            if oss in self.OSSDict:
                return(self.OSSs[self.OSSDict[oss]])
            handleError(self,
                        BrwFSNoSuchOSSError,
                        "BrwFS.getOSS(): Warning - No OSS %s on FS %s" % (oss, self.name))
        if not ost is None:
            for oss in self.OSSs:
                if ost in oss.OSTDict:
                    return(oss)
            handleError(self,
                        BrwFSNoSuchOSSError,
                        "BrwFS.getOSS(): Warning - No OST %s on OFS %s" % (ost, self.name))
        handleError(self,
                    BrwFSNoSuchOSSError,
                    "BrwFS.getOSS(): Warning - Nothing requested")

    def getOST(self, ost=None):
        if ost is None:
            handleError(self,
                        BrwFSNoSuchOSTError,
                        "BrwFS.getOST(): Warning - Nothing requested")
        for oss in self.OSSs:
            if ost in oss.OSTDict:
                return(oss.OSTs[oss.OSTDict[ost]])
        handleError(self,
                    BrwFSNoSuchOSTError,
                    "BrwFS.getOST(): Warning - No OST %s on FS %s" % (ost, self.name))

    def setSteps(self, Steps):
        self.Steps = Steps
        for oss in self.OSSs:
            oss.setSteps(self.Steps)

    def setBins(self):
        for Bins in self.Bins:
            self.Read[Bins.id].setBins(Bins)
            self.Write[Bins.id].setBins(Bins)
            for oss in self.OSSs:
                oss.Bins = self.Bins
                oss.setBins()

    def getData(self, stat=None):
        """
        This supports accessing all OSSs.

        Get data from the MySQL connection 'conn' for the interval
        from begin to end. begin and end are themselves already
        Timestamp objects, and are required.
        """
        if self.Steps is None:
            print "BrwFS.getData(): Error - You must supply a TimeSteps oject first"
            return
        if self.conn is None:
            handleError(self,
                        BrwFSNoConnectionError,
                        "BrwFS.getData(): Error - Please provide a MySQL connection")
            # not reached
        if self.Debug == True:
            self.DebugMessages += "BrwFS.getData(): get data from %d/%d to %d/%d" % (begin.sie, begin.ts_id, end.sie, end.ts_id)
        self.end = self.Steps.end
        self.begin = self.Steps.begin
        query = "SELECT * FROM OSS_INFO,OST_INFO,TIMESTAMP_INFO,BRW_STATS_DATA WHERE "
        if not stat is None:
            stats_id = self.Bins[self.BrwNameDict[stat]].id
            query += "STATS_ID=" + str(stats_id) + " AND "
        query += "OSS_INFO.OSS_ID=OST_INFO.OSS_ID AND "
        query += "OST_INFO.OST_ID=BRW_STATS_DATA.OST_ID AND "
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
                        BrwFSQueryError,
                        "BrwFS.getData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwFSQueryError,
                        "BrwFS.getData(): WARNING - No data")
            # not reached
        # Build up the arrays.
        for row in rows:
            ossname = row['HOSTNAME']
            ostname = row['OST_NAME']
            if not ossname in self.OSSDict:
                handleError(self,
                            BrwFSNoSuchOSSError,
                            "BrwFS.getData(): Warning - No OSS %s on BrwFS %s" % (ossname, self.name))
                # not reached
            oss = self.OSSs[self.OSSDict[ossname]]
            ost = oss.getOST(ostname)
            id = row['STATS_ID']
            bin = row['BIN']
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            read = int(row['READ_COUNT'])
            write = int(row['WRITE_COUNT'])
            self.register(oss, ost, id, bin, sie, read, write)
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
        for oss in self.OSSs:
            oss.getDataSlice(conn=conn, stat=stat)

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
        for oss in self.OSSs:
            oss.setData(self.begin, self.end)
        self.interpolate()
        self.setRead()
        self.setWrite()
        self.haveData = True

    def interpolate(self):
        for oss in self.OSSs:
            oss.interpolate()

    def setRead(self):
        for Bins in self.Bins:
            Read = self.Read[Bins.id]
            for oss in self.OSSs:
                ossRead = oss.Read[Bins.id]
                if Read.count == 0:
                    Read.copy(ossRead)
                else:
                    Read.add(ossRead)

    def setWrite(self):
        for Bins in self.Bins:
            Write = self.Write[Bins.id]
            for oss in self.OSSs:
                ossWrite = oss.Write[Bins.id]
                if Write.count == 0:
                    Write.copy(ossWrite)
                else:
                    Write.add(ossWrite)

    def report(self, mode):
        if (self.Bins is None) or (len(self.Bins) == 0):
            handleError(self,
                        BrwFSNoBinsError,
                        "BrwFS.report(): Error - No Bins for OSS %s" % self.name)
        for i, Bins in enumerate(self.Bins):
            if (Bins is None) or (Bins.bins() == 0):
                handleError(self,
                            BrwFSNoBinsError,
                            "BrwFS.report(): Error - No Bins for OSS %s histogram" % (self.name, i))
        if (self.Steps is None) or (self.Steps.steps() == 0):
            handleError(self,
                        BrwFSNoStepsError,
                        "BrwFS.report(): - Error - No steps for OSS %s" % self.name)
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

# End of class BrwFS
#*******************************************************************************
