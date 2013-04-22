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

from pyLMT import Timestamp, HistBins, TimeSteps, HistSeries, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class BrwOSTError(Error):
    """
    Generic Error for problems with BrwOST objects.
    """

class BrwOSTNoSuchStatError(BrwOSTError):
    """
    The requested Stat is not on this OST
    """

class BrwOSTQueryError(BrwOSTError):
    """
    Something went wrong while trying to query the DB.
    """

class BrwOSTNoConnectionError(BrwOSTError):
    """
    We don't seem to have a MySQL conneciton.
    """

class BrwOSTQueryBrwOSTError(BrwOSTError):
    """
    We didn't get anything back from the query.
    """


class BrwOSTNoDataError(BrwOSTError):
    """
    We didn't get anything back from the query.
    """

class BrwOSTNoStepsError(BrwOSTError):
    """
    There should always be a Steps object in place.
    """

class BrwOSTNoBinsError(BrwOSTError):
    """
    There should always be a Bins object in place for each histogram type.
    """

class BrwOSTNoStatError(BrwOSTError):
    """
    There should always be a stat value when getting a data slice
    """

class BrwOSTDataSliceError(BrwOSTError):
    """
    There should always be exactly one immediately prior value
    """

class BrwOSTTS_IdDataError(BrwOSTError):
    """
    There should always be exactly one immediately prior value
    """

class BrwOSTIdDataError(BrwOSTError):
    """
    There should always be exactly one entry for a given OST_NAME
    """

BrwOSTStatNames = [
    "BRW_RPC",
    "BRW_DISPAGES",
    "BRW_DISBLOCKS",
    "BRW_FRAG",
    "BRW_FLIGHT",
    "BRW_IOTIME",
    "BRW_IOSIZE"
    ]

#*******************************************************************************
# Begin class BrwOST
class BrwOST():
    """
    The BRW_STATS_DATA table has rows that gather histogram values
    for seven different histograms for each of read RPCs and write
    RPCs. They are also broken out by <ost name>.
    """

    def __init__(self, name):
        """
        OST, histogram type, bin, read, write, and timestamp values all
        come in from the same query,and include many BrwOST at once.
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
        # histogram type, and they are identified by the BrwIdDict and/or
        # BrwNameDict.
        self.Bins = []
        self.BrwIdDict = {}
        self.BrwNameDict = {}
        # There are read and write histograms for each BrwOSTStatNames
        # entry and indexed by the string representation of the STATS_ID
        self.Read = {}
        self.Write = {}
        self.Steps = None
        self.haveData = False


    def clear_data(self):
        self.begin = None
        self.end = None
        # There will be a separate Bins entry (HistBins object) for each
        # histogram type, and they are identified by the BrwIdDict.
        self.Bins = []
        self.BrwIdDict = {}
        self.BrwNameDict = {}
        # There are read and write histograms for each BrwOSTStatNames
        # entry and indexed by the string representation of the STATS_ID
        self.Read = {}
        self.Write = {}
        self.Steps = None
        self.haveData = False

    def debug(self, module=None):
        if (module == None) or (module == "BrwOST"):
           self.Debug = not self.Debug
           for Bins in self.Bins:
                self.Read[Bins.id].debug()
                self.Write[Bins.id].debug()
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
            print "Register BRW stats for OST %s: (%s, %d, %d, %d, %d)" % (self.name,
                                                                           hist,
                                                                           bin,
                                                                           sie,
                                                                           read,
                                                                           write)
        if self.Steps == None:
            print "BrwOST.register(): Warning - registering BrwOST %s sie %d without a TimeSteps object" % (self.name, sie)
            return
        if self.Steps.steps() == 0:
            print "BrwOST.register(): Warning - registering BrwOST %s sie %d with zero length TimeSteps object" % (sel.name, sie)
            return
        self.Read[hist].register(bin, sie, read)
        self.Write[hist].register(bin, sie, write)

    def setSteps(self, Steps):
        # The steps are a universal sequence for the data, but each
        # histogram will have its own Bins sequence./
        self.Steps = Steps

    def setBins(self):
        for Bins in self.Bins:
            self.Read[Bins.id].setBins(Bins)
            self.Write[Bins.id].setBins(Bins)

    def getBrwStats(self, conn=None, stat=None, bins=None):
        """
        Get the list of BrwStats using the provided MySQL
        connection 'conn'.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BrwOSTNoConnectionError,
                            "BrwOST.getBrwStats(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "BrwOST.getBrwStats(): get the list of BrwStats on %s" % self.name
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
                        BrwOSTQueryBrwOSTError,
                        "BrwOST.getBrwStats: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwOSTQueryBrwOSTError,
                        "BrwOST.getBrwStats(): WARNING - No data")
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
            Bins = HistBins.HistBins(id, name, description, units)
            if self.DebugModules["HistBins"] == True:
                Bins.debug()
            self.Bins.append(Bins)
            self.Read[id] = HistSeries.HistSeries(name, "count")
            self.Read[id].setSteps(self.Steps)
            self.Write[id] = HistSeries.HistSeries(name, "count")
            self.Write[id].setSteps(self.Steps)
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
                        BrwOSTQueryBrwOSTError,
                        "BrwOST.getBrwStats: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwOSTQueryBrwOSTError,
                        "BrwOST.getBrwStats(): WARNING - No bins data")
            # not reached
        for row in rows:
            id = row["STATS_ID"]
            bin = row["BIN"]
            self.Bins[self.BrwIdDict[stats_id]].examine(bin)
        for Bins in self.Bins:
            Bins.register()
            if (Bins.bins() == 0):
                handleError(self,
                            BrwOSTNoBinsError,
                            "BrwOST.getBrwStats(): Warning - No bins for %s" % Bins.name)
                # not reached
        self.setBins()
        return

    def showBrwStatsNames(self):
        if self.Bins is None:
            return
        for Bins in self.Bins:
            print Bins.name

    def getStatIndex(self, stat=None):
        if stat is None:
            handleError(self,
                        BrwOSTNoSuchStatError,
                        "BrwOST.getStatIndex(): Warning - Nothing requested")
        if stat in self.BrwNameDict:
            return(self.BrwNameDict[stat])
        handleError(self,
                    BrwOSTNoSuchStatError,
                    "BrwOST.getStatIndex(): Warning - No Stat %s on OST %s" % (stat, self.name))

    def getStatId(self, stat=None):
        if stat is None:
            handleError(self,
                        BrwOSTNoSuchStatError,
                        "BrwOST.getStatId(): Warning - Nothing requested")
        index = self.getStatIndex(stat)
        if (index < 0) or (index >= len(self.Bins)):
            handleError(self,
                        BrwOSTNoSuchStatError,
                        "BrwOST.getStatId(): Warning - Index out of range")
        return(self.Bins[index].id)

    def getBins(self, stat=None):
        if stat is None:
            handleError(self,
                        BrwOSTNoSuchStatError,
                        "BrwOST.getBins(): Warning - Nothing requested")
        index = self.getStatIndex(stat)
        if (index < 0) or (index >= len(self.Bins)):
            handleError(self,
                        BrwOSTNoSuchStatError,
                        "BrwOST.getBins(): Warning - Index out of range")
        # This should really be a call to a HistBins method getBins()
        return(self.Bins[index].Bins)

    def getUnits(self, stat=None):
        if stat is None:
            handleError(self,
                        BrwOSTNoSuchStatError,
                        "BrwOST.getUnits(): Warning - Nothing requested")
        index = self.getStatIndex(stat)
        if (index < 0) or (index >= len(self.Bins)):
            handleError(self,
                        BrwOSTNoSuchStatError,
                        "BrwOST.getUnits(): Warning - Index out of range")
        # This should really be a call to a HistBins method getUnits()
        return(self.Bins[index].units)

    def getData(self, conn=None, stat=None):
        """
        Get data from the MySQL connection 'conn' for the interval
        from begin to end. begin and end are themselves already
        Timestamp objects, and are required.
        """
        if self.Steps is None:
            print "BrwOST.getData(): Error - You must supply a TimeSteps oject first"
            return
        self.end = self.Steps.end
        self.begin = self.Steps.begin
        if conn == None:
            if self.conn == None:
                print "BrwOST.getData(): Error - Please provide a MySQL connection"
                return
        else:
            self.conn = conn
        if self.Debug == True:
            print "BrwOST.getData(): get data from %d/%d to %d/%d" % (begin.sie, begin.ts_id, end.sie, end.ts_id)
        query = "SELECT * FROM OST_INFO,TIMESTAMP_INFO,BRW_STATS_DATA WHERE "
        if not stat is None:
            stats_id = self.Bins[self.BrwNameDict[stat]].id
            query += "STATS_ID=" + str(stats_id) + " AND "
        query += "OST_INFO.OST_ID=BRW_STATS_DATA.OST_ID AND "
        query += "OST_INFO.OST_NAME='" + self.name + "' AND "
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
                        BrwOSTQueryError,
                        "BrwOST.getData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BrwOSTNoDataError,
                        "BrwOST.getData(): WARNING - No data")
            # not reached
        # Build up the arrays.
        for row in rows:
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            self.register(row['STATS_ID'], row['BIN'], sie, int(row['READ_COUNT']), int(row['WRITE_COUNT']))
        cursor.close()
        self.interpolate()
        self.haveData = True
        return

    def getOstId(self):
        """
        Get the OST_ID first to simplify the following queries
        """
        query  = "select OST_ID from OST_INFO where OST_NAME='"
        query += self.name + "'"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                print "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BrwOSTQueryError,
                        "BrwOST.getOSTId: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) != 1:
            handleError(self,
                        BrwOSTIdDataError,
                        "BrwOST.getOstId(): WARNING - Did not get exactly one row (%d instead)" % len(rows))
            # not reached
        # Build up the arrays.
        row = rows[0]
        ostId = row['OST_ID']
        cursor.close()
        return(ostId)

    def getTS_ID_before(self, ost_id, stats_id, bin, ts_id):
        latest = None
        distance = 1
        while (latest is None) and (distance < ts_id):
            query  = "select max(TS_ID) from BRW_STATS_DATA where "
            query += "OST_ID='" + str(ost_id) + "' AND "
            query += "STATS_ID=" + str(stats_id) + " AND "
            query += "BIN = " + str(bin) + " AND "
            query += "TS_ID > " + str(ts_id-distance) + " AND "
            query += "TS_ID <= " + str(ts_id)
            #print query
            try:
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
                if self.Debug == True:
                    print "\t%s" % query
                cursor.execute (query)
            except MySQLdb.Error, e:
                cursor.close()
                handleError(self,
                            BrwOSTQueryError,
                            "BrwOST.getOSTId: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
                # not reached
            rows = cursor.fetchall()
            if len(rows) == 0:
                distance *= 10
                cursor.close()
                continue
            if len(rows) > 1:
                handleError(self,
                            BrwOSTTS_IdDataError,
                            "BrwOST.geTS_Id_before(): WARNING - Did not get exactly one row (%d instead)" % len(rows))
                # not reached
            row = rows[0]
            if (row is None) or (row['max(TS_ID)'] is None):
                distance *= 10
                cursor.close()
                continue
            #print "found ", row['max(TS_ID)']
            latest = row['max(TS_ID)']
            cursor.close()
        return(latest)

    def getDataSlice(self, conn=None, stat=None):
        """
        Get data from the MySQL connection 'conn' for the statistic
        'stat'. We're only interested in the two consecutive timestamps
        so we may have to look back and forward to establish values for
        all bins.
        """
        if self.Steps is None:
            print "BrwOST.getDataSlice(): Error - You must supply a TimeSteps oject first"
            return
        self.end = self.Steps.end
        self.begin = self.Steps.begin
        if conn == None:
            if self.conn == None:
                print "BrwOST.getDataSlice(): Error - Please provide a MySQL connection"
                return
        else:
            self.conn = conn
        if self.Debug == True:
            print "BrwOST.getDataSlice(): get data from %d/%d to %d/%d" % (begin.sie, begin.ts_id, end.sie, end.ts_id)
        if stat is None:
            handleError(self,
                        BrwOSTNoStatError,
                        "BrwOST.getDataSlice(): WARNING - No stat provided")
            # not reached
        #print "BrwOST.getDataSlice(): OST %s stat %s step %s" % (self.name, stat, self.begin.timestamp)
        stats_id = self.Bins[self.BrwNameDict[stat]].id
        bins = self.Bins[self.BrwNameDict[stat]].Bins
        ost_id = self.getOstId()
        for bin in bins:
            #print "OST %d Stat %d bin %f ts_id %d" % (ost_id, stats_id, bin, self.begin.ts_id)
            ts_id = self.getTS_ID_before(ost_id, stats_id, bin, self.begin.ts_id)
            query = "SELECT * FROM BRW_STATS_DATA WHERE "
            query += "OST_ID='" + str(ost_id) + "' AND "
            query += "STATS_ID=" + str(stats_id) + " AND "
            query += "BIN = " + str(bin) + " AND "
            query += "TS_ID =" + str(ts_id)
            #print query
            try:
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
                if self.Debug == True:
                    print "\t%s" % query
                cursor.execute (query)
            except MySQLdb.Error, e:
                cursor.close()
                handleError(self,
                            BrwOSTQueryError,
                            "BrwOST.getDataSlice: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
                # not reached
            rows = cursor.fetchall()
            if len(rows) != 1:
                handleError(self,
                            BrwOSTDataSliceError,
                            "BrwOST.getDataSlice(): WARNING - Did not get exactly one row (%d instead)" % len(rows))
            # not reached
            # Build up the arrays.
            row = rows[0]
            # Note that we are lying here and returning the known last vlaue
            # prior to 'begin' but we are saying it is at 'begin'. This is a
            # defect of how the Series/HistSeries objects are organized.
            # We also only ever fill in the first timestep.
            self.register(row['STATS_ID'], row['BIN'], self.begin.sie, int(row['READ_COUNT']), int(row['WRITE_COUNT']))
            cursor.close()
        self.haveData = True
        return

    def setData(self, begin, end):
        self.begin = begin
        self.end = end
        for Bins in self.Bins:
            self.Read[Bins.id].count = 1
            self.Write[Bins.id].count = 1

    def interpolate(self):
        for Bins in self.Bins:
            Read = self.Read[Bins.id]
            Write = self.Write[Bins.id]
            n = Read.interpolate()
            if n == 0:
                handleError(self,
                            BrwOSTNoDataError,
                            "BrwOST.interpolate(): Warning - No Read data")
                # not reached
            n = Write.interpolate()
            if n == 0:
                handleError(self,
                            BrwOSTNoDataError,
                            "BrwOST.interpolate(): Warning - No Write data")
                # not reached

    def report(self, mode=None):
        print "%s (count = %d)" % (self.name, self.Read[self.Bins[0].id].count)
        if (self.Bins is None) or (len(self.Bins) == 0):
            handleError(self,
                        BrwOSTNoBinsError,
                        "BrwOST.report(): Error - No HistBins objects for OST %s" % self.name)
        for i, Bins in enumerate(self.Bins):
            if (Bins is None) or (Bins.bins() == 0):
                handleError(self,
                            BrwOSTNoBinsError,
                            "BrwOST.report(): Error - No Bins for OST %s histogram" % (self.name, i))
        if (self.Steps is None) or (self.Steps.steps() == 0):
            handleError(self,
                        BrwOSTNoStepsError,
                        "BrwOST.report(): Error - No Steps for OST %s" % self.name)
        if (mode is None) or (mode == 'read'):
            print "Read BRW Stats Histograms:"
            for key, Hist in self.Read.iteritems():
                values = Hist.Values[:,-1] - Hist.Values[:,0]
                volume = np.sum(values*self.Bins[self.BrwIdDict[key]].Bins)
                print "name = %s, key = %s, total = %d, volume = %d" % (Hist.name,
                                                                        key,
                                                                        int(np.sum(values)),
                                                                        int(volume))
                print self.Bins[self.BrwIdDict[key]].Bins
                print values
        if (mode is None) or (mode == 'write'):
            print "Write BRW Stats Histograms:"
            for key, Hist in self.Write.iteritems():
                values = Hist.Values[:,-1] - Hist.Values[:,0]
                volume = np.sum(values*self.Bins[self.BrwIdDict[key]].Bins)
                print "name = %s, key = %s, total = %d, volume = %d" % (Hist.name,
                                                                        key,
                                                                        int(np.sum(values)),
                                                                        int(volume))
                print self.Bins[self.BrwIdDict[key]].Bins
                print values

# End of class BrwOST
#*******************************************************************************
