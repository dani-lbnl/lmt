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

from pyLMT import Counter, CPU, OSS, Timestamp, TimeSteps, Graph, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class BulkError(Error):
    """
    Generic Error for problems with Bulk objects.
    """

class BulkNoStepsError(BulkError):
    """
    Most things require that the Steps attribute be set (to a TimeStepes object).
    """

class BulkNoConnectionError(BulkError):
    """
    You need a MySQLdb connection to do any useful querying.
    """

class BulkQueryOSSsError(BulkError):
    """
    Something went wrong findout out what the OSS are.
    """

class BulkMissingParamsError(BulkError):
    """
    You have to have a 'begin' and an 'end'. They are Timestamp Objects.
    """

class BulkQueryError(BulkError):
    """
    Something went wrong while trying to query the DB for the OST
    data.
    """

class BulkNoDataError(BulkError):
    """
    The query returned an empty result.
    """

class BulkNoSuchOSSError(BulkError):
    """
    The OSS was not found.
    """

class BulkNoSuchOSTError(BulkError):
    """
    The OSS was not found.
    """

class BulkQueryCPUError(BulkError):
    """
    Something went wrong while trying to query the DB for the CPU
    utilization data.
    """

class BulkQuickQueryError(BulkError):
    """
    Something went wrong while trying to query the DB for the OST
    data.
    """

class BulkSummaryDataError(BulkError):
    """
    The query returned an empty result.
    """

class BulkNoStatsError(BulkError):
    """
    The time series statistics have not been calculated.
    """

#*******************************************************************************
# Begin class Bulk
class Bulk():
    """
    Container class for Bulk I/O data from all OSSs for a file system. There will
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
        self.DebugModules = {"OSS":False, "OST":False, "Timestamp":False, "TimeSteps":False}
        self.ErrorMessages = None
        self.conn = None
        self.fsID = None
        # Here, and below can get clear_data()ed
        self.begin = None
        self.end = None
        self.OSSs = []
        self.OSSDict = {}
        self.Read = Counter.Counter("Bulk read", "count/sec")
        self.Write = Counter.Counter("Bulk write", "count/sec")
        self.Bulk = Counter.Counter("Bulk", "count/sec")
        self.ReadHist = None
        self.WriteHist = None
        self.BulkHist = None
        self.HistBins = None
        self.CPU = None
        self.Steps = None
        self.numOSTs = 0
        self.haveData = False
        self.total = 0

    def clear_data(self):
        self.begin = None
        self.end = None
        self.OSSs = []
        self.OSSDict = {}
        self.Read = Counter.Counter("Bulk read", "count/sec")
        self.Write = Counter.Counter("Bulk write", "count/sec")
        self.Bulk = Counter.Counter("Bulk", "count/sec")
        self.ReadHist = None
        self.WriteHist = None
        self.BulkHist = None
        self.HistBins = None
        self.CPU = None
        self.Steps = None
        self.numOSTs = 0
        self.haveData = False
        self.total = 0

    def debug(self, module=None):
        if (module is None) or (module == "Bulk"):
            self.Debug = not self.Debug
        if module == "OSS":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "OST":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "Timestamp":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "TimeSteps":
            self.DebugModules[module] = not self.DebugModules[module]
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def register(self, oss, ost, sie, read, write):
        """
        All the sie values should alredy be registered with self.Steps
        """
        if self.Steps is None:
            handleError(self,
                        BulkNoStepsError,
                        "Bulk.register(): Warning - registering OSS %s sie %d without a TimeSteps object" % (oss.name, sie))
            # not reached
        if self.Steps.steps() == 0:
            handleError(self,
                        BulkNoStepsError,
                        "Bulk.register(): Warning - registering OSS %s sie %d with zero length TimeSteps object" % (oss.name, sie))
            # not reached
        oss.register(ost, sie, read, write)

    def getOSSs(self, conn=None):
        """
        Get the list of OSSs that are on this file system using the provided MySQL
        connection 'conn'.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BulkNoConnectionError,
                            "Bulk.getOSSs(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "Bulk.getOSTs(): get the list of OSTs on OSS %s" % self.name
        query = "SELECT * FROM OSS_INFO"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BulkQueryOSSsError,
                        "Bulk.getOSSs: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BulkQueryOSSsError,
                        "Bulk.getOSSs(): WARNING - No data")
            # not reached
        for row in rows:
            name = row["HOSTNAME"]
            self.OSSDict[name] = len(self.OSSs)
            oss = OSS.OSS(fs=self.name, name=name)
            if self.Debug == True:
                oss.debug()
                oss.debug("OST")
            # If I got all the OST_INFO rows at once I could do this all at once
            # rather than initiating a separate query for each hostname.
            self.OSSs.append(oss)
        cursor.close()
        for oss in self.OSSs:
            oss.getOSTs(conn)
            self.numOSTs += oss.getNumOSTs()
        return

    def getNumOSTs(self):
        return(self.numOSTs)

    def getNumOSSs(self):
        return(len(self.OSSs))

    def showOSSs(self):
        for oss in self.OSSs:
            print oss.name

    # You can ask for the OSS by name or by the name of one of its OSTs
    def getOSS(self, oss=None, ost=None):
        if not oss is None:
            if oss in self.OSSDict:
                return(self.OSSs[self.OSSDict[oss]])
            handleError(self,
                        BulkNoSuchOSSError,
                        "Bulk.getOSS(): Warning - No OSS %s on FS %s" % (oss, self.name))
        if not ost is None:
            for oss in self.OSSs:
                if ost in oss.OSTDict:
                    return(oss)
            handleError(self,
                        BulkNoSuchOSSError,
                        "Bulk.getOSS(): Warning - No OST %s on OFS %s" % (ost, self.name))
        handleError(self,
                    BulkNoSuchOSSError,
                    "Bulk.getOSS(): Warning - Nothing requested")

    def getOST(self, ost=None):
        if ost is None:
            handleError(self,
                        BulkNoSuchOSTError,
                        "Bulk.getOST(): Warning - Nothing requested")
        for oss in self.OSSs:
            if ost in oss.OSTDict:
                return(oss.OSTs[oss.OSTDict[ost]])
        handleError(self,
                    BulkNoSuchOSTError,
                    "Bulk.getOST(): Warning - No OST %s on FS %s" % (oss, self.name))

    def setSteps(self, Steps):
        self.Steps = Steps
        self.begin = self.Steps.begin
        self.end = self.Steps.end
        for oss in self.OSSs:
            oss.setSteps(Steps)
        self.Read.setSteps(Steps)
        self.Write.setSteps(Steps)
        self.Bulk.setSteps(Steps)

    def getData(self):
        """
        Get data from the MySQL connection self.conn for the interval
        from Steps.begin to Steps.end. begin and end are themselves already
        Timestamp objects, and are required. The self.conn was already registered
        when we called self.getOSSs()
        """
        if self.Steps is None:
            print "Bulk.getData(): Error - You must supply a TimeSteps oject first"
            return
        if self.conn is None:
            handleError(self,
                        BulkNoConnectionError,
                        "Bulk.getData(): Error - Please provide a MySQL connection")
            # not reached
        if self.Debug == True:
            self.Debug
            self.DebugMessages += "Bulk.getData(): get data from %d/%d to %d/%d" % (selfSteps.begin.sie, self.Steps.begin.ts_id, self.Steps.end.sie, self.Steps.end.ts_id)
        query = "SELECT * FROM OSS_INFO,OST_INFO,TIMESTAMP_INFO,OST_DATA WHERE "
        query += "OSS_INFO.OSS_ID=OST_INFO.OSS_ID AND "
        query += "OST_INFO.OST_ID=OST_DATA.OST_ID AND "
        query += "TIMESTAMP_INFO.TS_ID=OST_DATA.TS_ID AND TIMESTAMP_INFO.TIMESTAMP >= '"
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
                        BulkQueryError,
                        "Bulk.getData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BulkQueryError,
                        "Bulk.getData(): WARNING - No data")
            # not reached
        # Build up the arrays
        for row in rows:
            oss_name = row['HOSTNAME']
            ost_name = row['OST_NAME']
            if not oss_name in self.OSSDict:
                handleError(self,
                            BulkNoSuchOSSError,
                            "Bulk.getData(): Warning - No OSS %s on file system %s" % (oss_name, self.name))
            # not reached
            oss = self.OSSs[self.OSSDict[oss_name]]
            ost = oss.getOST(ost_name)
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            read = float(row['READ_BYTES'])
            write = float(row['WRITE_BYTES'])
            self.register(oss, ost, sie, read, write)
        cursor.close()
        self.setData()
        return

    def setData(self):
        if self.Debug == True:
            self.DebugMessages += "Bulk.setData(): file system %s" % self.name
        for oss in self.OSSs:
            oss.setData(begin=self.begin, end=self.end)
            if self.Read.count == 0:
                self.Read.copy(oss.Read)
            else:
                self.Read.add(oss.Read)
            if self.Write.count == 0:
                self.Write.copy(oss.Write)
            else:
                self.Write.add(oss.Write)
            if self.Bulk.count == 0:
                self.Bulk.copy(oss.OSS)
            else:
                self.Bulk.add(oss.OSS)
        self.haveData = True

    def getCPU(self):
        """
        """
        if self.Steps is None:
            print "OSS.getCPU(): Error - You must supply a TimeSteps oject first"
            return
        if self.conn is None:
            handleError(self,
                        BulkNoConnectionError,
                        "Bulk.getCPU(): Error - Please provide a MySQL connection")
            # not reached
        query = "SELECT HOSTNAME,TIMESTAMP,PCT_CPU FROM TIMESTAMP_INFO,OSS_DATA,OSS_INFO WHERE "
        query += "OSS_DATA.OSS_ID=OSS_INFO.OSS_ID AND "
        query += "TIMESTAMP_INFO.TS_ID=OSS_DATA.TS_ID AND TIMESTAMP_INFO.TIMESTAMP >= '"
        query += self.begin.timestr
        query += "' AND TIMESTAMP_INFO.TIMESTAMP <= '"
        query += self.end.timestr
        query += "'"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BulkQueryCPUError,
                        "Bulk.queryCPU: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BulkQueryCPUError,
                        "Bulk.getCPU(): WARNING - No data")
            # not reached
        # If we are getting ops data as well, we hope that the set of timestamps
        # for which there is PCT_CPU data is the same as for OPS. There may be
        # missing or extra values or both. Interpolation should handle a few
        # missing, but handle the extras gracefully. Obviously, if there is no
        # ops data then we need to get the TimeSteps.
        if self.Steps is None:
            Steps = TimeSteps.TimeSteps()
            if self.Debug == True:
                Steps.debug()
            for row in rows:
                Steps.examine(row['TIMESTAMP'], Timestamp.calc_sie(row['TIMESTAMP']), row['TS_ID'])
            Steps.register()
            if (Steps.steps() == 0):
                handleError(self,
                            BulkQueryCPUError,
                            "Bulk.getData(): Warning - No steps")
                # not reached
            self.setSteps(Steps)
        # In the second phase we actually build up the array
        for oss in self.OSSs:
            oss.CPU = CPU.CPU(oss.name+" CPU utilization")
            if oss.Debug == True:
                oss.CPU.debug()
            oss.CPU.setSteps(self.Steps)
            oss.CPU.initCount()
        for row in rows:
            oss = self.OSSs[self.OSSDict[row['HOSTNAME']]]
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            if sie is None:
                # or should I throw an exception?
                continue
            oss.CPU.register(sie, float(row['PCT_CPU']))
        cursor.close()
        self.CPU = CPU.CPU("ave. CPU utilization")
        self.CPU.setSteps(self.Steps)
        # CPU values get accumulated as teh average value rather than
        # the sum so we aren't using the usual TimeSeries.add() method.
        # Note also that self.CPU never calls interpolate(). the call
        # to stats() need to come after the OSS.CPU values have been
        # accumulated.
        self.CPU.Values = np.zeros_like(self.Steps, dtype=np.float64)
        # I don't initCount here because the aggregate CPU is going to
        # get initialized by the first oss.CPU that  is added.
        for oss in self.OSSs:
            oss.setCPU()
            self.CPU.add(oss.CPU)
        self.haveData = True

    def quickRegister(self, sie, read, write, cpu):
        index = self.Steps.getIndex(sie)
        self.Read.Values[index] = read
        self.Write.Values[index] = write
        self.Bulk.Values[index] = read + write
        self.CPU.Values[index] = cpu

    def getQuickData(self, conn=None):
        """
        Get previously aggregated data from the MySQL connection 'conn'
        for the interval from Steps.begin to Steps.end. begin and end
        are themselves already Timestamp objects.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            BulkNoConnectionError,
                      "Bulk.getQuickDate(): Error - No connection to MySQL DB")
                # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "Bulk.getQuickData(): get data from %d/%d to %d/%d" % (self.Steps.begin.sie, self.Steps.begin.ts_id, self.Steps.end.sie, self.Steps.end.ts_id)
        query = "SELECT * FROM FILESYSTEM_AGGREGATE,TIMESTAMP_INFO WHERE "
        query += "TIMESTAMP_INFO.TS_ID=FILESYSTEM_AGGREGATE.TS_ID "
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
                        BulkQuickQueryError,
                        "Bulk.getQuickData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        BulkNoDataError,
                        "Bulk.getQuickData(): WARNING - No data")
            # not reached
        self.CPU = CPU.CPU("ave. CPU utilization")
        self.CPU.setSteps(self.Steps)
        # Build up the arrays.
        for row in rows:
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            read = float(row['READ_RATE'])
            write = float(row['WRITE_RATE'])
            cpu = float(row['PCT_CPU'])
            self.quickRegister(sie, read, write, cpu)
        cursor.close()
        return

    def insertHeader(self):
        header  = "insert ignore into FILESYSTEM_AGGREGATE "
        header += "(FILESYSTEM_ID,TS_ID,READ_RATE,WRITE_RATE,PCT_CPU) values "
        if self.fsID is None:
            query = "select FILESYSTEM_ID from FILESYSTEM_INFO where"
            query += " FILESYSTEM_NAME='"
            query += self.name + "'"
            try:
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
                if self.Debug == True:
                    print "\t%s" % query
                cursor.execute (query)
            except MySQLdb.Error, e:
                cursor.close()
                handleError(self,
                            BulkSummaryDataError,
                            "Bulk.insertHeader: Error %d: %s\n%s" % (e.args[0],
                                                                     e.args[1],
                                                                     query))
                # not reached
            rows = cursor.fetchall()
            if len(rows) == 0:
                handleError(self,
                            BulkSummaryDataError,
                            "Bulk.insertHeader(): Error - failed to determine FILESYSTEM_ID")
                # not reached
            self.fsID = rows[0]['FILESYSTEM_ID']
            cursor.close()
        return(header)


    def insertValues(self, sie):
        """
        This should be building up the values for an insert MySQLdb
        call, but if something goes wrong for one of them I just ignore
        it. Maybe I shouldn't.
        """
        index = self.Steps.getIndex(sie)
        if index is None:
            return(none)
        ts_id = self.Steps.getTS_ID(index)
        if ts_id is None:
            return(None)
        values = "(" + str(self.fsID) + ","
        values += str(ts_id) + ","
        values += str(self.Read.Values[index]) + ","
        values += str(self.Write.Values[index]) + ","
        values += str(self.CPU.Values[index]) + ")"
        return(values)

    def doInsert(self, insert):
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % insert
            cursor.execute (insert)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        BulkSummaryDataError,
                        "Bulk.doInsert: Error %d: %s\n%s" % (e.args[0],
                                                             e.args[1],
                                                             insert))
                # not reached
        cursor.close()
        self.conn.commit()

    def report(self):
        if self.Steps is None:
            handleError(self,
                        BulkNoStepsError,
                        "Bulk.report(): - Warning - No steps")
                # not reached
        print self.name
        for oss in self.OSSs:
            oss.report()
        print
        for data in (self.Read, self.Write, self.Bulk):
            if data is None:
                handleError(self,
                            BulkNoDataError,
                            "Bulk.report(): - Warning - No data")
                # not reached
            if data.Stats.total is None:
                handleError(self,
                            BulkNoStatsError,
                            "Bulk.report(): - Warning - %s Stats not calculated" % data.name)
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
        if not self.CPU is None:
            self.CPU.header()
            self.CPU.report()

    def doMask(self, mask):
        """
        'mask' is a string representation of the values we want to suppress.
        It can be 'mincpu=x1,maxcpu=x2,minval=y1,maxval=y2'. The x1,x2,y1,y2
        values are all from 0 to 100 (percent), and then scaled to the range
        in the y direction as needed. Any unspecified value takes its default:
        mincpu=0.0
        maxcpu=100.0
        minval=0.0
        maxval=np.max()
        If max is less than min then the sense is reversed - i.e. the step
        must be outside the given range.
        If the values range is constrained at all it masked based on the
        OSS values, which are the sum of Read and Write values.
        """
        mincpu = 0.0
        maxcpu = 100.0
        minval = 0.0
        maxval = 100.0
        bb = dict(item.split("=") for item in mask.split(","))
        if 'mincpu' in bb:
            mincpu = float(bb['mincpu'])
        if 'maxcpu' in bb:
            maxcpu = float(bb['maxcpu'])
        if 'minval' in bb:
            minval = float(bb['minval'])
        if 'maxval' in bb:
            maxval = float(bb['maxval'])
        minval *= np.max(self.Bulk.Values)/100.0
        maxval *= np.max(self.Bulk.Values)/100.0
        if self.Debug == True:
            self.DebugMessages += "cpu[%.0f,%.0f],values[%.0f,%.0f]" % (mincpu,
                                                        maxcpu,
                                                        minval,
                                                        maxval)
        self.CPU.Values = ma.array(self.CPU.Values)
        self.CPU.Values.mask = False
        self.Write.Values = ma.array(self.Write.Values)
        self.Write.Values.mask = False
        self.Read.Values = ma.array(self.Read.Values)
        self.Read.Values.mask = False
        self.Bulk.Values = ma.array(self.Bulk.Values)
        self.Bulk.Values.mask = False
        if mincpu < maxcpu:
            cpu_indices = np.logical_or((self.CPU.Values < mincpu),
                                        (self.CPU.Values > maxcpu))
        else:
            cpu_indices = np.logical_and((self.CPU.Values < mincpu),
                                         (self.CPU.Values > maxcpu))
        if minval < maxval:
            val_indices = np.logical_or((self.Bulk.Values < minval),
                                        (self.Bulk.Values > maxval))
        else:
            val_indices = np.logical_and((self.Bulk.Values < minval),
                                         (self.Bulk.Values > maxval))
        if (mincpu < maxcpu) and (minval < maxval):
            indices = np.logical_or(cpu_indices, val_indices)
        else:
            indices = np.logical_and(cpu_indices, val_indices)
        self.CPU.Values.mask = indices
        self.Read.Values.mask = indices
        self.Write.Values.mask = indices
        self.Bulk.Values.mask = indices

# End of class Bulk
#*******************************************************************************
