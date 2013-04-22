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

import sys
import MySQLdb
import numpy as np
import numpy.ma as ma

from pyLMT import CPU, Operation, Timestamp, TimeSteps, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class MDSError(Error):
    """
    Generic Error for problems with MDS objects.
    """

class MDSNoStepsError(MDSError):
    """
    Most things require that the Steps attribute be set (to a TimeStepes object).
    """

class MDSNoConnectionError(MDSError):
    """
    You need a MySQLdb connection to do any useful querying.
    """

class MDSQueryOpsError(MDSError):
    """
    Something went wrong findout out what the Operations are.
    """

class MDSMissingParamsError(MDSError):
    """
    You have to have a 'begin' and an 'end'. They are Timestamp Objects.
    """

class MDSQueryError(MDSError):
    """
    Something went wrong while trying to query the DB for the Operations
    data.
    """

class MDSNoDataError(MDSError):
    """
    The query returned an empty result.
    """

class MDSNoSuchOSSError(MDSError):
    """
    The Operation wasn't not found.
    """

class MDSQueryCPUError(MDSError):
    """
    Something went wrong while trying to query the DB for the CPU
    utilization data.
    """

class MDSQuickQueryError(MDSError):
    """
    Something went wrong while trying to query the DB for the Operations
    data.
    """

class MDSSummaryDataError(MDSError):
    """
    The query returned an empty result.
    """

class MDSNoStatsError(MDSError):
    """
    The time series statistics have not been calculated.
    """

class MDSNoSolutionError(MDSError):
    """
    No Solution for Ax = y. Perhaps it was not attempted?
    """

#*******************************************************************************
# Begin class MDS
class MDS:
    """
    Container class for MDS data.
    """
    def __init__(self, host="Hopper", fs="scratch"):
        """
        """
        self.Debug = False
        self.DebugMessages = None
        if (not host is None) and (type(host) is str):
            self.name = host
        else:
            self.name = ''
        if (not fs is None) and (type(fs) is str):
            self.name += ' ' + fs
        self.DebugModules = {"Timestamp":False, "Operation":False, "TimeSteps":False}
        self.host = host
        self.mds = fs
        self.conn = None
        self.mdsID = None
        # Here, and below can get clear_data()ed
        self.begin = None
        self.end = None
        self.Ops = []
        self.OpsDict = {}
        self.haveData = False
        self.Steps = None
        self.MDS = None
        self.CPU = None
        self.MDSHist = None
        self.HistBins = None
        self.x = None
        self.total = 0

    def clear_data(self):
        self.begin = None
        self.end = None
        self.Ops = []
        self.OpsDict = {}
        self.haveData = False
        self.Steps = None
        self.MDS = None
        self.CPU = None
        self.MDSHist = None
        self.HistBins = None
        self.x = None
        self.total = 0

    def debug(self, module=None):
        if (module is None) or (module == "MDS"):
            self.Debug = not self.Debug
        if module == "Timestamp":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "Operation":
            self.DebugModules[module] = not self.DebugModules[module]
        if module == "TimeSteps":
            self.DebugModules[module] = not self.DebugModules[module]
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def opsFromDB(self, conn=None):
        if conn is None:
            if self.conn is None:
                handleError(self,
                            MDSNoConnectionError,
                            "MDS:opsFromDB() Error - No db connection")
                # not reached
        else:
            self.conn = conn
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            query = "SELECT * FROM OPERATION_INFO"
            cursor.execute (query)
            rows = cursor.fetchall()
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        MDSQueryOpsError,
                        "MDS.opsFromDB: Error %d: %s" % (e.args[0], e.args[1]))
            # not reached
        for row in rows:
            self.OpsDict[row['OPERATION_NAME']] = len(self.Ops)
            op = Operation.Operation(name=row['OPERATION_NAME'],
                                     units=row['UNITS'])
            self.Ops.append(op)
            if self.Debug == True:
                op.debug()
        cursor.close()
        self.MDS = Operation.Operation("all MDS ops", "count")

    def showOps(self):
        for op in self.Ops:
            print "%s %s" % (op.name, op.units)

    def getOp(self, name=None):
        if not name in self.OpsDict:
            return(None)
        return(self.Ops[self.OpsDict[name]])

    def setSteps(self, Steps):
        self.Steps = Steps
        self.begin = self.Steps.begin
        self.end = self.Steps.end
        for op in self.Ops:
            op.setSteps(Steps)
        self.MDS.setSteps(Steps)

    def getNumOps(self):
        return(len(self.Ops))

    def getData(self):
        """
        Get data from the self.conn for the interval from Steps.begin to Steps.end.
        begin and end are themselves already Timestamp objects, and are required.
        Populate MDSData.
        """
        if self.Steps is None:
            print "MDS.getData(): Error - You must supply a TimeSteps oject first"
            return
        if self.conn == None:
            handleError(self,
                        FSNoConnectionError,
                        "FS.getData(): Error - Please provide a MySQL connection")
            # not reached
        if self.Debug == True:
            self.MDS.debug()
        if self.Debug == True:
            self.DebugMessages += "MDS.getData(): get data from %d/%d to %d/%d" % (self.Steps.begin.sie, self.Steps.begin.ts_id, self.Steps.end.sie, self.Steps.end.ts_id)
        query = "SELECT * FROM OPERATION_INFO,TIMESTAMP_INFO,MDS_OPS_DATA WHERE "
        query += "OPERATION_INFO.OPERATION_ID=MDS_OPS_DATA.OPERATION_ID AND "
        query += "TIMESTAMP_INFO.TS_ID=MDS_OPS_DATA.TS_ID AND TIMESTAMP_INFO.TIMESTAMP >= '"
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
                        MDSQueryError,
                        "MDS.queryData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        MDSNoDataError,
                        "MDS.queryData(): WARNING - No data")
            # not reached
        # Build up the arrays.
        for row in rows:
            name = row['OPERATION_NAME']
            if not name in self.OpsDict:
                handleError(self,
                            MDSNoSuchOpError,
                            "MDS.queryData(): Error - Name %s is not among listed operations. Skipping" % name)
                # not reached
            op = self.Ops[self.OpsDict[name]]
            sie = op.Steps.getSie(timestamp=row['TIMESTAMP'])
            # profile data (with SUM and SUMSQUARES != 0.0) can't be handled
            # with this code.
            # Some operations get data posted but it is always 0.0, so skip those.
            if (row['SUM'] != 0.0) or (row['SAMPLES'] == 0.0):
                continue
            op.register(sie, float(row['SAMPLES']))
        cursor.close()
        for op in self.Ops:
            if (op.Values is None) or (op.Steps is None) or (op.Steps.steps() == 0):
                continue
            n = op.interpolate()
            if n == 0:
                if self.Debug == True:
                    self.DebugMessages += "op %s has no data" % op.name
                continue
            op.stats()
            self.MDS.add(op)
        self.MDS.stats()
        if (not self.MDS is None) and (self.MDS.Values is None):
            if self.Debug == True:
                self.DebugMessages += "MDS.getData(): Warning - No MDS (aggregate) data"
            return
        self.haveData = True
        return

    def getCPU(self):
        if self.Steps is None:
            print "MDS.getCPU(): Error - You must supply a TimeSteps oject first"
            return
        self.CPU = CPU.CPU("MDS CPU utilization")
        if self.Debug == True:
            self.CPU.debug()
        self.CPU.setSteps(self.Steps)
        query = "SELECT TIMESTAMP,PCT_CPU FROM TIMESTAMP_INFO,MDS_DATA WHERE "
        query += "TIMESTAMP_INFO.TS_ID=MDS_DATA.TS_ID AND TIMESTAMP_INFO.TIMESTAMP >= '"
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
                        MDSQueryCPUError,
                        "MDS.queryCPU: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        MDSQueryCPUError,
                  "MDS.queryCPU(): WARNING - No data")
            # not reached
        # Build up the array
        for row in rows:
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            if sie is None:
                continue
            self.CPU.register(sie, float(row['PCT_CPU']))
        cursor.close()
        n = self.CPU.interpolate()
        if n == 0:
            handleError(self,
                        MDSQueryCPUError,
                        "MDS.queryCPU: Warning - No data")
            # not reached
        self.CPU.stats()

    def attribute(self):
        """
        Set up m by n+1 matrix A with m = steps - 1, n = num ops and
        a_{m,n} = op[m].Values[n]. y = CPU.Values. Solve for x. There is a
        bonus op at 'n' that has a_{i,n} = 1 for all i. That models
        CPU overhead not attributable to any operation.
        """
        if (self.Steps is None) or (self.Steps.steps() == 0):
            handleError(self,
                        MDSNoStepsError,
                        "MDS.attribute(): Warning - No steps")
            # not reached
        if (self.CPU is None) or (self.CPU.Values is None) or (len(self.CPU.Values) == 0):
            handleError(self,
                        MDSNoDataError,
                        "MDS.attribute(): Warning - No CPU data")
            # not reached
        y = self.CPU.Values
        m = self.Steps.steps()
        n = 0
        total = 0
        for op in self.Ops:
            if ((op.Values is None) or (op.Steps is None) or
                (op.Steps.steps() == 0) or (op.Stats is None)):
                continue
            self.total += op.Stats.total
            n += 1
        if n == 0:
            handleError(self,
                        MDSNoDataError,
                        "MDS.attibute(): Warning - No ops with data")
            # not reached
        A = np.empty((m, n+1), dtype=np.float64)
        i = 0
        for op in self.Ops:
            if ((op.Values is None) or (op.Steps is None) or
                (op.Steps.steps() == 0) or (op.Stats is None)):
                continue
            A[:,i] = op.Values
            i += 1
        A[:,n] = np.ones(m, dtype=np.float64)
        self.x, residues, rank, singulars = np.linalg.lstsq(A, y)

    def quickRegister(self, sie, ops):
        index = self.Steps.getIndex(sie)
        self.MDS.Values[index] = ops

    def getQuickData(self, conn=None):
        """
        Get previously aggregated data from the MySQL connection 'conn'
        for the interval from Steps.begin to Steps.end. begin and end are themselves
        already Timestamp objects, and are required.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            MDSNoConnectionError,
                            "MDS.getQuickDate(): Error - No connection to MySQL DB")
                # not reached
        else:
            self.conn = conn
        self.MDS = Operation.Operation("all MDS ops", "count")
        if self.Debug == True:
            self.MDS.debug()
        if self.Debug == True:
            self.DebugMessages += "MDS.getQuickData(): get data from %d/%d to %d/%d" % (self.Steps.begin.sie, self.Steps.begin.ts_id, self.Steps.end.sie, self.Steps.end.ts_id)
        query = "SELECT * FROM MDS_AGGREGATE,TIMESTAMP_INFO WHERE "
        query += "TIMESTAMP_INFO.TS_ID=MDS_AGGREGATE.TS_ID "
        query += "AND TIMESTAMP_INFO.TIMESTAMP >= '"
        query += self.begin.timestr
        query += "' AND TIMESTAMP_INFO.TIMESTAMP <= '"
        query += self.end.timestr
        query += "' order by TIMESTAMP_INFO.TS_ID"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessage += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        MDSQuickQueryError,
                        "MDS.getQuickData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        MDSNoDataError,
                        "MDS.getQuickData(): WARNING - No data")
            # not reached
        # Build up the arrays.
        for row in rows:
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            ops = float(row['OPS_RATE'])
            self.quickRegister(sie, ops)
        cursor.close()
        return

    def insertHeader(self):
        header  = "insert ignore into MDS_AGGREGATE "
        header += "(MDS_ID,TS_ID,OPS_RATE) values "
        if self.mdsID is None:
            query = "select MDS_ID from MDS_INFO,FILESYSTEM_INFO where"
            query += " MDS_INFO.FILESYSTEM_ID=FILESYSTEM_INFO.FILESYSTEM_ID"
            query += " and FILESYSTEM_NAME='"
            query += self.mds + "'"
            try:
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
                if self.Debug == True:
                    self.DebugMessages += "\t%s" % query
                cursor.execute (query)
            except MySQLdb.Error, e:
                cursor.close()
                handleError(self,
                            MDSNoStepsError,
                            "MDS.insertHeader: Error %d: %s\n%s" % (e.args[0],
                                                                    e.args[1],
                                                                    query))
                # not reached
            rows = cursor.fetchall()
            if len(rows) == 0:
                handleError(self,
                            MDSNoDataError,
                            "MDS.insertHeader(): Error - failed to determine MDS_ID")
                # not reached
            self.mdsID = rows[0]['MDS_ID']
            cursor.close()
        return(header)


    def insertValues(self, sie):
        index = self.Steps.getIndex(sie)
        if index is None:
            return(none)
        ts_id = self.Steps.getTS_ID(index)
        if ts_id is None:
            return(None)
        values = "(" + str(self.mdsID) + ","
        values += str(ts_id) + ","
        values += str(self.MDS.Values[index]) + ")"
        return(values)

    def doInsert(self, insert):
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                print "\t%s" % insert
            cursor.execute (insert)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        MDSNoStepsError,
                        "MDS.doInsert: Error %d: %s\n%s" % (e.args[0],
                                                            e.args[1],
                                                            insert))
            # not reached
        cursor.close()
        self.conn.commit()

    def attribute_report(self):
        if self.x is None:
            handleError(self,
                        MDSNoSolutionError,
                        "MDS.attribute_report(): Warning - no solution for x")
            # not reached
        i = 0
        for op in self.Ops:
            if ((op.Values is None) or (op.Steps is None) or
                (op.Steps.steps() == 0) or (op.Stats is None)):
                continue
            if self.x[i] != 0:
                format = "Op %s \t(%d ops\t%8.4f%%):\tx_%d = %f, 100%% at %6.0f"
                print format % (op.name,
                                op.Stats.total,
                                100.0*float(op.Stats.total)/float(self.total),
                                i,
                                self.x[i],
                                (100.0 - self.x[-1])/self.x[i])
            else:
                format = "Op %s \t(%d ops\t%8.4f%%):\tx_%d = %f"
                print format % (op.name,
                                op.Stats.total,
                                100.0*float(op.Stats.total)/float(self.total),
                                i,
                                self.x[i])
            i += 1
        print "Unatributed:\t\t\t\t\tx_%d = %f" % (i, self.x[i])


    def header1(self):
        print "%12s\t%6s\t%8s\t%6s\t%8s\t%8s\t%6s (%8s)\t%8s (%8s)" % ("op",
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
                                                                     "ops",
                                                                     "ops/sec",
                                                                     "ops/sec",
                                                                     "ops/sec",
                                                                     "count",
                                                                     "",
                                                                     "tot>ave",
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

    def header3(self):
        print "%12s\t%6s\t%8s\t%6s\t%8s\t%8s\t%6s (%8s)" % ("",
                                                            "#steps",
                                                            "",
                                                            "max",
                                                            "ave",
                                                            "stdev",
                                                            "#>ave",
                                                            "frac")

    def report(self):
        self.header1()
        self.header2()
        for op in self.Ops:
            if ((op.Steps is None) or (op.Steps.steps() == 0) or (op.Stats is None)):
                continue
            op.report()
        if (not self.MDS is None) and (not self.MDS.Steps is None) and (self.MDS.Steps.steps() > 0):
            self.MDS.report()
        print
        if (not self.CPU is None) and (not self.CPU.Steps is None) and (self.CPU.Steps.steps() > 0):
            self.header3()
            self.header2()
            self.CPU.report()
            self.attribute()
            self.attribute_report()

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
        minval *= np.max(self.MDS.Values)/100.0
        maxval *= np.max(self.MDS.Values)/100.0
        if self.Debug == True:
            self.DebugMessages += "cpu[%.0f,%.0f],values[%.0f,%.0f]" % (mincpu,
                                                                        maxcpu,
                                                                        minval,
                                                                        maxval)
        self.CPU.Values = ma.array(self.CPU.Values)
        self.MDS.Values = ma.array(self.MDS.Values)
        if mincpu < maxcpu:
            cpu_indices = np.logical_or((self.CPU.Values < mincpu),
                                        (self.CPU.Values > maxcpu))
        else:
            cpu_indices = np.logical_and((self.CPU.Values < mincpu),
                                         (self.CPU.Values > maxcpu))
        if minval < maxval:
            val_indices = np.logical_or((self.MDS.Values < minval),
                                        (self.MDS.Values > maxval))
        else:
            val_indices = np.logical_and((self.MDS.Values < minval),
                                         (self.MDS.Values > maxval))
        if (mincpu < maxcpu) and (minval < maxval):
            indices = np.logical_or(cpu_indices, val_indices)
        else:
            indices = np.logical_and(cpu_indices, val_indices)
        self.CPU.Values.mask = indices
        self.MDS.Values.mask = indices

# End of class MDS
#*******************************************************************************
