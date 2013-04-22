"""
Copyright University of California, 2012
author: Andrew Uselton, Lawrence Berekeley National Lab, acuselton@lbl.gov

An OSS object has one or more OST objects to account for the devises
mounted via that server. There is also a cumulative Counter object
for the sum across all OSTs for read, for write, and for the sum of the
two. In addition there is a CPU object to hold CPU utilization dat for
the server.
"""

import MySQLdb
import numpy as np
import numpy.ma as ma

from pyLMT import Counter, CPU, OST, Timestamp, TimeSteps, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class OSSError(Error):
    """
    Generic Error for problems with OSS objects. 
    """

class OSSQueryOSTsError(OSSError):
    """
    Something went wrong while trying to query the DB for the OSTs
    on this OSS.
    """

class OSSNoSuchOSTError(OSSError):
    """
    Atempt to act on non-existent OST. 
    """

class OSSMissingParamsError(OSSError):
    """
    You must suply a begin and end pair of Timestamp objects for the
    data query.
    """

class OSSNoConnectionError(OSSError):
    """
    You have to have a MySQLdb connectioon to get any data.
    """

class OSSQueryError(OSSError):
    """
    Something went wrong while trying to query the DB for the OST
    data.
    """

class OSSNoStepsError(OSSError):
    """
    In most cases you can't do anything without a TimeSteps object. 
    """

class OSSNoCPUDataError(OSSError):
    """
    There was no data from the CPU query. 
    """

class OSSNoDataError(OSSError):
    """
    There is no data. 
    """

class OSSNoStatsError(OSSError):
    """
    The statistics summary has not been calculated.
    """

class OSSSummaryDataError(OSSError):
    """
    Something happened while trying to insert summary values
    back into the DB. Do you have write privilages?
    """

class OSSBadModeError(OSSError):
    """
    The mode passed around should be one of 'Read', 'Write', or 'Both'.
    """

#*******************************************************************************
# Begin class OSS
class OSS():
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
        self.DebugModules = {"OST":False, "Timestamp":False, "TimeSteps":False}
        self.ErrorMessages = None
        self.conn = None
        self.ossID = None
        # Here, and below can get clear_data()ed
        self.begin = None
        self.end = None
        self.OSTs = []
        self.OSTDict = {}
        self.Read = Counter.Counter("OSS read", "count/sec")
        self.Write = Counter.Counter("OSS write", "count/sec")
        self.OSS = Counter.Counter("OSS", "count/sec")
        self.ReadHist = None
        self.WriteHist = None
        self.OSSHist = None
        self.HistBins = None
        self.CPU = None
        self.Steps = None
        self.Missing = None
        self.haveData = False
        self.total = 0

    def clear_data(self):
        self.begin = None
        self.end = None
        self.OSTs = []
        self.OSTDict = {}
        self.Read = Counter.Counter("OSS read", "count/sec")
        self.Write = Counter.Counter("OSS write", "count/sec")
        self.OSS = Counter.Counter("OSS", "count/sec")
        self.ReadHist = None
        self.WriteHist = None
        self.OSSHist = None
        self.HistBins = None
        self.CPU = None
        self.Steps = None
        self.Missing = None
        self.haveData = False
        self.total = 0

    def debug(self, module=None):
        if (module is None) or (module == "OSS"):
            self.Debug = not self.Debug
            self.Read.debug()
            self.Write.debug()
            self.OSS.debug()
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
        
    def showStep(self, step):
        self.Read.showStep(step)
        self.Write.showStep(step)
        self.OSS.showStep(step)
        for ost in self.OSTs:
            ost.showStep(step)

    def getNumOSTs(self):
        return(len(self.OSTs))
               
    def register(self, ost, sie, read, write):
        """
        All the sie values should alredy be registered with self.Steps
        """
        if self.Debug == True:
            self.DebugMessages += "OST %s" % self.name
        if self.Steps is None:
            handleError(self,
                        OSSNoStepsError,
                        "OSS.register(): Warning - registering OST %s sie %d without a TimeSteps object" % (ost.name, sie))
            # not reached
        if self.Steps.steps() == 0:
            handleError(self,
                        OSSNoStepsError,
                        "OSS.register(): Warning - registering OST %s sie %d with zero length TimeSteps object" % (ost.name, sie))
            # not reached
        ost.register(sie, read, write)
            
    def getOSTs(self, conn=None):
        """
        This supports accessing just the one OSS, which is really
        only going to happen when this module is called with __main__().

        Get the list of OSTs that are on this OSS using the provided MySQL
        connection 'conn'.
        """
        if conn is None:
            if self.conn is None:
                handleError(self,
                            OSSNoConnectionError,
                            "OSS.getOSTs(): Error - No connection to MySQL DB")
            # not reached
        else:
            self.conn = conn
        if self.Debug == True:
            self.DebugMessages += "OSS.getOSTs(): get the list of OSTs on OSS %s" % self.name
        query = "SELECT * FROM OSS_INFO,OST_INFO WHERE OSS_INFO.OSS_ID=OST_INFO.OSS_ID AND "
        query += "OSS_INFO.HOSTNAME='" + self.name + "'"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % query
            cursor.execute (query)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        OSSQueryOSTsError,
                        "OSS.getOSTs: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        OSSQueryOSTsError,
                        "OSS.getOSTs(): WARNING - No data")
            # not reached
        for row in rows:
            name = row["OST_NAME"]
            self.OSTDict[name] = len(self.OSTs)
            ost = OST.OST(name=name)
            if self.DebugModules["OST"] == True:
                ost.debug()
                ost.debug("TimeSeries")
            self.OSTs.append(ost)
        cursor.close()
        return

    def showOSTs(self):
        for o in self.OSTs:
            print o.name

    def getOST(self, ost=None):
        if ost is None:
            handleError(self,
                        OSSNoSuchOSTError,
                        "OSS.getOST(): Warning - Nothing requested")
        if ost in self.OSTDict:
            return(self.OSTs[self.OSTDict[ost]])
        handleError(self,
                    OSSNoSuchOSTError,
                    "OSS.getOST(): Warning - No OST %s on OSS %s" % (oss, self.name))

    def setSteps(self, Steps):
        self.Steps = Steps
        self.end = self.Steps.end
        self.begin = self.Steps.begin
        for o in self.OSTs:
            o.setSteps(Steps)
        self.Read.setSteps(Steps)
        self.Write.setSteps(Steps)
        self.OSS.setSteps(Steps)
        
    def getData(self):
        """
        This supports accessing just the one OSS, which is really
        only going to happen when this module is called with __main__().
        
        Get data from the MySQL connection 'conn' for the interval
        from self.begin to self.end. begin and end are themselves already 
        Timestamp objects, and are required. 
        """
        if self.Steps is None:
            print "OSS.getData(): Error - You must supply a TimeSteps oject first"
            return
        if self.conn is None:
            handleError(self,
                        OSSNoConnectionError,
                        "OSS.getData(): Error - Please provide a MySQL connection")
            # not reached
        if self.Debug == True:
            self.DebugMessages += "OSS.getData(): get data from %d/%d to %d/%d" % (self.Steps.begin.sie, self.Steps.begin.ts_id, self.Steps.end.sie, self.Steps.end.ts_id)
        query = "SELECT * FROM OSS_INFO,OST_INFO,TIMESTAMP_INFO,OST_DATA WHERE "
        query += "OSS_INFO.OSS_ID=OST_INFO.OSS_ID AND "
        query += "OST_INFO.OST_ID=OST_DATA.OST_ID AND "
        query += "OSS_INFO.HOSTNAME='" + self.name + "' AND "
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
                        OSSQueryError,
                        "OSS.getData: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        OSSQueryError,
                        "OSS.getData(): WARNING - No data")
            # not reached
        # Build up the arrays
        for row in rows:
            name = row['OST_NAME']
            if not name in self.OSTDict:
                handleError(self,
                            OSSNoSuchOSTError,
                            "OSS.getData(): Warning - No OST %s on OSS %s" % (name, self.name))
                # not reached
            o = self.OSTs[self.OSTDict[name]]
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            read = float(row['READ_BYTES'])
            write = float(row['WRITE_BYTES'])
            self.register(o, sie, read, write)
        cursor.close()
        self.setData()
        return

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
        self.stats()
        self.setRead()
        self.setWrite()
        self.setOSS()
        self.haveData = True

    def interpolate(self):
        for o in self.OSTs:
            n = o.interpolate()
            if n == 0:
                if self.Debug == True:
                    self.DebugMessages += "OSS.interpolate(): Warning - No data in %s" % o.name
                continue
        
    def stats(self):
        for o in self.OSTs:
            o.stats()
        
    def setRead(self):
        for o in self.OSTs:
            if self.Read.count == 0:
                self.Read.copy(o.Read)
            else:
                self.Read.add(o.Read)

    def setWrite(self):
        for o in self.OSTs:
            if self.Write.count == 0:
                self.Write.copy(o.Write)
            else:
                self.Write.add(o.Write)

    def setOSS(self):
        for o in self.OSTs:
            if self.OSS.count == 0:
                self.OSS.copy(o.OST)
            else:
                self.OSS.add(o.OST)
                
    def getCPU(self):
        if self.Steps is None:
            print "OSS.getCPU(): Error - You must supply a TimeSteps oject first"
            return
        if self.conn is None:
            handleError(self,
                        BulkNoConnectionError,
                        "Bulk.getCPU(): Error - Please provide a MySQL connection")
            # not reached
        query = "SELECT TIMESTAMP,PCT_CPU FROM TIMESTAMP_INFO,OSS_DATA,OSS_INFO WHERE "
        query += "OSS_DATA.OSS_ID=OSS_INFO.OSS_ID and OSS_INFO.HOSTNAME='"
        query += self.name + "' AND "
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
                        OSSNoCPUDataError,
                        "OSS.queryCPU: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
                # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        OSSNoCPUDataError,
                        "OSS.getCPU(): WARNING - No data")
            # not reached
        # Build up the array
        self.CPU = CPU.CPU(self.name+" CPU utilization")
        if self.Debug == True:
            self.CPU.debug()
        self.CPU.setSteps(self.Steps)
        for row in rows:
            sie = self.Steps.getSie(timestamp=row['TIMESTAMP'])
            if sie is None:
                continue
            self.CPU.register(sie, float(row['PCT_CPU']))
        cursor.close()
        self.setCPU()

    def setCPU(self):
        masked_vals = np.where(self.CPU.Values.mask == True)
        self.Missing = np.zeros(len(self.CPU.Values), dtype=np.int32)
        if ma.count_masked(self.CPU.Values) != 0:
            self.Missing[masked_vals] = 1
        n = self.CPU.interpolate()
        if n == 0:
            handleError(self,
                        OSSNoCPUDataError,
                        "OSS.setCPU(): WARNING - No data")
            # not reached
        self.CPU.stats()
        self.haveData = True
        
    def insertRow(self, sie):
        index = self.Steps.getIndex(sie)
        if index is None:
            handleError(self,
                        OSSNoSuchStepError,
                        "OSS.insertRow(): Error - sie value %d is not in Steps" % sie)
            # not reached
        readSdev = 0
        writeSdev = 0
        read = 0.0
        write = 0.0
        readSq = 0.0
        writeSq = 0.0
        count = 0
        for ost in self.OSTs:
            val = ost.Read.Values[index]
            read += val
            readSq += val*val
            val = ost.Write.Values[index]
            write += val
            writeSq += val*val
            count += 1
        if count > 0:
            ave = read/count
            aveSq = readSq/count
            sdevSq = aveSq - ave*ave
            if sdevSq > 0:
                readSdev = np.sqrt(sdevSq)
            ave = write/count
            aveSq = writeSq/count
            sdevSq = aveSq - ave*ave
            if sdevSq > 0:
                writeSdev = np.sqrt(sdevSq)
        insert  = "insert ignore into OSS_AGGREGATE "
        insert += "(OSS_ID,TS_ID,READ_RATE,WRITE_RATE,"
        insert += "READ_SDEV,WRITE_SDEV) values ("
        insert += "(select OSS_ID from OSS_INFO where HOSTNAME='"
        insert += self.name + "'),"
        insert += "(select TS_ID from TIMESTAMP_INFO where unix_timestamp(TIMESTAMP)="
        insert += str(sie) + "),"
        insert += str(self.Read.Values[index]) + ","
        insert += str(self.Write.Values[index]) + ","
        insert += str(readSdev) + ","
        insert += str(writeSdev) + ")"
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.DebugMessages += "\t%s" % insert
            cursor.execute (insert)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        OSSSummaryDataError,
                        "OSS.insert: Error %d: %s\n%s" % (e.args[0],
                                                          e.args[1],
                                                          insert))
            # not reached
        cursor.close()

    def insertHeader(self):
        header  = "insert ignore into OSS_AGGREGATE "
        header += "(OSS_ID,TS_ID,READ_RATE,WRITE_RATE,"
        header += "READ_SDEV,WRITE_SDEV) values "
        if self.ossID is None:
            query = "select OSS_ID from OSS_INFO,FILESYSTEM_INFO where"
            query += " OSS_INFO.FILESYSTEM_ID=FILESYSTEM_INFO.FILESYSTEM_ID"
            query += " and FILESYSTEM_NAME='"
            query += self.fs + "'"
            query += " and HOSTNAME='"
            query += self.name + "'"
            try:
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
                if self.Debug == True:
                    self.DebugMessages += "\t%s" % query
                cursor.execute (query)
            except MySQLdb.Error, e:
                cursor.close()
                handleError(self,
                            OSSSummaryDataError,
                            "OSS.insertHeader: Error %d: %s\n%s" % (e.args[0],
                                                                    e.args[1],
                                                                    query))
                # not reached
            rows = cursor.fetchall()
            if len(rows) == 0:
                handleError(self,
                            OSSSummaryDataError,
                            "OSS.insertHeader(): Error - failed to determine OSS_ID")
                # not reached
            self.ossID = rows[0]['OSS_ID']
            cursor.close()
        return(header)


    def insertValues(self, sie):
        """
        If anything goes wrong with constructing the values sting
        portion of the insert then this method returns None. It may
        be better to raise an exception. I don't know.
        """
        index = self.Steps.getIndex(sie)
        if index is None:
            return(none)
        ts_id = self.Steps.getTS_ID(index)
        if ts_id is None:
            return(None)
        readSdev = 0
        writeSdev = 0
        read = 0.0
        write = 0.0
        readSq = 0.0
        writeSq = 0.0
        count = 0
        for ost in self.OSTs:
            val = ost.Read.Values[index]
            read += val
            readSq += val*val
            val = ost.Write.Values[index]
            write += val
            writeSq += val*val
            count += 1
        if count == 0:
            return(None)
        ave = read/count
        aveSq = readSq/count
        sdevSq = aveSq - ave*ave
        if sdevSq > 0:
            readSdev = np.sqrt(sdevSq)
        ave = write/count
        aveSq = writeSq/count
        sdevSq = aveSq - ave*ave
        if sdevSq > 0:
            writeSdev = np.sqrt(sdevSq)
        values = "(" + str(self.ossID) + ","
        values += str(ts_id) + ","
        values += str(self.Read.Values[index]) + ","
        values += str(self.Write.Values[index]) + ","
        values += str(readSdev) + ","
        values += str(writeSdev) + ")"
        return(values)
    
    def doInsert(self, insert):
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            if self.Debug == True:
                self.Debug += "\t%s" % insert
            cursor.execute (insert)
        except MySQLdb.Error, e:
            cursor.close()
            handleError(self,
                        OSSSummaryDataError,
                        "OSS.doInsert: Error %d: %s\n%s" % (e.args[0],
                                                    e.args[1],
                                                    insert))
            # not reached
        cursor.close()
        self.conn.commit()

    def report(self):
        if self.Steps is None:
            handleError(self,
                        OSSNoStepsError,
                        "OSS.report(): - Error - No steps")
            # not reached
        print self.name
        for o in self.OSTs:
            o.report()
        print
        for data in (self.Read, self.Write, self.OSS):
            if data is None:
                handleError(self,
                            OSSNoDataError,
                            "OSS.report(): - Warning - No data")
                # not reached
            if data.Stats is None:
                handleError(self,
                            OSSNoStatsError,
                            "OSS.report(): - Warning - %s Stats not calculated" % data.name)
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
        minval *= np.max(self.OSS.Values)/100.0
        maxval *= np.max(self.OSS.Values)/100.0
        if self.Debug == True:
            self.DebugMessages += "cpu[%.0f,%.0f],values[%.0f,%.0f]" % (mincpu,
                                                                        maxcpu,
                                                                        minval,
                                                                        maxval)
        self.CPU.Values = ma.array(self.CPU.Values)
        self.Write.Values = ma.array(self.Write.Values)
        self.Read.Values = ma.array(self.Read.Values)
        self.OSS.Values = ma.array(self.OSS.Values)
        if mincpu < maxcpu:
            cpu_indices = np.logical_or((self.CPU.Values < mincpu),
                                        (self.CPU.Values > maxcpu))
        else:
            cpu_indices = np.logical_and((self.CPU.Values < mincpu),
                                         (self.CPU.Values > maxcpu))
        if minval < maxval:
            val_indices = np.logical_or((self.OSS.Values < minval),
                                        (self.OSS.Values > maxval))
        else:
            val_indices = np.logical_and((self.OSS.Values < minval),
                                         (self.OSS.Values > maxval))
        if (mincpu < maxcpu) and (minval < maxval):
            indices = np.logical_or(cpu_indices, val_indices)
        else:
            indices = np.logical_and(cpu_indices, val_indices)
        self.CPU.Values.mask = indices
        self.Read.Values.mask = indices
        self.Write.Values.mask = indices
        self.OSS.Values.mask = indices
        
    def show(self, mode=None):
        if (mode is None) or (mode == 'Read'):
            print "Reads"
            for step in self.Steps.Steps:
                if not step is ma.masked:
                    self.Read.showStep(self.Steps.getIndex(step))
        if (mode is None) or (mode == 'Write'):
            print "Writes"
            for step in self.Steps.Steps:
                if not step is ma.masked:
                    self.Write.showStep(self.Steps.getIndex(step)+1)
        if mode == 'Both':
            print "OSS"
            for step in self.Steps.Steps:
                if not step is ma.masked:
                    self.OSS.showStep(self.Steps.getIndex(step)+1)

    def CalcSdev(self, mode, scale=1024.0*1024.0):
        sum   = np.zeros_like(self.Steps.Steps, dtype=np.float64)
        sumSq = np.zeros_like(self.Steps.Steps, dtype=np.float64)
        count = 0
        if not ((mode == 'Both') or (mode == 'Read') or
                (mode == 'Write')):
            handleError(self,
                        OSSBadModeError,
                        "OSS.CalcSdev(): Warning - unrecognized mode %s" % mode)
            # not reached
        for ost in self.OSTs:
            if mode == 'Both':
                values = ost.Both.Values/scale
            if mode == 'Read':
                values = ost.Read.Values/scale
            if mode == 'Write':
                values = ost.Write.Values/scale
            sum += values
            sumSq += values*values
            count += 1
        if count == 0:
            handleError(self,
                        OSSNoOSTsError,
                        "OSS.CalcSdev(): Warning - No OSTs to calculate standard deviation on")
            # not reached
        ave = sum/count
        aveSq = sumSq/count
        sdevSq = aveSq - ave*ave
        sdevSq[sdevSq < 0.0] = 0.0
        sdev = np.sqrt(sdevSq)
        return(ave, sdev)
        
# End of class OSS
#*******************************************************************************
