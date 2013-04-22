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

from pyLMT import defaultErrorHandler
from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class TimestampError(Error):
    """
    Base class for errors in dealing with Timestamps.
    """

class TimestampConvertError(TimestampError):
    """
    Something went wrong while trying to convert one form of timestamp
    to another.
    """

class TimestampQueryError(TimestampError):
    """
    Some timestamps do not appear in the LMT DB. When querying one may
    ask for the nearest value before (resp. after), but if that goes
    wrong you get this error.
    """

class TimestampTypeError(TimestampError):
    """
    The Timestamp object can only be initialized with one of:
    [str,int,long,float,np.float64]
    """

import re
import argparse
import time
import datetime as dt
import datetime
import MySQLdb
import numpy as np

#*******************************************************************************

def day(args, fsrc):
    """
    args.day is a string retpresentation of the day of interest.
    We only look at fsrc['conn']
    which should have been established by a call to the configururation
    processing helper.
    begin_ts and end_ts are Timestamp class objects.
    """
    begin_ts = Timestamp(calc_sie(args.day))
    end_ts = Timestamp(begin_ts.sie + 60*60*24)
    if args.verbose == True:
        begin_ts.debug()
        end_ts.debug()
    end_ts.no_later_than(fsrc['conn'])
    begin_ts.no_earlier_than(fsrc['conn'])
    return(begin_ts, end_ts)

#*******************************************************************************

def process_timestamps(args, fsrc):
    """
    args.begin and args.end are string retpresentations of the start
    and end time during whcih to query. We only look at fsrc['conn']
    which should have been established be a call to the configururation
    processing helper.
    begin_ts and end_ts are Timestamp class objects.
    """
    if (args.begin != None) and (args.end != None):
        begin_sie = calc_sie(args.begin)
        end_sie = calc_sie(args.end)
    else:
        end_sie = int(time.time())
        begin_sie = end_sie - 600
    if ('extra' in args) and (args.extra != None):
        begin_sie -= (float(args.extra)/100.0)*(end_sie - begin_sie)
        end_sie   += (float(args.extra)/100.0)*(end_sie - begin_sie)
    begin_ts = Timestamp(begin_sie)
    end_ts   = Timestamp(end_sie)
    if args.verbose == True:
        end_ts.debug()
        begin_ts.debug()
    end_ts.no_later_than(fsrc['conn'])
    begin_ts.no_earlier_than(fsrc['conn'])
    if (end_ts.sie is None) or (end_ts.timestamp is None) or (end_ts.ts_id is None):
        end_ts = None
    if (begin_ts.sie is None) or (begin_ts.timestamp is None) or (begin_ts.ts_id is None):
        begin_ts = None
    return(begin_ts, end_ts)

#*******************************************************************************
def dst(sie):
    """
    Figure out if the timestamp is during daylight savings time or not
    """
    d = dt.datetime.fromtimestamp(sie)
    start = dt.datetime(d.year, 4, 1)
    start_dt = start - dt.timedelta(days=start.weekday() + 1)
    end = dt.datetime(d.year, 11, 1)
    end_dt = end - dt.timedelta(days=end.weekday() + 1)
    if start_dt <= d.replace(tzinfo=None) < end_dt:
        return(True)
    else:
        return(False)

#*******************************************************************************
# These functions translate between the seconds in epoch timestamp and
# the "YYYY-MM-DD HH:MM:SS" text format.

# from sie to text
def format_timestamp(sie):
    return(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sie)))

#*******************************************************************************
# from text to sie
def calc_sie(timestamp):
    """
    The LMT DB TIMESTAMP field of the TIMESTAMP_INFO table is of type
    datetime.datetime. Seconds in epoch (sie) can be calculated from such
    an object directly.

    If the timestamp is an numeric type then treat it as already sie and
    just return it as an int (or long is fine).

    If timestamp is a string that consists entirely of digits treat it as
    a string representatin of sie. Otherwise see if datetime can parse it
    according to one of the provided formats.

    fmt[0] = '%Y-%m-%d %H:%M:%S'
    fmt[1] = '%m-%d %H:%M:%S'
    fmt[2] = '%Y-%m-%d %H:%M'
    fmt[3] = '%m-%d %H:%M'
    fmt[4] = '%Y-%m-%d %H'
    fmt[5] = '%d %H:%M:%S'
    fmt[6] = '%Y-%m-%d'
    fmt[7] = '%m-%d %H'
    fmt[8] = '%d %H:%S'
    fmt[9] = '%H:%M:%S'
    fmt[10] = '%m-%d'
    fmt[11] = '%d %H'
    fmt[12] = '%H:%M'

    Failing all of these
    """
    sie_struct = None
    if (type(timestamp) == int) or (type(timestamp) == long):
        return(timestamp)
    elif type(timestamp) == np.float64:
        return(int(timestamp))
    elif type(timestamp) == datetime.datetime:
        try:
            sie_struct = timestamp.timetuple()
        except:
            raise TimestampConvertError("Timestamp.calc_sie(): Failed to get timetuple from timestamp")
    elif type(timestamp) == str:
        if re.match('^\d+$', timestamp):
            return(int(timestamp))
        struct = None
        today = dt.datetime.today()
        formats = ['%Y-%m-%d %H:%M:%S', #0
                   '%m-%d %H:%M:%S',    #1
                   '%Y-%m-%d %H:%M',    #2
                   '%m-%d %H:%M',       #3
                   '%Y-%m-%d %H',       #4
                   '%d %H:%M:%S',       #5
                   '%Y-%m-%d',          #6
                   '%m-%d %H',          #7
                   '%d %H:%M',          #8
                   '%H:%M:%S',          #9
                   '%m-%d',             #10
                   '%d %H',             #11
                   '%H:%M']             #12
        defaults = [timestamp,
                    str(today.year)+'-'+timestamp,
                    timestamp,
                    str(today.year)+'-'+timestamp,
                    timestamp,
                    str(today.year)+'-'+str(today.month)+'-'+timestamp,
                    timestamp,
                    str(today.year)+'-'+timestamp,
                    str(today.year)+'-'+str(today.month)+'-'+timestamp,
                    str(today.year)+'-'+str(today.month)+'-'+str(today.day)+' '+timestamp,
                    str(today.year)+'-'+timestamp,
                    str(today.year)+'-'+str(today.month)+'-'+timestamp,
                    str(today.year)+'-'+str(today.month)+'-'+str(today.day)+' '+timestamp]
        for index in range(len(formats)):
            fmt = formats[index]
            try:
                struct = dt.datetime.strptime(timestamp, fmt)
                break
            except ValueError:
                pass
        if struct == None:
            print "Timestamp.calc_sie(): Failed to get time_struct from timestamp %s" % timestamp
        else:
            #print index, fmt, struct, type(struct)
            # Check if the format needs to have the year or the month supplied
            if index in [0,1,5,9]:
                sie_struct = time.strptime(defaults[index], formats[0])
            if index in [2,3,8,12]:
                sie_struct = time.strptime(defaults[index], formats[2])
            if index in [4,7,11]:
                sie_struct = time.strptime(defaults[index], formats[4])
            if index in [6,10]:
                sie_struct = time.strptime(defaults[index], formats[6])
            #print index, fmt, sie_struct, type(sie_struct)
    else:
        return(None)
    return int(time.mktime(sie_struct))

#*******************************************************************************
# begin class Timestamp

class Timestamp:
    """
    The TIMESTAMP_INFO table's TIMESTAMP field is returned to Python as
    an object of type 'datetime.datetime'.
    Timestamp handling: TS_ID, and TIMESTAMP from the DB
    and sie (seconds in epoch) corresponding to the TIMESTAMP.
    The format of a TIMESTAMP is '+%Y-%m-%d %H:%M:%S' (using
    the 'date' command syntax, or the 19 character string:
    'yyyy-mm-dd hh:mm:ss'
    Keep in mind that the MySQLdb interface returns TIMESTAMP
    as a type datetime.datetime object, not a string.

    """
    count = 0

    def __init__(self, SIE = None):
        """
        By default get the current time. If you specify SIE keep in
        mind whether it came in as a stirng or an integer.
        """
        self.Debug = False
        self.DebugMessages = None
        self.ErrorMessages = None
        if SIE == None:
            self.sie = int(time.time())
        else:
            if not ((type(SIE) is str) or (type(SIE) is int) or (type(SIE) is long) or (type(SIE) is float) or (type(SIE) is np.float64)):
                handleError(self,
                            TimestampTypeError,
                            "Timestamp.__init__(): type %s of provided SIE not recognized" % type(SIE))
            if type(SIE) is str:
                self.sie = int(SIE)
            if (type(SIE) is float) or (type(SIE) is np.float64):
                self.sie = int(SIE)
            if (type(SIE) is int) or (type(SIE) is long):
                self.sie = SIE
        self.format()
        # This is a datetime.datetime object like what would be returned from
        # a query to the LMT DB. It is blank here, since we do not have a
        # connection to a DB (for now).
        self.timestamp = None
        self.ts_id = self.count
        self.count += 1

    def debug(self):
        self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def format(self):
        self.timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.sie))
        if self.Debug == True:
            self.DebugMessages += "Timestamp.format(): "
            self.show()

    def Sie(self):
        """
        The timestamp returned by a query of the TIMSTAMP_INFO table
        is a datetime.datetime object, from which you can get the sie
        value.
        """
        try:
            sie_struct = self.timestamp.timetuple()
        except:
            handleError(self,
                        TimestampConvertError,
                        "Timestamp.Sie(): Failed to get time_struct from timestamp")
            # not reached
        try:
            self.sie = int(time.mktime(sie_struct))
        except:
            handleError(self,
                        TimestampConvertError,
                        "Timestamp.Sie(): Failed to caclulate sie from time_struct")
            # not reached
        if self.Debug == True:
            self.DebugMessages += "Timestamp.Sie(): "
            self.show()

    def no_later_than(self, conn):
        """
        Get the timestamp entry from the 'conn' DB.
        """
        try:
            cursor = conn.cursor()
            query = "SELECT * FROM TIMESTAMP_INFO WHERE TS_ID=(SELECT max(TS_ID) FROM TIMESTAMP_INFO WHERE TIMESTAMP <='" + self.timestr + "')"
            cursor.execute (query)
            row = cursor.fetchone()
            if row is None:
                self.ts_id = None
                self.sie = None
                self.timestamp = None
                return
            self.ts_id = row[0]
            self.timestamp = row[1]
            # the resulting time may be different from the value requested
            self.Sie()
            self.format()
        except MySQLdb.Error, e:
            message = "TimeStamp.no_later_than: Error %d: %s" % (e.args[0], e.args[1])
            message += "TimeStamp.no_later_than: Failed to get timestamp SIE = "+str(self.sie)+ " (" + self.timestr + ")"
            handleError(self,
                        TimestampQueryError,
                        message)
            # not reached
        cursor.close()
        if self.Debug == True:
            self.DebugMessages += "Timestamp.no_later_than(): "
            self.show()

    def no_earlier_than(self, conn):
        """
        By default get the currently latest timestamp entry from the 'conn' DB.
        If you specify TS_ID then that entry needs to be present. If you
        specify TIMESTAMP or SIE then the last TS_ID before or at that
        time will be returned.
        """
        try:
            cursor = conn.cursor()
            query = "SELECT * FROM TIMESTAMP_INFO WHERE TS_ID=(SELECT min(TS_ID) FROM TIMESTAMP_INFO WHERE TIMESTAMP >='" + self.timestr + "')"
            cursor.execute (query)
            row = cursor.fetchone()
            if row is None:
                self.ts_id = None
                self.sie = None
                self.timestamp = None
                return
            self.ts_id = row[0]
            self.timestamp = row[1]
            # the resulting time may be different from the value requested
            self.Sie()
            self.format()
        except MySQLdb.Error, e:
            message = "TimeStamp.no_earlier_than: Error %d: %s" % (e.args[0], e.args[1])
            message += "TimeStamp.no_earlier_than: Failed to get timestamp SIE = "+str(self.sie)+ " (" + self.timestr + ")"
            handleError(self,
                        TimestampQueryError,
                        message)
            # not reached
        cursor.close()
        if self.Debug == True:
            self.DebugMessages += "Timestamp.no_earlier_than(): "
            self.show()


    def show(self):
        try:
            print "TS_ID", self.ts_id,
            print "TIMESTAMP", self.timestamp,
            print "SIE", self.sie
        except:
            print "Timestamp.show(): something is wrong with this timestamp"

# End of class Timestamp
#*******************************************************************************
