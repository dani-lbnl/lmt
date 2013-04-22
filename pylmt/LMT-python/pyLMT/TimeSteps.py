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

from pyLMT import Timestamp, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class TimeStepsError(Error):
    """
    Generic Error for problems with TimeSteps objects.
    """

class TimeStepsExamineError(TimeStepsError):
    """
    Timestamps are introduced into a TimeSteps object one at a time.
    They should be in strictly increasing order of seconds in epoch.
    If you get a Timestamp outside the expected range or that is
    prior to one already examined, then that is an error.
    Similarly, it is an error to try to examine new Timestamps
    after the Steps array has already been allocated.
    """

class TimeStepsRegisterError(TimeStepsError):
    """
    Once all the Timestamps have been examined a Steps array is provisioned
    to hold exactly those values. They are put in place one by one, and if
    any array entry fails to get a values that is an error.
    """

class TimeStepsNoConnectionError(TimeStepsError):
    """
    You need a MySQL connection to get any data
    """

class TimeStepsMissingParamsError(TimeStepsError):
    """
    Begin and end are Timestamp object, and must be present to
    specify the interval of interest.
    """

class TimeStepsQueryError(TimeStepsError):
    """
    Something went wrong while trying to query the DB for the number
    of time steps.
    """

class TimeStepsNoStepsError(TimeStepsError):
    """
    Most things require that the Steps attribute be set (to a TimeSteps object).
    """

#*******************************************************************************
# Begin class TimeSteps
class TimeSteps:
    """
    Container class for TimeSteps data.

    'begin' and 'end' are seconds-in-epoch (sie) values and beginTS and endTS
    are timestamps in the form used by the LMT DB: 'YYYY-MM-DD hh:mm;ss'.

    Steps in an array of sie values that goes from 'begin' to 'end'.

    There may be some duplication and out of order values in the raw sequence
    of sie values presented, as when there is a transition to or from daylight
    savings time.

    Sie values in the Steps array are unique and monotonically increasing with
    index. There is a valid sie value at each array index.

    Diff holds the differential of the Steps array, usually all values will
    be 5, but there may be longer gaps in the sequence.

    numSteps = len(Steps)

    The dictionary StepDict holds the index in Steps for each sie value, i.e.:
      Steps[StepDict[sie]] = sie

    Since we don't know in advance what sie values we'll need we have to build
    StepDict incrementally with the examine() method and then fill in the Steps
    array with the register() method..

    """
    def __init__(self):
        """
        """
        self.Debug = False
        self.DebugMessages = None
        self.ErrorMessages = None
        # Here, and below can get clear_data()ed
        # examine(0 will determine this value
        self.numSteps = 0
        # register() should come up with the same
        self.__registered = 0
        # An index into the Steps array
        self.current = None
        # The timestamp (YYYY-MM-DD hh:mm:ss) value for the start of the
        # time series
        self.conn = None
        self.begin = None
        # and for the end
        self.end = None
        # An array of the sie values
        self.Steps = None
        self.Diff = None
        # If we want find the index in Steps for a given sie
        # we need a reverse-lookup
        self.StepDict = {}
        self.TS_IDDict = {}
        self.Timestamps = {}
        self.dst = None
        self.HaveData = False

    def clearData(self):
        self.numSteps = 0
        self.__registered = 0
        self.current = None
        self.conn = None
        self.begin = None
        self.end = None
        self.Steps = None
        self.Diff = None
        self.StepDict = {}
        self.Timestamps = {}
        self.dst = None
        self.HaveData = False

    def debug(self, module=None):
        if (module == None) or (module == "TimeSteps"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def show(self):
        print self.begin, self.end, self.numSteps, self.HaveData
        print  self.StepDict[self.Steps[0]], self.Steps[0]
        for i in range(1, self.numSteps ):
            print  self.StepDict[self.Steps[i]], self.Steps[i], self.Diff[i-1]

    def examine(self, timestamp, sie, ts_id):
        """
        We do not initially know how many unique sie values there will be.
        As new sie values are presented add them to the StepDict. We
        should only ever see monotonically increasing sie values. Note
        the earliest and latest.
        """
        if not self.Steps is None:
            handleError(self,
                        TimeStepsExamineError,
                        "TimeSteps.examine(): Warning - New examine() call after timesteps registration has begun. Did you mean to clearData()?")
            # not reached
        if sie in self.StepDict:
            return
        if self.begin.sie > sie:
            handleError(self,
                        TimeStepsExamineError,
                        "TimeSteps.examine(): Warning - new sie %d is before previously estamblished beginning %d" % (sie, self.begin.sie))
            # not reached
        if self.end.sie < sie:
            handleError(self,
                        TimeStepsExamineError,
                        "TimeSteps.examine(): Warning - new sie %d is after previously estamblished end %d" % (sie, self.end.sie))
            # not reached
        self.StepDict[sie] = self.numSteps
        self.TS_IDDict[sie] = int(ts_id)
        self.Timestamps[timestamp] = sie
        self.numSteps += 1

    def steps(self):
        return(self.numSteps)

    def getIndex(self, sie):
        if not sie in self.StepDict:
            return None
        self.current = self.StepDict[sie]
        return(self.current)

    def getTS_ID(self, index):
        if (not type(index) is int) or (index < 0) or (index >= self.numSteps):
            return(None)
        return(self.TS_IDs[index])

    def getSie(self, index=None, timestamp=None):
        if (index == None) and (timestamp == None):
            return None
        if index != None:
            if not index in range(self.numSteps):
                return None
            self.current = index
            return(self.Steps[index])
        if not timestamp in self.Timestamps:
            return None
        sie = self.Timestamps[timestamp]
        self.current = self.StepDict[sie]
        return(sie)

    def getSteps(self):
        """
        N.B. this returns the array, not the number of steps in it.
        cf. steps()
        """
        return(self.Steps)

    def register(self):
        """
        We have all the sie and timestamp values in dictionaries, so just
        build the time step sequence from them
        """
        self.numSteps = len(self.StepDict)
        self.Steps = ma.empty(self.numSteps, dtype=np.int32)
        self.Steps.mask = True
        self.TS_IDs = ma.empty(self.numSteps, dtype=np.int32)
        self.TS_IDs.mask = True
        self.Steps.soften_mask()
        self.TS_IDs.soften_mask()
        for sie,index in self.StepDict.iteritems():
            self.Steps[index] = sie
            self.TS_IDs[index] = self.TS_IDDict[sie]
        self.__registered += 1
        if ma.count_masked(self.Steps) != 0:
            handleError(self,
                        TimeStepsRegisterError,
                        "TimeSteps.register(): Warning - registered all %d, but still have %d masked values" % (self.numSteps, len(np.nonzero(self.Steps.mask))))
            # not reached
        if self.Debug == True:
            self.DebugMessages += "TimeSteps.register(): Registred all %d expected values, now to check them and take the Step differences" % self.numSteps
        self.Diff = np.diff(self.Steps)
        if (np.min(self.Diff) <= 0) and (self.Debug == True):
            message = "TimeSteps.register(): Warning - negative or zero time differentials at\n"
            message += "indices:" + str(np.where(self.Diff <= 0)) + "values:" + str(self.Diff[np.where(self.Diff <= 0)])
            handleError(self,
                        TimeStepsRegisterError,
                        message)
            # not reached
        self.HaveData = True

    def getDst(self):
        if self.dst != None:
            return(self.dst)
        if (self.begin == None) or (self.end == None):
            return(None)
        self.dst = Timestamp.dst(self.begin)
        if (self.dst != Timestamp.dst(self.end.sie)) and (self.Debug == True):
            self.DebugMessages += "Timestamp.dst(): Warning - The interval spans a change to or from daylight savings time."
        return(self.dst)

    def getTimeSteps(self, begin, end, conn=None):
        """
        Get the steps we'll be dealing with
        """
        if (begin == None) or (end == None):
            handleError(self,
                        TimeStepsMissingParamsError,
                        "TimeSteps.getTimeSteps(): Error - You must supply Timestamp ojects 'begin' and 'end'")
            # not reached
        if conn is None:
            if self.conn is None:
                handleError(self,
                            TimeStepsNoConnectionError,
                            "TimeSteps.getTimeSteps(): Error - Please provide a MySQL connection")
        else:
            self.conn = conn
            # not reached
        if self.Debug == True:
            self.DebugMessages += "TimeSteps.getTimeSteps(): get data size from %d/%d to %d/%d" % (begin.sie, begin.ts_id, end.sie, end.ts_id)
        self.end = end
        self.begin = begin
        query = "SELECT TS_ID,TIMESTAMP FROM TIMESTAMP_INFO WHERE "
        query += "TIMESTAMP >= '"
        query += self.begin.timestr
        query += "' AND TIMESTAMP <= '"
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
                        TimeStepsQueryError,
                        "TimeSteps.getTimeSteps: Error %d: %s\n%s" % (e.args[0], e.args[1], query))
            # not reached
        rows = cursor.fetchall()
        if len(rows) == 0:
            handleError(self,
                        TimeStepsQueryError,
                        "TimeSteps.getTimeSteps(): WARNING - No data")
            # not reached
        # First establish the sequence of time steps involved
        for row in rows:
            self.examine(row['TIMESTAMP'], Timestamp.calc_sie(row['TIMESTAMP']), row['TS_ID'])
        self.register()
        if (self.steps() == 0):
            handleError(self,
                        TimeStepsNoStepsError,
                        "TimeSteps.getTimeSteps(): Warning - No steps")
            # not reached
            return self.steps()

# End of class TimeSteps
#*******************************************************************************
