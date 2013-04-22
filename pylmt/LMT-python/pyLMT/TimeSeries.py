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
import os
import re
import time
import string
import argparse
import datetime
import traceback
import MySQLdb
import numpy as np
import numpy.ma as ma

from pyLMT import Series, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class TimeSeriesError(Error):
    """
    Generic Error for problems with TimeSeries objects.
    """

class TimeSeriesRegisterError(TimeSeriesError):
    """
    By the time you register an observation you had better already
    have sorted out the TimeSteps object, so that you know how big
    to make arrays.
    """

#*******************************************************************************
# Begin class TimeSeries
class TimeSeries(Series.Series):
    """
    Container class for TimeSeries data. Steps is a TimeSteps object that
    gives the list of seconds-in-epoch (sie) values at which there may be
    data. It is possible for a particular step to lack an observation, in
    which case it should remain masked until we interpolate after registering
    all the values we do get..

    The  'name' is arbitrary text for descibing the semantics of the values
    in the time series. The 'units' is arbitrary text to give further semantics
    to the time series data. Neither is required for this class to carry out
    any of the actions I have in mind.

    There may be values for sie presented in the 'examine()' phase that do not appear
    in the 'register()' phase. If is the case then the missing values need to be
    interpolated.
    """
    def __init__(self, name=None, units=None):
        """
        """
        Series.Series.__init__(self, name, units)
        self.DebugMessages = None
        self.ErrorMessages = None
        self.DebugModules["Timestamp"] = False
        self.DebugModules["TimeSteps"] = False
        self.Steps = None

    def clearData(self):
        Series.Series.clearData(self)
        self.Steps = None

    def debug(self, module=None):
        Series.Series.debug(self, module)
        if (module == None) or (module == "TimeSeries"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def setSteps(self, Steps):
        """
        In a lot of cases we want to be able to set up the TimeSeries object
        (especially it's derived classes) prior to establishing the set of Steps
        theat are of interest.
        """
        self.Steps = Steps
        if not self.Steps is None:
            length = Steps.steps()
        else:
            length = 0
        if length > 0:
            Series.Series.setLength(self, length)

    def showStep(self, step):
        # Here 'step' is an index into the Steps array. In other contexts
        # 'step' might be a Seconds In Epoch value. The calls to getSie()
        # and getIndex() validate that things are set up correctly
        sie = self.Steps.getSie(step)
        index = self.Steps.getIndex(sie)
        if index != step:
            print "TimeSeries.showStep(): Warning - Steps mismatch. step = %d, but index = %d" % (step, index)
        print "index = %d" % index
        print "sie = %d" % sie
        print "Values[%d] = %f" % (index, self.Values[index])

    def show(self):
        if self.Steps == None:
            return
        print self.name, self.units, self.count, self.Steps.steps()
        for i in range(self.Steps.steps()):
            self.showStep(i)
        self.Stats.show()

    def register(self, sie, obs):
        """
        All the sie values should alredy be registered with self.Steps
        """
        if self.Steps == None:
            handleError(self,
                        TimeSeriesRegisterError,
                        "TimeSeries.register(): Warning - registering obs %f at sie %d without a TimeSteps object" % (obs, sie))
            # not reached
        if self.Steps.steps() == 0:
            handleError(self,
                        TimeSeriesRegisterError,
                        "TimeSeries.register(): Warning - registering obs %f sie %d with zero length TimeSteps object" % (obs, sie))
            # not reached
        index = self.Steps.getIndex(sie)
        if (index is None) and (self.Debug == True):
            self.DebugMessages += "TimeSeries.register(): WARNING - Attempt to register an observation at an sie not previously registered: ignoring"
        Series.Series.register(self, index, obs)

    def gapFraction(self, before, at, after):
        """
        Interpolation is identical with the Series object except for
        this calculation. Since the sequence of steps may be uneven
        across the interpolated gap we need to account for that.
        """
        return((float(self.Steps.Steps[at]) -
                float(self.Steps.Steps[before]))/
               (float(self.Steps.Steps[after]) -
                float(self.Steps.Steps[before])))

    def interpolate(self):
        """
        There may be missing observations at the beginning, in the middle,
        or at the end. For the beginning and end propogate the first/last
        value present to the missing values. In the middle interpolate
        across the missing values. The only difference
        """
        return(Series.Series.interpolate(self))

    def differential(self):
        """
        Take the differential and then adjust with the time diffeentials.
        This does not take into account The idea that the time series
        was a counter. That will need to be handled in Classes where
        we know that is the case. See the commented code.
        """
        #This is generally straight-forward, but if we are looking at counters they can get
        #reset once in a while. It is important to preserve the invariants on the one hand and
        #note if and when this has happened on the other.
        #self.Resets = np.where(self.Diff < 0)
        #self.Diff[self.Diff < 0] = 0
        #self.Diff /= self.Steps.Diff
        Series.Series.differential(self)
        # The Steps.Diff will also have one fewer entry (see Series.diffeential)
        self.Values[1:] /= self.Steps.Diff

    def getSteps(self):
        """
        N.B. this returns the array not the number of steps in it.
        cf. Steps.steps().
        """
        return(self.Steps.getSteps())

    def getValues(self):
        return(self.Values)

    def copy(self, series):
        Series.Series.copy(self, series)
        self.Steps = series.Steps

    def add(self, series):
        if self.Steps == None:
            self.setSteps(series.Steps)
            # if we're initializing Steps and Values then we need
            # zero Values out with non-masked values.
            self.Values = np.zeros(series.length, dtype=np.float64)
        Series.Series.add(self, series)

# End of class TimeSeries
#*******************************************************************************
