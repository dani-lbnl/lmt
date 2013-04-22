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

from pyLMT import Series2, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class HistSeriesError(Error):
    """
    Generic Error for problems with HistSeries objects.
    """

class HistSeriesRegisterError(HistSeriesError):
    """
    By the time you register an observation you had better already
    have sorted out the TimeSteps object and the bins array, so that
    you know how big to make arrays.
    """

#*******************************************************************************
# Begin class HistSeries
class HistSeries(Series2.Series2):
    """
    Container class for HistSeries data. Steps is a TimeSteps object that
    gives the list of seconds-in-epoch (sie) values at which there may be
    data. It is possible for a particular step to lack a histogram, or for
    some of the histogram vector entries to be missing, in which case it
    should remain masked until we interpolate after registering all the
    values we do get.

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
        Series2.Series2.__init__(self, name, units)
        self.DebugMessages = None
        self.ErrorMessages = None
        self.DebugModules["Timestamp"] = False
        self.DebugModules["TimeSteps"] = False
        self.Bins = None
        self.Steps = None
        self.count = 0

    def clearData(self):
        Series2.Series2.clearData(self)
        self.Bins = None
        self.Steps = None

    def debug(self, module=None):
        Series.Series.debug(self, module)
        if (module == None) or (module == "HistSeries"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def setBins(self, Bins):
        """
        In a lot of cases we want to be able to set up the HistSeries object
        (especially it's derived classes) prior to establishing the set of Steps
        that are of interest.
        """
        self.Bins = Bins
        if not self.Bins is None:
            numBins = Bins.bins()
        else:
            numBins = 0
        if not self.Steps is None:
            numSteps = self.Steps.steps()
        else:
            numSteps = 0
        if (numBins > 0) and (numSteps > 0):
            Series2.Series2.setWidthAndLength(self, numBins, numSteps)

    def setSteps(self, Steps):
        """
        """
        self.Steps = Steps

    def showStep(self, step):
        # Here 'step' is an index into the Steps array. In other contexts
        # 'step' might be a Seconds In Epoch value. The calls to getSie()
        # and getIndex() validate that things are set up correctly
        sie = self.Steps.getSie(step)
        index = self.Steps.getIndex(sie)
        if index != step:
            print "HistSeries.showStep(): Warning - Steps mismatch. step = %d, but index = %d" % (step, index)
        print "index = %d" % index
        print "sie = %d" % sie
        # We'll want to think more about how to display each historgram vector
        print self.Values[:,step]

    def show(self):
        if self.Steps == None:
            return
        print self.name, self.units, self.count, self.Steps.steps()
        for sIndex in range(self.Steps.steps()):
            self.showStep(sIndex)

    def register(self, bin, sie, obs):
        """
        All the sie values should alredy be registered with self.Steps, and
        each bin value should have an entry in Bins.
        """
        if self.Bins is None:
            handleError(self,
                        HistSeriesRegisterError,
                        "HistSeries.register(): Warning - registering histogram value %f at bin %f without a Bins object" % (obs, bin))
            # not reached
        if self.Bins.bins() == 0:
            handleError(self,
                        HistSeriesRegisterError,
                        "HistSeries.register(): Warning - registering histogram value %f at bin %f with zero length Bins object" % (obs, bin))
            # not reached
        bIndex = self.Bins.getIndex(bin)
        if bIndex is None:
            handleError(self,
                        HistSeriesRegisterError,
                        "HistSeries.register(): Warning - failed to identify bin %f" % bin)
        if self.Steps is None:
            handleError(self,
                        HistSeriesRegisterError,
                        "HistSeries.register(): Warning - registering histogram value %f at sie %d without a TimeSteps object" % (obs, sie))
            # not reached
        if self.Steps.steps() == 0:
            handleError(self,
                        HistSeriesRegisterError,
                        "HistSeries.register(): Warning - registering histogram value %f sie %d with zero length TimeSteps object" % (obs, sie))
            # not reached
        sIndex = self.Steps.getIndex(sie)
        if sIndex is None:
            handleError(self,
                        HistSeriesRegisterError,
                        "HistSeries.register(): Warning - failed to identify step %d" % sie)
        if ((bIndex is None) or (sIndex is None)) and (self.Debug == True):
            self.DebugMessages += "HistSeries.register(): WARNING - Attempt to register an histogram value at an sie and/or bin not previously registered: ignoring"
        Series2.Series2.register(self, bIndex, sIndex, obs)

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
        Series2.Series2.differential(self)
        # The Steps.Diff will also have one fewer entry (see Series.diffeential)
        self.Values[:,1:] /= self.Steps.Diff

    def getSteps(self):
        """
        N.B. this returns the array not the number of steps in it.
        cf. Steps.steps().
        """
        return(self.Steps.getSteps())

    def getBins(self):
        """
        N.B. this returns the array not the number of steps in it.
        cf. Steps.steps().
        """
        return(self.Bins.getBins())

    def getValues(self):
        return(self.Values)

    def copy(self, series):
        Series2.Series2.copy(self, series)
        self.setSteps(series.Steps)
        self.setBins(series.Bins)

    def add(self, series):
        Series2.Series2.add(self, series)

# End of class HistSeries
#*******************************************************************************
