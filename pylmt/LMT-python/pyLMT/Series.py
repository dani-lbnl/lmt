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

import numpy as np
import numpy.ma as ma

from pyLMT import Statistics, defaultErrorHandler
from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class SeriesError(Error):
    """
    Various errors can occur in a Series object. Some of them better
    resemble asserts, but I have looked at that facility yet. Soon.
    """

class SeriesRegisterError(SeriesError):
    """
    If you haven't allocated the Values array yet then you can't
    register values in the series.
    """

class SeriesIndexError(SeriesError):
    """
    You can only register values within the bounds of the pre-allocated
    Values array.
    """

class SeriesInterpolateError(SeriesError):
    """
    A Values array entry didn't get unmasked after assignment. This really
    looks more like an assert.
    """

class SeriesDifferentialError(SeriesError):
    """
    One or more Values array entries is still masked, but shouldn't be
    by the time we have interpolated and want to differentiate.This also
    looks more like an assert.
    """

#*******************************************************************************
# Begin class Series
class Series:
    """
    Container class for Series data. It is possible for a particular entry
    to lack a value, in which case it should remain masked.

    The  'name' is arbitrary text for descibing the semantics of the values
    in the series. The 'units' is arbitrary text to give further semantics
    to the series data. Neither is required for this class to carry out
    any of the actions I have in mind.

    """
    def __init__(self, name=None, units=None):
        """
        """
        self.Debug = False
        # I might ought to have a handleDebug method. It may be I want to print immediately
        # sometimes and save up messages other times.
        self.DebugMessages = None
        self.DebugModules = {"Statistics":False}
        self.ErrorMessages = None
        self.name = name
        self.units = units
        # Here, and below can get clear_data()ed
        self.length = 0
        # The set of values in the series
        self.Values = None
        # When more than one Series is combined we can track that here.
        self.count = 0
        # When the time comes we can attach various statistics about the
        # Series.
        self.Stats = None

    def clearData(self):
        self.Values = None
        self.count = 0
        self.Stats = None
        self.DebugMessages = None
        self.ErrorMessages = None

    def debug(self, module=None):
        if (module == None) or (module == "Series"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None
        if module in self.DebugModules:
            self.DebugModules[module] = not self.DebugModules[module]

    def show(self):
        """
        One of the few places where I'll use 'print'. It is being explicitely told
        to do so, so we assume it will be okay.
        """
        print self.name, self.units, self.count, self.Values
        if (not self.Debug is None) and (self.DebugMessages != ''):
            print 'Debug:'
            print self.DebugMessages
        self.Stats.show()

    def setLength(self, length):
        self.Values = ma.empty(length, dtype=np.float64)
        self.Values.mask = True
        self.length = length

    def register(self, index, value):
        """
        """
        if self.Debug == True:
            self.DebugMessages += "Series.register(): name %s, index %d, value %f" % (self.name, index, value)
        if self.Values == None:
            handleError(self,
                        SeriesRegisterError,
                        "Series.register(): Error - No Values array. Did you forget to call setLength?")
            # not reached
        if (index < 0) or (index >= self.length):
            handleError(self,
                        SeriesIndexError,
                        "Series.register(): Error - Index %d out of range for %d long series" % (index, self.length))
            #not reached
        # I could also check that value is a number of some sort
        self.Values[index] = value

    def gapFraction(self, before, at, after):
        return((float(at) - float(before))/
               (float(after) - float(before)))

    def interpolate(self):
        """
        There may be missing observations at the beginning, in the middle,
        or at the end. For the beginning and end propogate the first/last
        value present to the missing values. In the middle interpolate
        across the missing values.
        """
        l = len(self.Values)
        if ma.count_masked(self.Values) == 0:
            # no masked values at all so no work to do
            return(l)
        self.Values.soften_mask()
        if ma.count(self.Values) == 0:
            # no registered values at all, so there was
            # no activity in this interval (or you messed up)
            self.Values[:] = 0
            return(l)
        # The first valid value
        i = np.where(self.Values.mask == False)[0][0]
        if i > 0:
            # there is a gap at the beginning
            self.Values[0:i] = self.Values[i]
        # The last valid value
        j = np.where(self.Values.mask == False)[0][-1]
        if j < l - 1:
            # there is a gap at the end
            self.Values[j+1:l] = self.Values[j]
        while ma.count_masked(self.Values[i:j]) > 0:
            # Find the begining of the first gap still remaining
            k = i + np.where(self.Values.mask[i:j+1] == True)[0][0]
            # Find its end
            m = k + np.where(self.Values.mask[k:j+1] == False)[0][0]
            # interpolate
            self.Values[k:m] = (self.Values[k-1] +
                                ((self.Values[m] - self.Values[k-1])*
                                 ((np.array(range(m-k), dtype=np.float64)+1)/(l-k+1))))
            i = m
        return(l)


    def differential(self):
        """
        The differential is going ot have one fewer elements than the
        original series, so we need to decide how to handle that. Setting
        the first value to zero is reasonable, but reducing the length
        by one is also possible. If we do the latter then that has to be
        worked out with the classes that coordinate with sequences of
        time steps.
        """
        if ma.count_masked(self.Values) != 0:
            print "obj %s: " % self.name
            for val in np.where(self.Values.mask == True)[0]:
                print val
            handleError(self,
                        SeriesDifferentialError,
                        "Series.differential(): Warning - Masked values present. Did you mean to interpolate?")
        self.Values[1:] = np.diff(self.Values)
        self.Values[0] = 0.0

    def setCount(self, count):
        self.count = count

    def getCount(self):
        return(self.count)

    def stats(self):
        """
        For no particularly good reason, the Series becomes valid
        once you've done the stats(). I set the count to 1 and thereafter
        it can be accumulated into aggregate Series objects. You can also
        test its status by checking if getCount returns something bigger
        than 0.
        """
        self.Stats = Statistics.Statistics(self.Values)
        self.count = 1

    def copy(self, series):
        self.Values = series.Values.copy()
        self.count = series.count
        self.length = series.length
        self.Values[np.where(self.Values.mask == True)] = 0
        if self.Stats == None:
            self.Stats = Statistics.Statistics(None)
        self.Stats.copy(series.Stats)

    def add(self, series):
        if self.Values == None:
            self.copy(series)
        else:
            self.Values[np.where(np.logical_and(series.Values.mask == False, self.Values.mask == True))] = 0.0
            self.Values[np.where(series.Values.mask == False)] += series.Values[np.where(series.Values.mask == False)]
            self.count += series.count
        self.stats()

    def getMax(self):
        if not self.Stats is None:
            return(self.Stats.getMax())
        else:
            return(0)

# End of class Series
#*******************************************************************************


