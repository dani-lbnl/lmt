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

from pyLMT import Series, Statistics, defaultErrorHandler
from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class SeriesError(Error):
    """
    Generic Error for problems with Series2 objects.
    """

class SeriesRegisterError(SeriesError):
    """
    Failure to register values in the Series
    """

class SeriesIndexError(SeriesError):
    """
    Unrecognized index for the Series
    """

class SeriesInterpolateError(SeriesError):
    """
    Problem interpolating over series missing values
    """

class SeriesDifferentialError(SeriesError):
    """
    Problem attempting to take differential of Series
    """

#*******************************************************************************
# Begin class Series
class Series2(Series.Series):
    """
    Container class for Series data where the data is a series of vectors, all
    the same sime. The first array index is for the position in the vector, and
    the second index is for which vector in the series.

    The  'name' is arbitrary text for descibing the semantics of the values
    in the series. The 'units' is arbitrary text to give further semantics
    to the series data. Neither is required for this class to carry out
    any of the actions I have in mind.

    """
    def __init__(self, name=None, units=None):
        """
        """
        Series.Series.__init__(self, name, units)
        self.width = 0

    def clearData(self):
        Series.Series.clearData(self)
        self.width = 0

    def debug(self, module=None):
        if (module == None) or (module == "Series2"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None
        if module in self.DebugModules:
            self.DebugModules[module] = not self.DebugModules[module]

    def show(self):
        """
        I do not know yet what will be wanted here. Certainly not the
        inherited method.
        """
        print self.name, self.units, self.count, self.Values
        if (not self.Debug is None) and (self.DebugMessages != ''):
            print 'Debug:'
            print self.DebugMessages

    def setWidthAndLength(self, width, length):
        self.Values = ma.empty((width, length), dtype=np.float64)
        self.Values.mask = True
        self.width = width
        self.length = length

    def register(self, vIndex, sIndex, value):
        """
        """
        if self.Debug == True:
            self.DebugMessages += "Series2.register(): name %s, index %d, value %f" % (self.name, index, value)
        if self.Values == None:
            handleError(self,
                        SeriesRegisterError,
                        "Series2.register(): Error - No Values array. Did you forget to call setLength?")
            # not reached
        if (vIndex < 0) or (vIndex >= self.width):
            handleError(self,
                        SeriesIndexError,
                        "Series2.register(): Error - Vector index %d out of range for %d long vectors" % (vIndex, self.width))
            #not reached
        if (sIndex < 0) or (sIndex >= self.length):
            handleError(self,
                        SeriesIndexError,
                        "Series2.register(): Error - Series index %d out of range for %d long series" % (sIndex, self.length))
            #not reached
        # I could also check that value is a number of some sort
        self.Values[vIndex,sIndex] = value

    def gapFraction(self, before, at, after):
        return((float(at) - float(before))/
               (float(after) - float(before)))

    def interpolate(self):
        # If there are no masked values there is nothing to do
        if ma.count_masked(self.Values) == 0:
            if self.Debug == True:
                self.DebugMessages += "Series2.interpolate(): No masked values"
            return(len(self.Values))
        for vIndex in range(self.width):
            self.interpolateItem(vIndex)

    def interpolateItem(self, vIndex):
        """
        Interpolate oover the series of values at vector position vIndex.
        There may be missing observations at the beginning, in the middle,
        or at the end. For the beginning and end propogate the first/last
        value present to the missing values. In the middle interpolate
        across the missing values.
        """
        self.Values.soften_mask()
        # At the beginning
        sIndex= 0
        # Find the first non-masked value, if any
        while (sIndex< self.length) and (self.Values[vIndex,sIndex] is ma.masked):
            sIndex+= 1
        if sIndex== self.length:
            # Nothing was ever registered, then this time series probably wasn't in use
            if self.Debug == True:
                self.DebugMessages += "Series2.interpolate(): Warning - There are no valid values over which to interpolate"
            return(0)
        # now go back and copy the value to all the previous ones back to the beginning
        after = sIndex
        before = None
        while sIndex> 0:
            sIndex-= 1
            self.Values[vIndex,sIndex] = self.Values[vIndex,sIndex+1]
        # Resume from the point after the first non-masked value
        sIndex= after+1
        while sIndex< self.length:
            # Find the next masked value, if any
            while (sIndex< self.length) and (not (self.Values[vIndex,sIndex] is ma.masked)):
                sIndex+= 1
            if sIndex== self.length:
                # This is the usual exit point
                if self.Debug == True:
                    self.DebugMessages += "Series2.interpolate(): last after value = %d" % after
                    if before != None:
                        self.DebugMessages +=  "Series2.interpolate(): last before value = %d" % before
                return(len(self.Values))
            # We have one or more masked values to interpolate across
            before = sIndex- 1
            # Find the next non-maked value, if any
            while (sIndex< self.length) and (self.Values[vIndex,sIndex] is ma.masked):
                sIndex+= 1
            if sIndex== self.length:
                # The masked value (or sequence of them) is at the end.
                # This will also be an exit point.
                sIndex= before + 1
                while sIndex< self.length:
                    self.Values[vIndex,sIndex] = self.Values[vIndex,before]
                    sIndex+= 1
                # and we're done
                if self.Debug == True:
                    self.DebugMessages +=  "Series2.interpolate(): last after value = %d" % after
                    self.DebugMessages +=  "Series2.interpolate(): last before value = %d" % before
                return(len(self.Values))
            # A masked value (or sequence of them) is in the middle, so
            # interpolate across the gap
            after = sIndex
            for j in range(before+1, after):
                # interpolate
                frac = self.gapFraction(before, j, after)
                self.Values[vIndex,j] = (self.Values[vIndex,before] +
                               frac*(self.Values[vIndex, after] - self.Values[vIndex, before]))
                if self.Values[vIndex,j] is ma.masked:
                    handleError(self,
                                SeriesInterpolateError,
                                "Series2.interpolate(): ERROR - Values[%d,%d] = %f is still masked!" % (vIndex, j, self.Values[vIndex, j]))
                    # not reached
            sIndex= after+1
        # We only reach this point if the next-to-last value is masked and
        # the last is not. Then the above assignment (sIndex= after+1) creates the
        # termination condition for the loop.
        if self.Debug == True:
            self.DebugMessages += "Series2.interpolate(): last after value = %d" % after
            self.DebugMessages +=  "Series2.interpolate(): last before value = %d" % before
        return(len(self.Values))


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
            handleError(self,
                        SeriesDifferentialError,
                        "Series2.differential(): Warning - Masked values present. Did you mean to interpolate?")
        for vIndex in range(self.width):
            self.Values[vIndex, 1:] = np.diff(self.Values[vIndex,:])
            self.Values[vIndex,0] = 0.0

    def setCount(self, count):
        self.count = count

    def getCount(self):
        return(self.count)

    def copy(self, series):
        self.Values = series.Values.copy()
        self.width = series.width
        self.length = series.length
        self.count = series.count

    def add(self, series):
        if self.Values == None:
            self.copy(series)
            self.count = 1
        else:
            self.Values += series.Values
            self.count += series.count

# End of class Series
#*******************************************************************************


