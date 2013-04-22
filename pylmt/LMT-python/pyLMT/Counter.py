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

from pyLMT import Series, TimeSeries, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class CounterError(Error):
    """
    Generic Error for problems with TimeSeries objects.
    """

class CounterDifferentialError(CounterError):
    """
    The Counter object assumes that the Values will be differentiaed
    as soon as they've been interpolated. Thus this call to
    Counter.differential() does nothing and is deprecated.
    """

#*******************************************************************************
# Begin class Counter
class Counter(TimeSeries.TimeSeries):
    """
    Container class for Counter data. This is very nearly identical to
    a TimeSeries object except that the observed values should be
    monotonically increasing. We then want to take the derivative and
    actually use that as our sequence of values. That means when the
    counter is reset we need to note that fact and artificially set
    the differential value to zero.
    """
    def __init__(self, name=None, units=None):
        """
        """
        TimeSeries.TimeSeries.__init__(self, name, units)
        self.DebugModules["TimeSeries"] = False
        self.Missing = None
        self.Resets = None

    def clearData(self):
        TimeSeries.TimeSeries.clearData(self)
        self.Missing = None
        self.Resets = None

    def debug(self, module=None):
        Series.Series.debug(self, module)
        if (module == None) or (module == "Counter"):
            self.Debug = not self.Debug

    def differential(self):
        handleError(self,
                    CounterDifferentialError,
                    "The Counter was already differentiated")

    def interpolate(self):
        """
        The time series gets its differential calculated as well, once
        the interpolation is complete.
        """
        masked_vals = np.where(self.Values.mask == True)
        self.Missing = np.zeros(len(self.Values), dtype=np.int32)
        if ma.count_masked(self.Values) != 0:
            self.Missing[masked_vals] = 1
        count = TimeSeries.TimeSeries.interpolate(self)
        if count == 0:
            return(0)
        if self.Steps == None:
            return(0)
        Series.Series.differential(self)
        self.Values[1:] /= self.Steps.Diff
        self.Resets = np.where(self.Values < 0)
        self.Values[self.Values < 0] = 0
        return(count)

# End of class Counter
#*******************************************************************************
