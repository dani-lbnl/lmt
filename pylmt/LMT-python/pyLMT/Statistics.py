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

from pyLMT import defaultErrorHandler
from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class StatisticsError(Error):
    """
    Generic Error for problems with Statistics objects. I think there is only
    one.
    """

class StatisticsCopyError(StatisticsError):
    """
    Passing None to the Statistics.copy() method is an error.
    """

#*******************************************************************************
# Begin class Statistics
class Statistics:
    """
    Container class for Statistics on an array of values.

    """
    def __init__(self, array):
        """
        """
        self.Debug = False
        self.DebugMessages = None
        self.ErrorMessages = None
        if (array is None) or (len(array) <= 0):
            # Sometimes we just want to get the object and fill it
            # in later, as when we copy from another Statsitics object.
            self.clearData()
            return
        self.total = np.sum(array)
        self.max = np.max(array)
        self.ave = np.average(array)
        self.stdev = np.std(array)
        self.above = len(array[array>self.ave])
        self.totAbv = np.sum(array[array>self.ave])
        if self.total > 0:
            self.totAbvFrac = float(self.totAbv)/float(self.total)
        else:
            self.totAbvFrac = 0

    def clearData(self):
        self.max = None
        self.ave = None
        self.stdev = None
        self.above = None
        self.totAbv = None
        self.totAbvFrac = None

    def debug(self, module=None):
        if (module is None) or (module == "Statistics"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def show(self):
        """
        This is one of the few times we actually call 'print' explicitely.
        We presume the caller knows the consequences.
        """
        print "total      = %f" % self.total
        print "max        = %f" % self.max
        print "ave        = %f" % self.ave
        print "stdev      = %f" % self.stdev
        print "above      = %f" % self.above
        print "totAbv     = %f" % self.totAbv
        print "totAbvFrac = %f" % self.totAbvFrac

    def copy(self, stats):
        if stats is None:
            handleError(self,
                        StatisticsCopyError,
                        "Statistics.copy(): Error - no Statistics to copy")
            # not reached
        self.total = stats.total
        self.max = stats.max
        self.ave = stats.ave
        self.stdev = stats.stdev
        self.above = stats.above
        self.totAbv = stats.totAbv
        self.totAbvFrac = stats.totAbvFrac

    def getMax(self):
        return(self.max)

# End of class Statistics
#*******************************************************************************
