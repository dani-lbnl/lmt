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

from pyLMT import defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class HistBinsError(Error):
    """
    Generic Error for problems with HistBins objects.
    """

class HistBinsExamineError(HistBinsError):
    """
    Even simpler than a TimeSteps object, we just want to remember the
    list of bins that were provided.
    """

class HistBinsRegisterError(HistBinsError):
    """
    Once all the bins have been examined a Bins array is provisioned
    to hold exactly those values. They are put in place one by one, and if
    any array entry fails to get a values that is an error.
    """

#*******************************************************************************
# Begin class HistBins
class HistBins:
    """
    Container class for HistBins data.

    Right now we just grab up the values. We could capture more semanics,
    since most bins collections have some structure, eg., counting in a
    range or powers of two in a range.
    """
    def __init__(self, id, name, description, units):
        """
        """
        self.id = id
        self.name = name
        self.description = description
        self.units = units
        self.Debug = False
        self.DebugMessages = None
        self.ErrorMessages = None
        # Here, and below can get clear_data()ed
        # examine(0 will determine this value
        self.numBins = 0
        # register() should come up with the same
        self.__registered = 0
        # An index into the Bins array
        self.current = None
        # An array of the bin values
        self.Bins = None
        # If we want find the index in Bins for a given bin
        # we need a reverse-lookup
        self.BinDict = {}
        self.HaveData = False

    def clearData(self):
        self.numBins = 0
        self.__registered = 0
        self.current = None
        self.Bins = None
        self.BinDict = {}
        self.HaveData = False

    def debug(self, module=None):
        if (module == None) or (module == "HistBins"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def show(self):
        print self.numBins, self.HaveData
        for bin in range(0, self.numBins ):
            print  self.BinDict[self.Bins[bin]], self.Bins[bin]

    def examine(self, bin):
        """
        We do not initially know how many unique bin values there will be.
        As new bin values are presented add them to the BinDict. We
        should only ever see monotonically increasing bin values. Note
        the earliest and latest.
        """
        if not self.Bins is None:
            handleError(self,
                        HistBinsExamineError,
                        "HistBins.examine(): Warning - New examine() call after timebins registration has begun. Did you mean to clearData()?")
            # not reached
        if bin in self.BinDict:
            return
        self.BinDict[bin] = self.numBins
        self.numBins += 1

    def bins(self):
        return(self.numBins)

    def getIndex(self, bin):
        if not bin in self.BinDict:
            return None
        self.current = self.BinDict[bin]
        return(self.current)

    def getBin(self, index=None):
        if index == None:
            return None
        if not index in range(self.numBins):
            return None
        self.current = index
        return(self.Bins[index])

    def getBins(self):
        """
        N.B. this returns the array, not the number of bins in it.
        cf. bins()
        """
        return(self.Bins)

    def register(self):
        """
        We have all the bin and timestamp values in dictionaries, so just
        build the time bin sequence from them
        """
        self.numBins = len(self.BinDict)
        self.Bins = ma.empty(self.numBins, dtype=np.float64)
        self.Bins.mask = True
        self.Bins.soften_mask()
        # the bins some times emerge in a random order
        # so put them in order, and then preserve that
        count = 0
        for bin,index in sorted(self.BinDict.iteritems()):
            self.Bins[count] = bin
            count += 1
        for count in range(self.numBins):
            self.BinDict[self.Bins[count]] = count
        self.__registered += 1
        if ma.count_masked(self.Bins) != 0:
            handleError(self,
                        HistBinsRegisterError,
                        "HistBins.register(): Warning - registered all %d, but still have %d masked values" % (self.numBins, len(np.nonzero(self.Bins.mask))))
            # not reached
        if self.Debug == True:
            self.DebugMessages += "HistBins.register(): Registred all %d expected values, now to check them and take the Bin differences" % self.numBins
        self.HaveData = True

# End of class HistBins
#*******************************************************************************
