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

from pyLMT import Counter, TimeSeries, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class OperationError(Error):
    """
    Generic Error for problems with Operation objects.
    """

#*******************************************************************************
# Begin class Operation
class Operation(Counter.Counter):
    """
    Container class for Operation data. It is just TimeSeries data with
    the one addition that you can assign the operation a 'weight'. That won't
    do anthing within the TimeSeries, but when you combine several Operations
    you can use the weight to do so.
    """
    def __init__(self, name=None, units=None, weight=1.0):
        self.weight = weight
        TimeSeries.TimeSeries.__init__(self, name, units)

    def clearData(self):
        self.weight = 1.0
        TimeSeries.TimeSeries.clearData(self)

    def setWeight(self, weight):
        """
        Generally you won't know the weight until after you have some data
        to look at. So allow the weight to be set after object creation.
        """
        self.weight = weight

    def getWeight(self):
        """
        """
        return(self.weight)

    def report(self):
        '''
        print "%12s\t%6s\t%8s\t%6s\t%8s\t%8s\t%6s (%8s)\t%8s (%8s)" % ("op",
                                                                       "#steps",
                                                                       "total",
                                                                       "max",
                                                                       "ave",
                                                                       "stdev",
                                                                       "#>ave",
                                                                       "frac",
                                                                       "tot>ave",
                                                                       "frac")
        '''
        if (self.Stats == None) or (self.Stats.total == 0.0):
            return
        format = "%12s\t%6d\t%5.3e\t%6d\t%5.3e\t%5.3e\t%6d (%5.3e)\t%5.3e (%5.3e)"
        print format % (self.name,
                        self.Steps.steps(),
                        self.Stats.total,
                        self.Stats.max,
                        self.Stats.ave,
                        self.Stats.stdev,
                        self.Stats.above,
                        float(self.Stats.above)/float(self.Steps.steps()),
                        self.Stats.totAbv,
                        self.Stats.totAbvFrac)

# End of class Operation
#*******************************************************************************
