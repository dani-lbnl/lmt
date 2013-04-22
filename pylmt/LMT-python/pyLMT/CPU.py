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

from pyLMT import TimeSeries, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class CPUError(Error):
    """
    Generic Error for problems with CPU objects.
    """

class CPUNoStepsError(CPUError):
    """
    Most operations need for there to be Steps in place before anything
    useful can be done.
    """

class CPUFooError(CPUError):
    """
    """

#*******************************************************************************
# Begin class CPU
class CPU(TimeSeries.TimeSeries):
    """
    Container class for CPU data. It is just TimeSeries data with
    the one addition that you can assign the operation a 'weight'. That won't
    do anthing within the TimeSeries, but when you combine several CPUs
    you can use the weight to do so.
    Be sure to call self.stats()
    """
    def __init__(self, name):
        # We may gather and average several CPU utilization series
        self.Debug = False
        self.DebugMessages = None
        self.ErrorMessages = None
        self.Count = None
        TimeSeries.TimeSeries.__init__(self, name, "percent")

    def debug(self, module=None):
        if (module is None) or (module == "CPU"):
            self.Debug = not self.Debug
        if self.Debug == True:
            self.DebugMessages = ''
        else:
            self.DebugMessages = None

    def clearData(self):
        self.Count = None
        TimeSeries.TimeSeries.clearData(self)

    def initCount(self):
        """
        Once we know the sequence of steps we can initialize the Count array
        to keep track of possible multiple sequences.
        """
        count = self.Steps.steps()
        if count <= 0:
            handleError(self,
                        CPUNoStepsError,
                        "CPU.initCount(): Error - You don't have any steps yest.")
            # not reached
        self.Count = np.ones(count, dtype=np.int32)

    def add(self, cpu):
        """
        If this instance is empty then copy the array and count from 'cpu'.
        Otherwise use this count and the count in 'cpu' to combine the two.
        """
        if self.Steps == None:
            handleError(self,
                        CPUNoStepsError,
                        "CPU.add(): Warning - adding without a TimeSteps object")
            # not reached
        if self.Steps.steps() == 0:
            handleError(self,
                        CPUNoStepsError,
                        "CPU.add(): Warning - adding with zero length TimeSteps object")
            # not reached
        if ma.count_masked(cpu.Values) != 0:
            print "%s still has masked values" % cpu.name
        if self.Count == None:
            self.Count = cpu.Count.copy()
        if self.Values == None:
            self.Values = cpu.Values.copy()
        else:
            self.Values = (self.Values*self.Count + cpu.Values*cpu.Count)/(self.Count + cpu.Count)
            self.Count += cpu.Count

    def header(self):
        print "%12s\t%6s\t%8s\t%6s\t%8s\t%8s\t%6s (%8s)" % ("",
                                                            "#steps",
                                                            "",
                                                            "max",
                                                            "ave",
                                                            "stdev",
                                                            "#>ave",
                                                            "frac")

    def report(self):
        '''
        '''
        if (self.Stats == None) or (self.Stats.total == 0.0):
            return
        print "%12s\t%6d\t\t\t%6d\t%5.3e\t%5.3e\t%6d (%5.3e)" % (self.name,
                                                                 self.Steps.steps(),
                                                                 self.Stats.max,
                                                                 self.Stats.ave,
                                                                 self.Stats.stdev,
                                                                 self.Stats.above,
                                                                 float(self.Stats.above)/float(self.Steps.steps()))

# End of class CPU
#*******************************************************************************
