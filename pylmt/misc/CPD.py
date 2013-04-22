#!/usr/bin/env python
# CPD.py
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
#
#
#   Read in previously caluculated histogram data and plot a graph
# of the cumulative power distribution.

import sys
import os
import string
import argparse
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy import linspace, polyval, polyfit, sqrt, randn
import Histograms

#*******************************************************************************
# Begin class CPD
class CPD:
    """
    Container class for 'cpd' computation. This is the (inverse) cumulative
    power distribution. For each bin, what fraction of the I/O is represented
    by observations at or above that bin.
    """

    def __init__(self, bins = 1250, min = 0.0, max = 2500.0, log = False):
        self.path = None
        self.bins = bins
        self.min = min
        self.max = max
        self.log = log
        self.Done = False
        self.hist = np.zeros((self.bins, 3), dtype=float)
        self.CPD = np.zeros((self.bins, 3), dtype=float)
        self.CPD[:,0] = self.min + np.array(range(self.bins), dtype=float)*(self.max - self.min)/self.bins

    def cpd(self, hist):
        self.hist[:,0] = hist.Histograms[:,0]
        self.hist[:,1] = hist.Histograms[:,1]*hist.Histograms[:,0]
        self.CPD[:,1] = 1 - np.cumsum(self.hist[:,1])/self.hist[:,1].sum()
        self.hist[:,2] = hist.Histograms[:,2]*hist.Histograms[:,0]
        self.CPD[:,2] = 1 - np.cumsum(self.hist[:,2])/self.hist[:,2].sum()
        self.Done = True

    def append(self, append):
        self.CPD[:,1] = self.CPD[:,1] + append.CPD[:,1]
        self.CPD[:,2] = self.CPD[:,2] + append.CPD[:,2]

    def load(self, path = None):
        if path == None:
            if self.path == None:
                self.path = "cpd.data"
        else:
            self.path = path
        self.CPD = np.loadtxt(self.path)
        self.Done = True

    def save(self, path = None):
        if path == None:
            if self.path == None:
                self.path = "cpd.data"
        else:
            self.path = path
        np.savetxt(self.path, self.CPD, fmt='%f\t%f\t%f')

    def plot(self, plot = None, fig = None):
        if fig == None:
            fig = plt.figure()
        if self.log == False:
            plt.plot(self.CPD[:,0], self.CPD[:,1], 'r-', label='read')
            plt.plot(self.CPD[:,0], self.CPD[:,2], 'b-', label='write')
            ax, = fig.get_axes()
            ax.set_ybound(lower = 0.0, upper = 1.0)
        else:
            plt.semilogy(self.CPD[:,0], self.CPD[:,1], 'r-', label='read')
            plt.semilogy(self.CPD[:,0], self.CPD[:,2], 'b-', label='write')
        plt.legend()
        return(fig)

# End of class CPD
#*******************************************************************************

if __name__ == "__main__":
    """
CPD.py <opts>
Options include:
-a <above> Plot the 'above' calculation at the file <above>
-d <data>  Path to directory of previously calculated data to load
            rather than loading ost data
-f <fs>    The file system (default <scratch>)
-h         A help message
-l         Plot using a log y-scale
-p <path>  Plot data to the file at path <plot>
-V         Print the version and exit
-y <bound> y-axis bound

   This module implements a particula model for the values x, and y and
 invokes a curve-fit sover for that model. It is only invoked in the commented
 out section of events_plot.py. The __main__ below recapitulates that activity.
    """
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access LMT data to do the cpd calculation')
    parser.add_argument('-a', '--above', default=None, type=str, help='Save a plot of the above calculation in file above')
    parser.add_argument('-d', '--data', default=None, type=str, help='Previously calculated histogram data')
    parser.add_argument('-m', '--mb', default=None, type=float, help='Print the (inverse) cumulative power distribution at x = mb')
    parser.add_argument('-p', '--plot', default=None, type=str, help='Save a plot of the CPD in file plot')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    hist = args.data + "/histogram.data"
    H = Histograms.Histograms()
    H.load(hist)
    C = CPD()
    C.cpd(H)
    if args.mb != None:
        bin = (args.mb - H.min)*H.bins/(H.max - H.min)
        print "read CPD =", C.CPD[bin,1]
        print "write CPD =", C.CPD[bin,2]
    C.plot()
    if args.plot == None:
        plt.show()
    else:
        plt.savefig(args.data + "/" + args.plot)
    sys.exit()
# The above stuff needs to be sorted out in class Histograms
    H.above()
    if args.above == None:
        plt.show()
    else:
        plt.savefig(args.data + "/" + args.above)
