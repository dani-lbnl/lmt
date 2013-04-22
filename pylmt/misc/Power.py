#!/usr/bin/env python
# Power.py <opts>
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
# Options include:
# -d <data>    Path to directory of previously calculated follows data to load
#               rather than loading ost data
# -f <fs>      The file system (default <scratch>)
# -h           A help message
# -l           Plot using a log y-scale
# -p           Plot data to the fil at path <plot>
# -r        Plot read values
# -V        Print the version and exit
# -w        Plot write values
#
#   This module calculates the power distribution of a given data set.
#
# 2011-07-29
# - version 0.2

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
# Begin class Power
class Power:
    """
    Container class for 'power' computation. This is the power
    distribution. For each bin, what fraction of the I/O is represented by
    observations in that bin.
    """

    def __init__(self, bins = 1250, min = 0.0, max = 2500.0, log = False):
        self.path = None
        self.bins = bins
        self.min = min
        self.max = max
        self.log = log
        self.Done = False
        self.Power = np.zeros((self.bins, 3), dtype=float)
        self.Power[:,0] = self.min + np.array(range(self.bins), dtype=float)*(self.max - self.min)/self.bins

    def power(self, hist):
        self.Power[:,1] = hist.Histograms[:,1]*hist.Histograms[:,0]
        self.Power[:,1] = self.Power[:,1]/self.Power[:,1].sum()
        self.Power[:,2] = hist.Histograms[:,2]*hist.Histograms[:,0]
        self.Power[:,2] = self.Power[:,2]/self.Power[:,2].sum()
        self.Done = True

    def append(self, append):
        self.Power[:,1] = self.Power[:,1] + append.Power[:,1]
        self.Power[:,2] = self.Power[:,2] + append.Power[:,2]

    def load(self, path = None):
        if path == None:
            if self.path == None:
                self.path = "power.data"
        else:
            self.path = path
        self.Power = np.loadtxt(self.path)
        self.Done = True

    def save(self, path = None):
        if path == None:
            if self.path == None:
                self.path = "power.data"
        else:
            self.path = path
        np.savetxt(self.path, self.Power, fmt='%f\t%f\t%f')

    def plot(self, plot = None, fig = None):
        if fig == None:
            fig = plt.figure()
        if self.log == False:
            plt.plot(self.Power[:,0], self.Power[:,1], 'r-', label='read')
            plt.plot(self.Power[:,0], self.Power[:,2], 'b-', label='write')
            ax, = fig.get_axes()
            max = self.Power[:,1:3].max()
            ax.set_ybound(lower = 0.0, upper = max)
        else:
            plt.semilogy(self.Power[:,0], self.Power[:,1], 'r-', label='read')
            plt.semilogy(self.Power[:,0], self.Power[:,2], 'b-', label='write')
        plt.legend()
        return(fig)

# End of class Power
#*******************************************************************************

if __name__ == "__main__":
    """
Power.py <opts>
Options include:
-d <data> Path to directory of previously calculated data to load
          rather than loading ost data
-f <fs>   The file system (default <scratch>)
-h        A help message
-l        Plot using a log y-scale
-p        Plot data to the fil at path <plot>
-r        Plot read values
-V        Print the version and exit
-w        Plot write values

   This module implements a particula model for the values x, and y and
 invokes a curve-fit sover for that model. It is only invoked in the commented
 out section of events_plot.py. The __main__ below recapitulates that activity.
    """
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-d', '--data', default=".", type=str, help='Directory with previously extracted OST data (default ".")')
    parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
    parser.add_argument('-l', '--log', action='store_true', default=False, help='plot with a log-scaled y-axis')
    parser.add_argument('-p', '--plot', default=None, type=str, help='Save a plot of the power distribution in file plot')
    parser.add_argument('-r', '--read', action='store_true', default=False, help='Plot read distribution')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-w', '--write', action='store_true', default=False, help='Plot write distribution')
    args = parser.parse_args()
    hist = args.data + "/histogram.data"
    if not os.access(hist, os.F_OK):
        print 'Missing file ', hist, 'in', args.data
        sys.exit(1)
    H = Histograms.Histograms()
    H.load(hist)
    P = Power()
    P.power(H)
    fig = P.plot()
    if args.plot == None:
        plt.show()
    else:
        plt.savefig(args.data + "/" + args.plot)
