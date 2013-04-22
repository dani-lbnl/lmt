#!/usr/bin/env python
# Follows.py
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
#   Read in previously caluculated follows data and plot a graph.

import sys
import argparse
import os
import re
import string
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy import linspace, polyval, polyfit, sqrt, randn

#*******************************************************************************
# Begin class Follows
class Follows:
    """
    Container class for 'follows' computation.
    """

    def __init__(self, low = 1500, high = 2500, lag = 1, bins = 1250, min = 0.0, max = 2500.0, normed = False, log = False):
        self.path = None
        self.low = low
        self.high = high
        self.lag = lag
        self.bins = bins
        self.min = min
        self.max = max
        self.normed = normed
        self.log = log
        self.Done = False
        self.Follows = np.zeros((self.bins, 3), dtype=int)
        self.Follows[:,0] = int(self.min) + np.array(range(self.bins), dtype=int)*int((self.max - self.min)/self.bins)

    def follows(self, fs):
        for ost in range(fs.OSTReads.num_osts):
            # Reads
            # Find the array elements that meet the criteria
            # followers = np.where((self.diffs[ost,:-1] > low) * (self.diffs[ost,:-1] < high))
            higher = fs.OSTReads.diffs[ost,:-1]/(1024*1024) >= self.low
            lower  = fs.OSTReads.diffs[ost,:-1]/(1024*1024) < self.high
            followers = np.where(higher * lower)
            # Point at the array element that follow them
            followers = followers + self.lag*np.ones_like(followers)
            # Select that set of elements
            follow = fs.OSTReads.diffs[ost, followers]/(1024*1024)
            Hist, Bins = np.histogram(follow, bins=self.bins, range=(self.min, self.max), normed=self.normed)
            self.Follows[:,1] = self.Follows[:,1] + Hist
            # Writes
            higher = fs.OSTWrites.diffs[ost,:-1]/(1024*1024) >= self.low
            lower  = fs.OSTWrites.diffs[ost,:-1]/(1024*1024) < self.high
            followers = np.where(higher * lower)
            # Point at the array element that follow them
            followers = followers + self.lag*np.ones_like(followers)
            # Select that set of elements
            follow = fs.OSTWrites.diffs[ost, followers]/(1024*1024)
            Hist, Bins = np.histogram(follow, bins=self.bins, range=(self.min, self.max), normed=self.normed)
            self.Follows[:,2] = self.Follows[:,2] + Hist
            self.Done = True

    def append(self, append):
        self.Follows[:,1] = self.Follows[:,1] + append.Follows[:,1]
        self.Follows[:,2] = self.Follows[:,2] + append.Follows[:,2]

    def load(self, path = None):
        if path == None:
            if self.path == None:
                self.path = "follows.data"
        else:
            self.path = path
        self.Follows = np.loadtxt(self.path)
        self.Done = True

    def save(self, path = None):
        if path == None:
            if self.path == None:
                self.path = "follows.data"
        else:
            self.path = path
        np.savetxt(self.path, self.Follows, fmt='%d\t%d\t%d')

    def plot(self, plot = None, ybound = None, fig = None, title = None):
        if fig == None:
            fig = plt.figure()
        if self.log == False:
            plt.plot(self.Follows[:,0], self.Follows[:,1], 'r-', label='read')
            plt.plot(self.Follows[:,0], self.Follows[:,2], 'b-', label='write')
        else:
            plt.semilogy(self.Follows[:,0], self.Follows[:,1], 'r-', label='read')
            plt.semilogy(self.Follows[:,0], self.Follows[:,2], 'b-', label='write')
        ax, = fig.get_axes()
        if title == None:
            plt.title(r'Distribution of "follows" observations')
        else:
            plt.title(title)
        if ybound != None:
            ax.set_ybound(upper = ybound)
        plt.axvspan(self.low, self.high, facecolor='g', alpha=0.5)
        plt.legend()
        plt.ylabel(r'$p_f(x)$')
        plt.xlabel('observation size (MB)')
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

# End of class Follows
#*******************************************************************************

if __name__ == "__main__":
    """
Follows.py <opts>
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
    parser = argparse.ArgumentParser(description='Access LMT data to do the follows calculation')
    parser.add_argument('-d', '--data', default=None, type=str, help='Plot Follows calculation from the data directory')
    parser.add_argument('-n', '--num', default=10, type=int, help='How many ranges to break the follows calculation into')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()
    for n in range(args.num):
        follows_file = args.data + ("/follows_%04d.data" % n)
        low  = n*2500/args.num
        high = (n+1)*2500/args.num
        F = Follows(low=low, high=high)
        F.load(follows_file)
        F.plot(args.data + ("/follows_%04d.png" % n) )

