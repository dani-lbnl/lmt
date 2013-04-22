#!/usr/bin/env python
# Threshhold.py
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
#   Read in previously caluculated threshhold data and plot a graph.

import sys
import os
import re
import argparse
import string
import argparse
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import Follows

#*******************************************************************************
# Begin class Threshhold
class Threshhold:
    """
    Container class for 'threshhold' computation. The x-axis is the sequence
    of bins as from the histogram calculation. Generate a histogram of the
    diffs for a provided fs, for each of Reads and Writes, and with values scaled
    to MB. The Trheshhold object has five columns. Column 0 is the
    sequence of histogram bins. Column 1 is the number of observations at or
    above that bin for reads, and Column 3 is for writes. Column 2 is the
    number of observations above a given value that are also followed by
    an observation above that value.
    """

    def __init__(self, lag = 1, bins = 1250, min = 0.0, max = 2500.0, log = False):
        self.path = None
        self.lag = lag
        self.bins = bins
        self.min = min
        self.max = max
        self.log = log
        self.Done = False
        self.Threshhold = np.zeros((self.bins, 5), dtype=int)
        self.Threshhold[:,0] = int(self.min) + np.array(range(self.bins), dtype=int)*int((self.max - self.min)/self.bins)

    def threshhold(self, fs):
        hist, bins = np.histogram(fs.OSTReads.diffs/(1024*1024), bins=self.bins, range=(self.min, self.max))
        N = hist.sum()
        C = np.cumsum(hist)
        self.Threshhold[0,1] = N
        self.Threshhold[1:,1] = N - C[:-1]
        hist, bins = np.histogram(fs.OSTWrites.diffs/(1024*1024), bins=self.bins, range=(self.min, self.max))
        N = hist.sum()
        C = np.cumsum(hist)
        self.Threshhold[0,3] = N
        self.Threshhold[1:,3] = N - C[:-1]
        for ost in range(fs.OSTReads.num_osts):
            # For each bin how many followers are above the bin
            for bin in range(self.bins):
                # x corresponds to the actual size of the observations in the bin
                x = int(self.min + bin*(self.max - self.min)/self.bins)
                # Reads
                # higher is a boolean array the same shape as the diffs for one OST
                # and selects elements with a value above that for this bin
                higher = fs.OSTReads.diffs[ost,:]/(1024*1024) >= x
                # selection the set of indices for diffs higher than the threshhold
                # that are also followed by a diff higher than the threshhold
                selection = np.where(higher[:-1]*higher[1:])
                self.Threshhold[bin,2] = self.Threshhold[bin,2] + len(selection[0])
                # Writes
                # higher is a boolean array the same shape as the diffs for one OST
                higher = fs.OSTWrites.diffs[ost,:]/(1024*1024) >= x
                # followers counts the set of indices for diffs higher than the threshhold
                # that are also followed by a diff higher than the threshhold
                selection = np.where(higher[:-1]*higher[1:])
                self.Threshhold[bin,4] = self.Threshhold[bin,4] + len(selection[0])
        self.Done = True

    def append(self, append):
        self.Threshhold[:,1] = self.Threshhold[:,1] + append.Threshhold[:,1]
        self.Threshhold[:,2] = self.Threshhold[:,2] + append.Threshhold[:,2]
        self.Threshhold[:,3] = self.Threshhold[:,3] + append.Threshhold[:,3]
        self.Threshhold[:,4] = self.Threshhold[:,4] + append.Threshhold[:,4]

    def load(self, path = None):
        if path == None:
            if self.path == None:
                self.path = "threshhold.data"
        else:
            self.path = path
        self.Threshhold = np.loadtxt(self.path)
        self.Done = True

    def save(self, path = None):
        if path == None:
            if self.path == None:
                self.path = "threshhold.data"
        else:
            self.path = path
        np.savetxt(self.path, self.Threshhold, fmt='%d\t%d\t%d\t%d\t%d')

    def plot(self, plot = None, fig = None, xlimit = 2000, title = None):
        xlimit = (xlimit - self.min)*self.bins/(self.max - self.min)
        if fig == None:
            fig = plt.figure()
        if self.log == False:
            plt.plot(self.Threshhold[:xlimit,0], self.Threshhold[:xlimit,2]/self.Threshhold[:xlimit,1], 'r-', label='read')
            plt.plot(self.Threshhold[:xlimit,0], self.Threshhold[:xlimit,4]/self.Threshhold[:xlimit,3], 'b-', label='write')
            ax, = fig.get_axes()
            ax.set_ybound(lower = 0.0, upper = 1.0)
        else:
            plt.semilogy(self.Threshhold[:xlimit,0], self.Threshhold[:xlimit,2]/self.Threshhold[:xlimit,1], 'r-', label='read')
            plt.semilogy(self.Threshhold[:xlimit,0], self.Threshhold[:xlimit,4]/self.Threshhold[:xlimit,3], 'b-', label='write')
        ax, = fig.get_axes()
        ax.set_xbound(upper = self.max)
        if title == None:
            plt.title(r'threshhold calculation')
        else:
            plt.title(title)
        plt.legend()
        plt.ylabel(r'$F(t)/N\uparrow(t)$')
        plt.xlabel('threshhold (MB)')
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

# End of class Threshhold
#*******************************************************************************

if __name__ == "__main__":
    """
Threshhold.py <opts>
Options include:
-d <data>   Path to directory of previously calculated data to load
             rather than loading ost data
-f <fs>     The file system (default <scratch>)
-h          A help message
-n          Break the Follows calculation into this many subunits
-p          Plot data to the fil at path <plot>
-t <title>  Put 'title' in the title of the graph
-V          Print the version and exit
-x <xbound> x-axis bounds
-y <ybound> y-axis bounds

Read in previously caluculated threshhold data and plot a graph.
    """
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access LMT data to do the threshhold calculation')
    parser.add_argument('-d', '--dir', default=None, type=str, help='Aggregate data for the days in the given dir into the dir\'s directory')
    parser.add_argument('-f', '--follows', action='store_true', default=False, help='plot the follows graphs as well')
    parser.add_argument('-n', '--num', default=10, type=int, help='How many ranges to break the follows calculation into')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the histogram plot in')
    parser.add_argument('-t', '--title', default=None, type=str, help='Title to put at the top of the graph')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
    parser.add_argument('-x', '--xbound', default=None, type=float, help='Only plot for values of x up to this limit')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Constrain the ybound of the Follows graph')
    args = parser.parse_args()
    threshhold_file = args.dir + "/threshhold.data"
    T = Threshhold()
    T.load(threshhold_file)
    if args.xbound == None:
        T.plot(plot = args.plot, title = args.title)
    else:
        T.plot(plot = args.plot, xlimit = args.xbound, title = args.title)
    if args.follows == False:
        sys.exit(0)
    for n in range(args.num):
        follows_file = args.dir + ("/follows_%04d.data" % n)
        low  = n*2500/args.num
        high = (n+1)*2500/args.num
        F = Follows.Follows(low=low, high=high)
        F.load(follows_file)
        F.plot(args.dir + ("/follows_%04d.png" % n), ybound=args.ybound )

