#!/usr/bin/env python
# Histograms.py
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
#   Read in previously caluculated histogram data and plot a graph.

import sys
import os
import re
import string
import argparse
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

#*******************************************************************************
# Begin class Histograms
class Histograms:
    """
    Container class for probability dentsity histogram computation.
    (Those provided by histograms():)
    Histograms - (bins, read, write) 3 x n array for the histograms
    bins       - number of bins in the Histograms
    min        - minimum value of bin content
    max        - maximum value
    normed     - whether to divide by the total number of observations
    log        - whether to plot on a log y-scale
    Done       - Set to true when the histogram has been populated
    """

    def __init__(self, bins = 1250, min = 0.0, max = 2500.0, normed=False, log = False):
        self.path = None
        self.bins = bins
        self.min = min
        self.max = max
        self.normed = normed
        self.log = log
        self.Histograms = np.zeros((self.bins, 3), dtype=int)
        self.Histograms[:,0] = int(self.min) + np.array(range(self.bins), dtype=int)*int((self.max - self.min)/self.bins)
        self.Done = False

    def histograms(self, fs):
        """
        Use the NumPy histogram function to get a histogram for the
        differential data sets.

        If normed is True you get an approximation for the probability
        distribution. But keeping them normed=False means you can combine
        histograms from two different samples.
        """
        if fs.DiffDone == False:
            print "Histograms.histograms(): You need to caclulate the differentials first"
            sys.exit(1)
        if (fs.OSTReads.DiffDone == False) or (fs.OSTWrites.DiffDone == False):
            print "Histograms.histograms(): No differentials for which to make histogram"
            return
        self.Histograms[:,1], Bins = np.histogram(fs.OSTReads.diffs/(1024*1024), bins=self.bins, range=(self.min, self.max), normed=self.normed)
        self.Histograms[:,2], Bins = np.histogram(fs.OSTWrites.diffs/(1024*1024), bins=self.bins, range=(self.min, self.max), normed=self.normed)
        self.Done = True

    def append(self, append):
        self.Histograms[:,1] = self.Histograms[:,1] + append.Histograms[:,1]
        self.Histograms[:,2] = self.Histograms[:,2] + append.Histograms[:,2]

    def show_text(self):
        if self.Done == False:
            print "Histograms.show_text(): The OST histograms have not been calculated"
            return
        if self.normed == True:
            format = "%16.0f\t%16.14e\t%16.14e"
        else:
            format = "%16.0f\t%16.0f\t%16.0f"
        for bin in range(self.bins):
            print format % (self.Histogram[bin, 0], self.Histogram[bin, 1], self.Histogram[bin, 2])

    def save(self, path=None):
        if self.Done == False:
            print "Histograms.save_hist_data(): The OST histograms have not been calculated"
            return
        if path == None:
            if self.path == None:
                self.path = "histogram.data"
        else:
            self.path = path
        np.savetxt(self.path, self.Histograms, fmt='%d\t%d\t%d')

    def load(self, path=None):
        if path == None:
            if self.path == None:
                self.path = "histogram.data"
        else:
            self.path = path
        self.Histograms = np.loadtxt(self.path)
        self.Done = True

    def plot(self, plot=None, fig = None, ybound = None, title = None):
        if fig == None:
            fig = plt.figure()
        if self.Done == False:
            print "Hitorams.plot(): The OST histograms have not been calculated"
            return
        if self.log == False:
            plt.plot(self.Histograms[:,0], self.Histograms[:,1], 'r-', label='read')
            plt.plot(self.Histograms[:,0], self.Histograms[:,2], 'b-', label='write')
        else:
            plt.semilogy(self.Histograms[:,0], self.Histograms[:,1], 'r-', label='read')
            plt.semilogy(self.Histograms[:,0], self.Histograms[:,2], 'b-', label='write')
        ax, = fig.get_axes()
        if ybound != None:
            ax.set_ybound(upper = ybound)
        if title != None:
            plt.title(title)
        plt.ylabel("Count")
        plt.xlabel("MB")
        plt.legend()
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

    def above(self, plot=None, fig = None, ybound=None, title = None):
        if fig == None:
            fig = plt.figure()
        if self.Done == False:
            print "Hitorams.above(): The OST histograms have not been calculated"
            return
        if self.log == False:
            total = self.Histograms[:,1].sum()
            cum = total - np.cumsum(self.Histograms[:,1])
            plt.plot(self.Histograms[:,0], cum, 'r-', label='read')
            total = self.Histograms[:,2].sum()
            cum = total - np.cumsum(self.Histograms[:,2])
            plt.plot(self.Histograms[:,0], cum, 'b-', label='write')
        else:
            plt.semilogy(self.Histograms[:,0], self.Histograms[:,1], 'r-', label='read')
            plt.semilogy(self.Histograms[:,0], self.Histograms[:,2], 'b-', label='write')
        ax, = fig.get_axes()
        if ybound != None:
            ax.set_ybound(upper = ybound)
        if title != None:
            plt.title(title)
        plt.legend()
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

# End of class Histograms
#*******************************************************************************

if __name__ == "__main__":
    """
Histograms.py <opts>
Options include:
-a <above> Plot the 'above' calculation at the file <above>
-d <data>   Path to directory of previously calculated data to load
             rather than loading ost data
-f <fs>     The file system (default <scratch>)
-h          A help message
-l         Plot using a log y-scale
-n          Break the Follows calculation into this many subunits
-p          Plot data to the file at path <plot>
-t <title>  Put 'title' in the title of the graph
-V          Print the version and exit
-y <ybound> y-axis bounds

Read in previously caluculated threshhold data and plot a graph.
    """
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Plot a histogram using the data provided')
    parser.add_argument('-a', '--above', default=None, type=str, help='The name of the file to save the "above" calculation plot in')
    parser.add_argument('-d', '--dir', default=".", type=str, help='Directory with previously computed historgram data')
    parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
    parser.add_argument('-l', '--log', action='store_true', default=False, help='plot with a log-scaled y-axis')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the histogram plot in')
    parser.add_argument('-t', '--title', default=None, type=str, help='Title to put at the top of the graph')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    histogram_file = args.dir + "/histogram.data"
    if args.title == None:
        args.title = args.dir
    H = Histograms(log = args.log)
    H.load(histogram_file)
# This doesn't allow for a way to just show the "above" plot
    if args.above == None and args.plot == None:
        H.plot(ybound = args.ybound, title = args.title)
    elif args.above == None:
        H.plot(args.plot, ybound = args.ybound, title = args.title)
    elif args.plot == None:
        H.above(args.above, ybound = args.ybound, title = args.title)
    else:
        H.plot(args.plot, ybound = args.ybound, title = args.title)
        H.above(args.above, ybound = args.ybound, title = args.title)
