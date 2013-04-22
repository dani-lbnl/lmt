#!/usr/bin/env python
# Model.py <opts>
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
# -d <data> Path to directory of previously calculated follows data to load
#            rather than loading ost data
# -f <fs>   The file system (default <scratch>)
# -h        A help message
# -l        Plot using a log y-scale
# -p        Plot data to the fil at path <plot>
# -r        Plot read values
# -V        Print the version and exit
# -w        Plot write values
#
#   This module implements a particula model for the values x, and y and
# invokes a curve-fit sover for that model. It is only invoked in the commented
# out section of events_plot.py. The __main__ below recapitulates that activity.
#
# 2011-07-29
# - version 0.2
#   I've tried a couple of simple example and don't see this code working.
#   I'll come back to it if it's important.

import sys
import os
import string
import argparse
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy import linspace, polyval, polyfit, sqrt, randn
# These two appear to need liblapack.so, which they can't find on Euclid.
# There is a /usr/common/usg/lapack/3.2.1/liblapack.a
from scipy import stats, optimize
import Events

#*******************************************************************************
# Curve fit support functions

def residual(beta, X, Y):
    """
    \beta is the vector of curve fit coeficients in:
    \bar{Y} \sim X^{1/(\beta_0 + \beta_1*X - e^{-(\beta_2 + X)/\beta_3})}
    X is the values (left edges) of the bins for the histogram Y
    Y gives the observed counts for the bins X
    We are aming to minimize (Y - \bar{Y})^2
    """
    Est = np.power(X, 1/(beta[0] + beta[1]*X -
                              np.exp(-(beta[2] + X)/beta[3])))
    return(np.sum((Y - Est)**2))


#*******************************************************************************
# Begin class Model
class Model:
    """
    Given x and y values find the coefficients that correspond to
    various curve fits.
    """
    def __init__(self, bins=1250, min=0, max=1250, x=None, y=None, log=False, label=None):
        """
        """
        if (x == None) or (y == None):
            print "You must specify x and y when you initialize the Solve object"
            sys.exit(1)
        self.bins = bins
        self.min = min
        self.max = max
        self.x = x
        self.y = y
        self.log = log
        self.label = label
        self.slope = None
        self.intercept = None
        self.r1 = None
        self.p1 = None
        self.stderr1 = None
        self.scale = None
        self.offset = None
        self.r2 = None
        self.p2 = None
        self.stderr2 = None
        self.beta = np.zeros(4)

    def solve(self, min=None, max=None):
        """
        curve fit the data from x = min to x = max given the model
        log(x)/log(y) = a*x + b.
        """
        if self.min == None:
            m = 0
        else:
            m = self.min
        if min == None:
            min = m
        self.min = min
        if self.max == None:
            m = 0
        else:
            m = self.max
        if max == None:
            max = m
        self.max = max
        print "min =", min, "max =", max
        model = np.log(self.x)/np.log(self.y)
        indices = np.where(self.y > 1)
        l = len(indices[0])
        if l < max:
            max = l
        # The original data set
        X = self.x[indices[0]]
        Y = self.y[indices[0]]
        # The initial estimate
        y = model[indices[0]]
        (a_s,b_s,r,tt,stderr) = stats.linregress(X[min:max-1], y[min:max-1])
        if np.isnan(a_s):
            print "slope is NaN"
            sys.exit(1)
        if np.isnan(b_s):
            print "intercept is NaN"
            sys.exit(1)
        self.slope = a_s
        self.intercept = b_s
        print "slope =", self.slope, "intercept = ", self.intercept
        self.r1 = r
        self.p1 = tt
        self.stderr1 = stderr
        y = np.log(a_s*self.x[indices[0]] + b_s - model[indices[0]])
        (a_s,b_s,r,tt,stderr) = stats.linregress(X[1:10], y[1:10])
        if np.isnan(a_s):
            print "a_s is NaN"
            sys.exit(1)
        if np.isnan(b_s):
            print "b_s is NaN"
            sys.exit(1)
        self.scale = -1/a_s
        self.offset = b_s/a_s
        print "scale =", self.scale, "offset = ", self.offset
        self.r2 = r
        self.p2 = tt
        self.stderr2 = stderr
        XX = X[min:max-1]
        YY = Y[min:max-1]
        self.beta = \
            optimize.fmin( residual, [self.intercept, self.slope, self.offset, self.scale],
                           args=(XX,YY), maxiter=10000, maxfun=10000 )
        print self.beta

    def plot_model(self, plot=None, fig = None, xbound = None, ybound = None, title = None):
        """
        """
        print "min =", self.min, "max =", self.max
        if fig == None:
            fig = plt.figure()
        if (self.slope != None) and (self.intercept != None) and \
                (self.scale != None) and (self.offset != None):
            model = np.power(self.x, 1/(self.intercept + self.slope*self.x -
                                        np.exp(-(self.offset + self.x)/self.scale)))
#            model = self.intercept + self.slope*self.x - \
#                np.exp(-(self.offset + self.x)/self.scale)
            if self.log == False:
                plt.plot(self.x, model, 'k--', label=self.label + ' initial model')
            else:
                plt.semilogy(self.x, model, 'k--', label=self.label + ' initial model')
            ax, = fig.get_axes()
            if xbound != None:
                ax.set_xbound(upper = xbound)
            if ybound != None:
                ax.set_ybound(lower = -0.5, upper = ybound)
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

    def plot_opt(self, plot=None, fig = None, xbound = None, ybound = None, title = None):
        """
        """
        print "min =", self.min, "max =", self.max
        if fig == None:
            fig = plt.figure()
        if (self.beta[0] != 0) and (self.beta[1] != 0) and \
                (self.beta[2] != 0) and (self.beta[3] != 0):
            model = np.power(self.x, 1/(self.beta[0] + self.beta[1]*self.x -
                                        np.exp(-(self.beta[2] + self.x)/self.beta[3])))
#            model = self.intercept + self.slope*self.x - \
#                np.exp(-(self.offset + self.x)/self.scale)
            if self.log == False:
                plt.plot(self.x, model, 'k-', label=self.label + ' curve fit')
            else:
                plt.semilogy(self.x, model, 'k-', label=self.label + ' curve fit')
            ax, = fig.get_axes()
            if xbound != None:
                ax.set_xbound(upper = xbound)
            if ybound != None:
                ax.set_ybound(lower = -0.5, upper = ybound)
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

# End of class Model
#******************************************************************************

if __name__ == "__main__":
    """
Model.py <opts>
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
    parser.add_argument('-p', '--plot', default=None, type=str, help='Save a plot of the follows distribution in file plot')
    parser.add_argument('-r', '--read', action='store_true', default=False, help='Plot read distribution')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-w', '--write', action='store_true', default=False, help='Plot write distribution')
    args = parser.parse_args()
    E = Events.Events(log=args.log)
    hist = args.data + "/events_hist.data"
    events = args.data + "/events_list.data"
    lengths = args.data + "/lengths_hist.data"
    rates = args.data + "/rates_hist.data"
    zeros = args.data + "/zeros_hist.data"
    sdevs = args.data + "/sdevs_hist.data"
    for file in [hist, events, lengths, rates, zeros, sdevs]:
        if not os.access(file, os.F_OK):
            print 'Missing file ', file, 'in', args.data
            sys.exit(1)
    E.load(hist, events, lengths, rates, zeros, sdevs)
    R = Model(x=E.Histograms[:,0],y=E.Histograms[:,1], log=args.log, label='read')
    W = Model(x=E.Histograms[:,0],y=E.Histograms[:,2], log=args.log, label='write')
    R.solve(min=3, max=100)
    W.solve(min=3, max=100)
    fig = E.plot_hist(plot='wait', xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    if args.read == True:
        R.plot_model(plot='wait', fig=fig, xbound=args.xbound, ybound=args.ybound, title=args.title)
        R.plot_opt(plot='wait', fig=fig, xbound=args.xbound, ybound=args.ybound, title=args.title)
    if args.write == True:
        W.plot_model(plot='wait', fig=fig, xbound=args.xbound, ybound=args.ybound, title=args.title)
        W.plot_opt(plot='wait', fig=fig, xbound=args.xbound, ybound=args.ybound, title=args.title)
    if args.plot == True:
        plt.savefig(args.data + "/events_hist.png")
    else:
        plt.show()
