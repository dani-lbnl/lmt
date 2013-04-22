#!/usr/bin/env python
# sigma.py
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
#If the distribution of the "follows" observations were identical to
#the distribution of the whole range of observations then it would be
#N*p(x) where N is the total number of observations above the
#threshhold and p(x) is the normalized distribution of
#observations. The chance that a "follows" observation is also above
#the threshhold is then F(x) = N*\integral_x^1 p(x') dx'. You would
#expect F(x) observations at or above bin x. You would expect an
#ensemble of observations to have that for its mean, and the standard
#deviation would be sqrt(F(x)). If the actual number of observations is
#K(x) then the chance of seeing that value is governed by the sigma
#value:
#
#abs(K(x) - F(x))/sqrt(F(x))
#

import sys
import argparse
import os
import re
import numpy as np
import numpy.ma as ma
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import Axes3D
import Threshhold
import Histograms

parser = argparse.ArgumentParser(description='Calculate the likelyhood function for observations of the threshhold data')
parser.add_argument('-d', '--dir', default=None, type=str, help='Directory with previously extracted and summarized data')
parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the histogram plot in')
parser.add_argument('-s', '--sigma', default=None, type=float, help='Find the bin value for where sigma exceed this value')
parser.add_argument('-t', '--title', default=None, type=str, help='Title to put at the top of the graph')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
parser.add_argument('-x', '--xbound', default=None, type=float, help='Set the x-axis upper bound to this value')
parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
args = parser.parse_args()
hist_file = args.dir + '/histogram.data'
if os.access(hist_file, os.F_OK) == False:
    print 'I do not see a histogram.data file in ', args.dist
    sys.exit(1)
# Hist gives an unnormalized histogram of the observations in
# question Hist(x) = N*p(x)
Hist = Histograms.Histograms()
Hist.load(hist_file)
thresh_file = args.dir + '/threshhold.data'
if os.access(thresh_file, os.F_OK) == False:
    print 'I do not see a threshhold.data file in ', args.dist
    sys.exit(1)
# Thresh gives the number of observations in the histogram above
# a threshhold value Thresh_1(x) = \Sum_x^{\infty} Hist(x) and
# the number of following observations also above the threshhold
# Thresh_2(x) = Thresh_1(x)*p_f(x)
# In the paper Thresh_1 is called N\uparrow(x)
Thresh = Threshhold.Threshhold()
Thresh.load(thresh_file)

# Follows(x) gives the expected number of following observations
# if they were uncorrelated and is called N\uparraw\uparrow(x)
# in the paper
Follows = np.zeros((Hist.bins, 3), dtype=float)
Follows[:,0] = Hist.Histograms[:,0]

# aggregate is just the total number of observations N
aggregate = Hist.Histograms[:,1].sum()
Follows[:,1] = Thresh.Threshhold[:,1]*Hist.Histograms[:,1]/aggregate
Follows[:,1] = Follows[:,1].sum() - np.cumsum(Follows[:,1])

aggregate = Hist.Histograms[:,2].sum()
Follows[:,2] = Thresh.Threshhold[:,3]*Hist.Histograms[:,2]/aggregate
Follows[:,2] = Follows[:,2].sum() - np.cumsum(Follows[:,2])

# Sigma(x) is the relative likelyhood that the actual values
# are just random fluctiations of the expected values.
#
Sigma = np.zeros((Hist.bins, 3), dtype=float)
Sigma[:,0] = Hist.Histograms[:,0]

Sigma[:,1] = Thresh.Threshhold[:,2] - Follows[:,1]
Sigma[:,1] = np.abs(Sigma[:,1])
Sigma[:,1] = np.sqrt(Sigma[:,1]/Follows[:,1])

Sigma[:,2] = Thresh.Threshhold[:,4] - Follows[:,2]
Sigma[:,2] = np.abs(Sigma[:,2])
Sigma[:,2] = np.sqrt(Sigma[:,2]/Follows[:,2])

if args.xbound == None:
    xbound = 990
else:
    xbound = args.xbound
fig = plt.figure()
plt.plot(Sigma[:xbound,0], Sigma[:xbound,1], 'r-', label='read')
plt.plot(Sigma[:xbound,0], Sigma[:xbound,2], 'b-', label='write')
ax, = fig.get_axes()
if args.ybound != None:
    ax.set_ybound(upper = args.ybound)
if args.title == None:
    plt.title(r'$\Sigma$ calculation')
else:
    plt.title(args.title)
ax.set_xbound(upper = 2.5 * xbound)
plt.legend()
plt.ylabel(r'$\Sigma(t)$')
plt.xlabel('threshhold (MB)')
if args.plot == None:
    plt.show()
else:
    plt.savefig(args.plot)

# Find the least index where all values of sigma above that
# bin are greater than args.sigma
# One caveat, based on a close examination of the /scratch2
# read data is that there is a minimum in the curve, and you
# want to be at or above that. Below that minimum the values
# are correlated simply because you are counting everything.
if args.sigma != None:
    lower = Sigma[:xbound,1] < args.sigma
    lower_list, = np.where(lower)
    if len(lower_list) == 0:
        bin = 0
    else:
        bin = np.max(np.where(lower)) + 1
    print "read threshhold =", Hist.min + bin*(Hist.max - Hist.min)/Hist.bins
    lower = Sigma[:xbound,2] < args.sigma
    lower_list, = np.where(lower)
    if len(lower_list) == 0:
        bin = 0
    else:
        bin = np.max(np.where(lower)) + 1
    print "write threshhold =", Hist.min + bin*(Hist.max - Hist.min)/Hist.bins
