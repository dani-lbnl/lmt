#!/usr/bin/env python
# thresh_plot.py
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
import argparse
import os
import re
import Threshhold
import Follows

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
T = Threshhold.Threshhold()
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
#    F.plot()

