#!/usr/bin/env python
# hist_plot.py
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
#   Read in previously caluculated histogram data and plot a graph.

import sys
import argparse
import os
import re
import Histograms

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
H = Histograms.Histograms(log = args.log)
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
