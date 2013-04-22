#!/usr/bin/env python
# thresh_day.py
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
#   Use the data.py module to read in OST data and produce
# aggregated histograms of data over long periods.
#
#   Go through the target directories, and if they don't already have
# summary histogram data produce it. If they do then just read that in.
# Accumulate the histogram data and at the end save it in the summary
# directory.
#
# In this case do the threshhold calculation and aggregate the results.

import sys
import argparse
import os
import re
import FS
import Threshhold
import Follows
import Histograms

parser = argparse.ArgumentParser(description='Access LMT data to do the follows calculation')
parser.add_argument('-d', '--dir', default=None, type=str, help='Directory with previously extracted LMT data')
parser.add_argument('-f', '--fs', default='scratch', type=str, help='The target file system')
parser.add_argument('-k', '--keep', action='store_true', default=False, help='Do not recalculate if files are already in place')
parser.add_argument('-n', '--num', default=10, type=int, help='How many ranges to break the follows calculation into')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
args = parser.parse_args()
keep = False
hist_file = args.dir + "/histogram.data"
threshhold_file = args.dir + "/threshhold.data"
if args.keep == True:
    keep = True
    if not os.access(hist_file, os.F_OK):
        keep = False
    if not os.access(threshhold_file, os.F_OK):
        keep = False
    for n in range(args.num):
        follows_file = args.dir + ("/follows_%04d.data" % n)
        if not os.access(follows_file, os.F_OK):
            keep = False
if keep == True:
    sys.exit(0)
fs = FS.FS(fs=args.fs)
nosts = None
nosts = fs.osts_from_dir(dir = args.dir)
if (nosts == None) or (nosts == 0):
    print "Error: Got no OSTs"
    sys.exit(1)
fs.get_ost_data_from_dir()
fs.aggregate()
fs.differentials()
H = Histograms.Histograms()
H.histograms(fs)
H.save(hist_file)
T = Threshhold.Threshhold()
T.threshhold(fs)
T.save(threshhold_file)
for n in range(args.num):
    low  = n*2500/args.num
    high = (n+1)*2500/args.num
    F = Follows.Follows(low = low, high = high)
    follows_file = args.dir + ("/follows_%04d.data" % n)
    F.follows(fs)
    F.save(follows_file)
