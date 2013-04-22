#!/usr/bin/env python
# hist.py
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

import sys
import argparse
import os
import re
import FS
import Histograms

parser = argparse.ArgumentParser(description='Access an LMT DB')
parser.add_argument('-d', '--dir', default=None, type=str, help='Just do the histogram for this one directory')
parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
parser.add_argument('-m', '--month', default=None, type=str, help='Aggregate data for the days in the given month into the month\'s directory')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
args = parser.parse_args()
if args.dir != None:
    fs = FS.FS(fs=args.fs)
    fs.osts_from_dir(args.dir)
    hist_file = args.dir + "/histogram.data"
    H = Histograms.Histograms()
    fs.get_ost_data_from_dir()
    fs.aggregate()
    fs.differentials()
    H.histograms(fs)
    H.save(hist_file)
    sys.exit(0)
first = True
Hist = Histograms.Histograms()
for dir in os.listdir(args.month+r"/.."):
    fs = None
    if args.month == dir:
        continue
    if re.match(args.month, dir) == None:
        continue
    day_dir = args.month + "/../" + dir
    fs = FS.FS(fs=args.fs)
    fs.osts_from_dir(day_dir)
    hist_file = day_dir + "/histogram.data"
    if first == True:
        first = False
        if os.access(hist_file, os.F_OK):
            Hist.load(hist_file)
        else:
            fs.get_ost_data_from_dir()
            fs.aggregate()
            fs.differentials()
            Hist.histograms(fs)
            Hist.save(hist_file)
    else:
        H = Histograms.Histograms()
        if os.access(hist_file, os.F_OK):
            H.load(hist_file)
        else:
            fs.get_ost_data_from_dir()
            fs.aggregate()
            fs.differentials()
            H.histograms(fs)
            H.save(hist_file)
        Hist.append(H)
Hist.save(args.month + "/histogram.data")
