#!/usr/bin/env python
# threshy.py
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
import Threshhold
import Follows
import Histograms

parser = argparse.ArgumentParser(description='Access LMT data to do the follows calculation')
parser.add_argument('-y', '--year', default=None, type=str, help='Aggregate data for the months in the given year into the year\'s directory')
parser.add_argument('-n', '--num', default=10, type=int, help='How many ranges to break the follows calculation into')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
args = parser.parse_args()
first = True
Hist = Histograms.Histograms()
Thresh = Threshhold.Threshhold()
Fol = []
for n in range(args.num):
    low  = n*2500/args.num
    high = (n+1)*2500/args.num
    Fol.append(Follows.Follows(low = low, high = high))
for dir in os.listdir(args.year+r"/.."):
    if len(dir) > 7:
        continue
    if args.year == dir:
        continue
    if re.match(args.year, dir) == None:
        continue
    month_dir = args.year + "/../" + dir
#for dir in [ '2008-08', '2008-09', '2008-10', '2008-11', '2008-12', '2009-01', '2009-02', '2009-03' ]:
#for dir in [ '2009-04', '2009-05', '2009-06', '2009-07', '2009-08', '2009-09', '2009-10', '2009-11', '2009-12', '2010-01', '2010-02', '2010-03', '2010-04', '2010-05', '2010-06', '2010-07', '2010-08', '2010-09', '2010-10', '2010-11', '2010-12', '2011-01', '2011-02', '2011-03', '2011-04', '2011-05']:
#    month_dir = dir
    for n in range(args.num):
        low  = n*2500/args.num
        high = (n+1)*2500/args.num
        follows_file = month_dir + ("/follows_%04d.data" % n)
        if first == True:
            if os.access(follows_file, os.F_OK):
                Fol[n].load(follows_file)
            else:
                print "failed to get follows data from month", month_dir
                sys.exit(1)
        else:
            F = Follows.Follows(low = low, high = high)
            if os.access(follows_file, os.F_OK):
                F.load(follows_file)
            else:
                print "failed to get follows data from month", month_dir
                sys.exit(1)
            Fol[n].append(F)
    hist_file = month_dir + "/histogram.data"
    threshhold_file = month_dir + "/threshhold.data"
    if first == True:
        first = False
        if os.access(hist_file, os.F_OK):
            Hist.load(hist_file)
        else:
            print 'Missing histogram file in', month_dir
            sys.exit(1)
        if os.access(threshhold_file, os.F_OK):
            Thresh.load(threshhold_file)
        else:
            print "failed to get threshhold data from month", month_dir
            sys.exit(1)
    else:
        H = Histograms.Histograms()
        if os.access(hist_file, os.F_OK):
            H.load(hist_file)
        else:
            try:
                H.load(hist_file)
            except e:
                print e
                print 'Missing histogram file in', month_dir
                sys.exit(1)
        Hist.append(H)
        T = Threshhold.Threshhold()
        if os.access(threshhold_file, os.F_OK):
            T.load(threshhold_file)
        else:
            print "failed to get threshhold data from month", month_dir
            sys.exit(1)
        Thresh.append(T)
for n in range(args.num):
    Fol[n].save(args.year + ("/follows_%04d.data" % n))
Hist.save(args.year + "/histogram.data")
Thresh.save(args.year + "/threshhold.data")
