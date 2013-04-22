#!/usr/bin/env python
# histy.py
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

import sys
import argparse
import os
import re
import Histograms

parser = argparse.ArgumentParser(description='Access an LMT DB')
parser.add_argument('-y', '--year', default=None, type=str, help='Aggregate data for the days in the given year into the year\'s directory')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
args = parser.parse_args()
first = True
Hist = Histograms.Histograms()
#for dir in os.listdir(args.year+r"/.."):
#    if len(dir) > 7:
#        continue
#    if args.year == dir:
#        continue
#    if re.match(args.year, dir) == None:
#        continue
#    month_dir = args.year + "/../" + dir
for dir in [ '2009', '2010', '2011' ]:
    month_dir = dir
    hist_file = month_dir + "/histogram.data"
    if first == True:
        first = False
        if os.access(hist_file, os.F_OK):
            Hist.load(hist_file)
        else:
            print "failed to get histogram data from month", month_dir
            sys.exit(1)
    else:
        H = Histograms.Histograms()
        if os.access(hist_file, os.F_OK):
            H.load(hist_file)
        else:
            print "failed to get histogram data from month", month_dir
            sys.exit(1)
        Hist.append(H)
Hist.save(args.year + '/histogram.data')
