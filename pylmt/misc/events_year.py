#!/usr/bin/env python
# events_year.py
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
# aggregated events (transactions) histograms of data over long periods.
#

import sys
import argparse
import os
import re
import Events

parser = argparse.ArgumentParser(description='Access an LMT DB')
parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
parser.add_argument('-k', '--keep', action='store_true', default=False, help='If all the files already exist do nothing')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
parser.add_argument('-y', '--year', default=None, type=str, help='Aggregate data for the months in the given year into the year\'s directory')
args = parser.parse_args()
if args.year == None:
    print "You need to provide a -y <year>"
    sys.exit(1)
if args.keep:
    hist = args.year + "/events_hist.data"
    events = args.year + "/events_list.data"
    lengths = args.year + "/lengths_hist.data"
    rates = args.year + "/rates_hist.data"
    zeros = args.year + "/zeros_hist.data"
    sdevs = args.year + "/sdevs_hist.data"
    have_all = True
    for file in [hist, events, lengths, rates, zeros, sdevs]:
        if not os.access(file, os.F_OK):
            have_all = False
    if have_all == True:
        sys.exit(0)
first = True
#for dir in os.listdir(args.year+r"/.."):
#    if len(dir) > 7:
#        continue
#    if args.year == dir:
#        continue
#    if re.match(args.year, dir) == None:
#        continue
#    month_dir = args.year + "/../" + dir
for month_dir in [ '2008-08', '2008-09', '2008-10', '2008-11', '2008-12', '2009-01', '2009-02', '2009-03' ]:
    hist = month_dir + "/events_hist.data"
    events = month_dir + "/events_list.data"
    lengths = month_dir + "/lengths_hist.data"
    rates = month_dir + "/rates_hist.data"
    zeros = month_dir + "/zeros_hist.data"
    sdevs = month_dir + "/sdevs_hist.data"
    files = True
    for file in [hist, events, lengths, rates, zeros, sdevs]:
        if not os.access(file, os.F_OK):
            print 'Missing file ', file, 'in', day_dir
            files = False
    if files == False:
        sys.exit(1)
    if first == True:
        first = False
        Ev = Events.Events()
        Ev.load(hist, events, lengths, rates, zeros, sdevs)
    else:
        E = Events.Events()
        E.load(hist, events, lengths, rates, zeros, sdevs)
        Ev.append(E)
hist = args.year + "/events_hist.data"
events = args.year + "/events_list.data"
lengths = args.year + "/lengths_hist.data"
rates = args.year + "/rates_hist.data"
zeros = args.year + "/zeros_hist.data"
sdevs = args.year + "/sdevs_hist.data"
Ev.save(hist, events, lengths, rates, zeros, sdevs)
