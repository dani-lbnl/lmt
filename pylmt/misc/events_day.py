#!/usr/bin/env python
# events_day.py
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
#   I should use a hybrid approach to saving this data. Keep the
# histogram for the section where it has non-zero values, and keep the
# events where they become sparse.
#
# The data I want to extract is rich and complex, and storing it in
# a hybrid fashion requires several files.
#
# events_hist.data - A histogram of <bins> from <min> to <max> for event sizes
# events_list.data - A list of <num> events larger than <max>
# length_hist.data - A histogram of <bins> of event lengths from 0 to <longest>
# zeros_hist.data - A histogram of <bins> of contiguous zeros from 0 to <zeros>

import sys
import argparse
import os
import re
import FS
import Events

parser = argparse.ArgumentParser(description='Access an LMT DB')
parser.add_argument('-d', '--dir', default='.', type=str, help='Do the events calculations for this directory')
parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
parser.add_argument('-k', '--keep', action='store_true', default=False, help='Do not recalculate if files are already in place')
parser.add_argument('-t', '--threshhold', default=None, type=float, help='Factor by which to adjust the threshhold (from the long-term average)')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
args = parser.parse_args()
hist = args.dir + '/events_hist.data'
events = args.dir + '/events_list.data'
lengths = args.dir + '/lengths_hist.data'
rates = args.dir + '/rates_hist.data'
zeros = args.dir + '/zeros_hist.data'
sdevs = args.dir + '/sdevs_hist.data'
if args.keep:
    have_all = True
    for file in [hist, events, lengths, rates, zeros, sdevs]:
        if not os.access(file, os.F_OK):
            have_all = False
    if have_all == True:
        sys.exit(0)
fs = FS.FS(fs=args.fs)
fs.osts_from_dir(args.dir)
fs.get_ost_data_from_dir()
fs.aggregate()
fs.differentials()
threshhold = (fs.OSTReads.average*fs.OSTReads.count_all + fs.OSTWrites.average*fs.OSTWrites.count_all)/((fs.OSTReads.count_all + fs.OSTWrites.count_all)*(1024*1024))
if args.threshhold != None:
    threshhold *= args.threshhold
E = Events.Events(fs=fs, threshhold=threshhold)
E.events()
E.show_threshhold()
E.show_fraction()
E.save(hist=hist, events=events, lengths=lengths, rates=rates, zeros=zeros, sdevs=sdevs)
#E.events(find=(None, None))
#E.plot_Event(args.dir + "/event_plot.png", title='Largest event with sdev < 0.5', read=True, write=True)
