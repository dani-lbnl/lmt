#!/usr/bin/env python
# events.py
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
import os
import re
import argparse
import Events

parser = argparse.ArgumentParser(description='Access an LMT DB')
parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
parser.add_argument('-m', '--month', default=None, type=str, help='Aggregate data for the days in the given month into the month\'s directory')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
args = parser.parse_args()
if args.month == None:
    print "You need to provide a -m <month>"
    sys.exit(1)
hist = args.month + "/events_hist.data"
events = args.month + "/events_list.data"
lengths = args.month + "/lengths_hist.data"
zeros = args.month + "/zeros_hist.data"
if not (os.access(hist, os.F_OK) and os.access(events, os.F_OK) and
        os.access(lengths, os.F_OK) and os.access(zeros, os.F_OK)):
    print 'Missing file in', args.month
    sys.exit(1)
Ev = Events.Events()
Ev.load(hist, events, lengths, zeros)
Ev.aves()
