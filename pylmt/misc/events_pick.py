#!/usr/bin/env python
# events_pick.py
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
#   Use the Events.py module to read in OST data and search the
# events for one that meets the designated criteria.
#
# By default find the longest event with sdev < 0.5. If sdev is
# specified then the longest event with that value for sdev.
# If the length is specified then the lowest sdev with that length.
# If both are specified then the first event with those two
# values. If there is no such event just return silently.
#
# This code needs to be retired. There is probably a need for a
# more general event search mechanism.

import sys
import argparse
import os
import re
import Events

parser = argparse.ArgumentParser(description='Access an LMT DB')
parser.add_argument('-d', '--dir', default='.', type=str, help='Do the events calculations for this directory')
parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
parser.add_argument('-l', '--length', default=None, type=float, help='')
parser.add_argument('-p', '--plot', action='store_true', default=False, help='send the plot(s) to a file')
parser.add_argument('-r', '--read', action='store_true', default=False, help='Plot read distribution')
parser.add_argument('-s', '--sdev', default=None, type=float, help=' A limit on the ratio of the standard deviation to the average')
parser.add_argument('-t', '--title', default=None, type=str, help='Title for the graph(s)')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
parser.add_argument('-w', '--write', action='store_true', default=False, help='Plot write distribution')
parser.add_argument('-x', '--xbound', default=None, type=float, help='Set the x-axis upper bound to this value')
parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
args = parser.parse_args()
fs = data.FS(args.dir, args.fs)
fs.get_ost_data()
fs.aggregate()
fs.differentials()
E = data.Events(fs)
E.events(find=(args.length, args.sdev))
E.show_Event()
if args.title == None:
    title = args.dir
    if args.sdev == None:
        if args.length == None:
            title += ": longest event with sdev/ave < 0.5"
        else:
            title += ": event of length %d with lowest sdev/ave"
        #if args.length == None:
    else:
        if args.length == None:
            title += ": longest event with %f <= sdev/ave < %f" % (args.sdev, args.sdev + 0.1)
        else:
            title += ": length = %d, %f <= sdev/ave < %f" % (args.length, args.sdev, args.sdev + 0.1)
        #if args.length == None:
    #if args.sdev == None:
else:
    title = args.title
#if args.title == None:

if args.plot == True:
    E.plot_Event(args.dir + "/event_plot.png", xbound=args.xbound, ybound=args.ybound, title=title, read=args.read, write=args.write)
else:
    E.plot_Event(xbound=args.xbound, ybound=args.ybound, title=title, read=args.read, write=args.write)
