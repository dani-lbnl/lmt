#!/usr/bin/env python
# events_plot.py
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
# and plot of the transaction distribution

import sys
import os
import argparse
import numpy as np
import matplotlib.pyplot as plt
import Events

parser = argparse.ArgumentParser(description='Access an LMT DB')
parser.add_argument('-d', '--data', default=".", type=str, help='Directory with previously extracted OST data (default ".")')
parser.add_argument('-e', '--hist', action='store_true', default=False, help='plot the events histogram below the cutoff (default 25,000 MB)')
parser.add_argument('-E', '--events', action='store_true', default=False, help='plot the events above the cutoff (default 25,000 MB)')
parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
parser.add_argument('-k', '--lrate', action='store_true', default=False, help='plot average rate versus length')
parser.add_argument('-l', '--log', action='store_true', default=False, help='plot with a log-scaled y-axis')
parser.add_argument('-L', '--lengths', action='store_true', default=False, help='plot the histogram of event lengths below the cutoff (default 1250 steps)')
parser.add_argument('-p', '--plot', action='store_true', default=False, help='send the plot(s) to a file')
parser.add_argument('-r', '--read', action='store_true', default=False, help='Plot read distribution')
parser.add_argument('-R', '--rates', action='store_true', default=False, help='plot the histogram of event rates below the system maximum (default 500 MB/s)')
parser.add_argument('-s', '--sdevs', action='store_true', default=False, help='plot the histogram standard deviations below the cutoff (default 1250 steps)')
parser.add_argument('-t', '--title', default=None, type=str, help='Title for the graph(s)')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
parser.add_argument('-w', '--write', action='store_true', default=False, help='Plot write distribution')
parser.add_argument('-W', '--weight', action='store_true', default=False, help='Plot weighted distribution over lengths')
parser.add_argument('-x', '--xbound', default=None, type=float, help='Set the x-axis upper bound to this value')
parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
parser.add_argument('-z', '--zbound', default=None, type=float, help='Set the z-axis upper bound to this value')
parser.add_argument('-Z', '--zeros', action='store_true', default=False, help='plot the histogram of zeros below the cutoff (default 1250 steps)')
args = parser.parse_args()
if (args.read == False) and (args.write == False):
    print "You need to specify at least read (-r) or write (-w)"
    sys.exit(1)
E = Events.Events(log=args.log)
hist = args.data + "/events_hist.data"
events = args.data + "/events_list.data"
lengths = args.data + "/lengths_hist.data"
rates = args.data + "/rates_hist.data"
zeros = args.data + "/zeros_hist.data"
sdevs = args.data + "/sdevs_hist.data"
for file in [hist, events, lengths, rates, zeros, sdevs]:
    if not os.access(file, os.F_OK):
        print 'Missing file ', file, 'in', args.data
        sys.exit(1)
E.load(hist, events, lengths, rates, zeros, sdevs)
if args.hist == True:
    fig = E.plot_hist(plot='wait', xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    if args.plot == True:
        plt.savefig(args.data + "/events_hist.png")
    else:
        plt.show()
if args.events == True:
    if args.plot == True:
        E.plot_list(args.data + "/events_list.png", xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    else:
        E.plot_list(xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
if args.lengths == True:
    if args.plot == True:
        E.plot_lengths(args.data + "/lengths_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    else:
        E.plot_lengths(xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
if args.rates == True:
    if args.plot == True:
        E.plot_rates(args.data + "/rates_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    else:
        E.plot_rates(xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
if args.zeros == True:
    if args.plot == True:
        E.plot_zeros(args.data + "/zeros_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    else:
        E.plot_zeros(xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
if args.sdevs == True:
    if args.plot == True:
        E.plot_sdevs(args.data + "/sdevs_hist.png", xbound=args.xbound, ybound=args.ybound, zbound=args.zbound, title=args.title, read=args.read, write=args.write)
    else:
        E.plot_sdevs(xbound=args.xbound, ybound=args.ybound, zbound=args.zbound, title=args.title, read=args.read, write=args.write)
if args.weight == True:
    if args.plot == True:
        E.plot_lengths_weight(args.data + "/lengths_weight_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    else:
        E.plot_lengths_weight(xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
if args.lrate == True:
    if args.plot == True:
        E.plot_lengths_rate(args.data + "/lengths_rates_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    else:
        E.plot_lengths_rate(xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
