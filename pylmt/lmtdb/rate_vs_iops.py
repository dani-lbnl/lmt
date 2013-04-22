#!/usr/bin/env python
# rate_vs_iops.py
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

import argparse
import MySQLdb
import numpy as np
import matplotlib.pyplot as plt
import time

from pyLMT import LMTConfig, Timestamp, TimeSteps, Graph, Bulk, BrwFS

#*******************************************************************************
# Support for basic calling conventions
def process_args(main=False):
    """
    The command line arguments needed for operating the OST class as
    a simple script.
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-o', '--ost', default=None, type=str, help='Name of the OST to examine')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (((args.begin == None) and (args.end != None)) or
        ((args.begin != None) and (args.end == None))):
        print "rate_vs_iops.validate_args(): Please provide both a begin and an end argument (or neither for the default)"
        return(None)
    # By default do both read and write
    if (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    args.stat = None
    args.bin  = None
    return(args)

#*******************************************************************************
# callable main function for working interactively
def do_main(args):
    """
    It looks like it is possible to get an incomplet coverage of the set of time
    steps if you only get rate and brw_stats data for one OST. I should fix this
    in the base modules.
    """
    fsrc = LMTConfig.process_configuration(args)
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    thisost = None
    thisbo  = None
    bulk = Bulk.Bulk(fsrc['name'])
    bulk.getOSSs(fsrc['conn'])
    bulk.setSteps(Steps)
    bulk.getData()
    bulk.getCPU()
    brwfs = BrwFS.BrwFS(fsrc['name'])
    brwfs.getOSSs(fsrc['conn'])
    brwfs.getBrwStats(fsrc['conn'], args.stat)
    brwfs.getData(beginTimestamp, endTimestamp, args.stat)
    return(bulk, brwfs)

#*******************************************************************************
def do_ost(ax, ost, brwost):
    # If we always want the BRW_IOSIZES histogram it is always '7'
    xvals = ost.OST.Values[1:]/(1024*1024)
    read  = np.diff(brwost.Read[7].Values, axis=1)
    write = np.diff(brwost.Write[7].Values, axis=1)
    yvals = np.sum(read, axis=0) + np.sum(write, axis=0)
    #print "len(xvals) = %d, len(yvals) = %d" % (len(xvals), len(yvals))
    ax.plot(xvals, yvals, 'k,')

#*******************************************************************************
def do_action(args, bulk, brwfs):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if not args.ost is None:
        ost     = bulk.getOST(ost=args.ost)
        brwost  = brwfs.getOST(ost=ost.name)
        do_ost(ax, ost, brwost)
    else:
        for oss in bulk.OSSs:
            for ost in oss.OSTs:
                brwost = brwfs.getOST(ost=ost.name)
                do_ost(ax, ost, brwost)
    ax.set_xbound(lower = 0)
    ax.set_ybound(lower = 0)
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    if not args.ost is None:
        Name = ost.name
    else:
        Name = bulk.name
    plt.title("%s data moved vs IOs on %s" % (dayStr, Name))
    ax.set_xlabel(r"$MB$")
    ax.set_ylabel(r"IOs")
    if args.plot is None:
        plt.show()
    else:
        plt.savefig(args.plot)
    plt.cla()
    return


#*******************************************************************************

if __name__ == "__main__":
    """
    rate_vs_iops.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -o <ost>    The name of the OST to examine
    -p <file>   File name of .png file for graph
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph

    Rudimentary test for OST module.

    """
    args = process_args(main=True)
    if not args is None:
        bulk, brwfs = do_main(args)
        if not ((bulk is None) or (brwfs is None)):
            do_action(args, bulk, brwfs)

