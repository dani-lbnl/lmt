#!/usr/bin/env python
# observations.py
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
import numpy.ma as ma
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
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-o', '--ost', default=None, type=str, help='Name of the OST to examine')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-s', '--stat', default=None, type=str, help='Name of the BRW statistic to examine')
    parser.add_argument('-t', '--time', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (args.time is None) or (args.stat is None):
        print "observation.validate_args(): Please provide both a stat "
        print "(one of {BRW_RPC, BRW_DISPAGES, BRW_DISBLOCKS, BRW_FRAG,"
        print "BRW_FLIGHT, BRW_IOTIME, BRW_IOSIZE})"
        print "and a time stamp in the format 'YYYY-MM-DD hh:mm:ss'"
        return(None)
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
    sie = Timestamp.calc_sie(args.time)
    beginTimestamp = Timestamp.Timestamp(sie - 5)
    endTimestamp   = Timestamp.Timestamp(sie)
    beginTimestamp.no_later_than(fsrc['conn'])
    endTimestamp.no_earlier_than(fsrc['conn'])
    if beginTimestamp.ts_id >= endTimestamp.ts_id:
        print "observation: failed to get time stamp range for %s" % args.time
        exit(1)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    bulk = Bulk.Bulk(fsrc['name'])
    bulk.getOSSs(fsrc['conn'])
    bulk.setSteps(Steps)
    bulk.getData()
    # FIXME
    # I am not handling masked values in the CPU data
    # Should be an easy fix
    bulk.getCPU()
    brwfs = BrwFS.BrwFS(fsrc['name'])
    brwfs.getOSSs(fsrc['conn'])
    # we'll query for all of them even when we only want one
    # because it make the processing more uniform
    brwfs.getBrwStats(fsrc['conn'], stat=args.stat)
    brwfs.getData(beginTimestamp, endTimestamp, stat=args.stat)
    return(bulk, brwfs)

#*******************************************************************************
def do_action(args, bulk, brwfs):
    step  = bulk.Steps.Diff[0]
    Plots = []
    if not args.ost is None:
        ost = bulk.getOST(ost=args.ost)
        read  = ost.Read.Values[-1]
        write = ost.Write.Values[-1]
        brwost = brwfs.getOST(ost=ost.name)
        if not args.stat in brwost.BrwNameDict:
            print "observation: %s is not in the BrwNameDict for %s" % (args.stat, ost.name)
            exit(1)
        # The array of HistBins objects is arranged in that order, now
        # we need the actual STATS_ID from the DB BRW_STATS_INFO table,
        # since the Read and Write HistSeries objects are indexed by that id
        id = brwost.getStatId(args.stat)
        readHist  = brwost.Read[id].Values[:,-1] - brwost.Read[id].Values[:,0]
        readHist[np.where(readHist.mask==True)] = 0
        Plots.append({'values': readHist,
                      'label':'read',
                      'color':'r'})
        writeHist = brwost.Write[id].Values[:,-1] - brwost.Write[id].Values[:,0]
        writeHist[np.where(writeHist.mask==True)] = 0
        Plots.append({'values': writeHist,
                      'label':'write',
                      'color':'b'})
        bins = brwost.getBins(args.stat)
        units = brwost.getUnits(args.stat)
        if args.report == True:
            print "%s %s" % (ost.name, args.time)
            print "Read = %f MB, write = %f MB" % (float(read)/(1024*1024),
                                                   float(write)/(1024*1024))
            print readHist
            print writeHist
        if args.plot != "noplot":
            Graph.BrwOST_hist(args.stat,
                              bins,
                              units,
                              Plots,
                              args.plot)
    else:
        readMB  = bulk.Read.Values[-1]
        writeMB = bulk.Write.Values[-1]
        if not args.stat in brwfs.BrwNameDict:
            print "observation: %s is not in the BrwNameDict for %s" % (args.stat, ost.name)
            exit(1)
        index = brwfs.BrwNameDict[args.stat]
        id = brwfs.getStatId(args.stat)
        readHist  = None
        writeHist = None
        for oss in brwfs.OSSs:
            for ost in oss.OSTs:
                read  = ost.Read[id].Values[:,-1] - ost.Read[id].Values[:,0]
                read[np.where(read.mask==True)] = 0
                write = ost.Write[id].Values[:,-1] - ost.Write[id].Values[:,0]
                write[np.where(write.mask==True)] = 0
                if readHist is None:
                    readHist = np.zeros_like(read)
                readHist += read
                if writeHist is None:
                    writeHist = np.zeros_like(write)
                writeHist += write
        Plots.append({'values': readHist,
                      'label':'read',
                      'color':'r'})
        Plots.append({'values': writeHist,
                      'label':'write',
                      'color':'b'})
        bins  = brwfs.getBins(args.stat)
        print "There are %d bins" % len(bins)
        units = brwfs.getUnits(args.stat)
        if args.report == True:
            print "%s %s" % (bulk.name, args.time)
            print "Read = %f MB, write = %f MB" % (float(readMB)/(1024*1024),
                                                   float(writeMB)/(1024*1024))
            print readHist
            print writeHist
        if args.plot != "noplot":
            Graph.BrwOST_hist(args.stat,
                              bins,
                              units,
                              Plots,
                              args.plot)
    return


#*******************************************************************************

if __name__ == "__main__":
    """
    observation.py <opts>
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

