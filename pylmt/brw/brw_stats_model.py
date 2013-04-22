#!/usr/bin/env python
# brw_stats_model.py
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
#   Query the LMT DB for read and write rates and CPU utilization along with
# brw_stats data for a specific statistic. Construct an 'A' matrix from the
# brw_stats data, and a 'y' matrix from the rate and CPU data. Solve 'Ax = y'
# for the 'x' matrix. Print the 'x' matrix. Calculate the 'y\bar' in
# 'y\bar = Ax'. Plot the original 'y' and the 'y\bar'.

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
    parser.add_argument('-H', '--hist', action='store_true', default=False, help='Produce a histogram of the file system utilization')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-o', '--ost', default=None, type=str, help='Name of the OST to examine')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-s', '--stat', default=None, type=str, help='Name of the BRW statistic to examine')
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
        print "brw_stats_model.validate_args(): Please provide both a begin and an end argument (or neither for the default)"
        return(None)
    # By default do both read and write
    if (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    if args.stat is None:
        print "Please provide a statistic for the analysis. Choose one of:"
        print "BRW_RPC, BRW_DISPAGES, BRW_DISBLOCKS, BRW_FRAG,"
        print "BRW_FLIGHT, BRW_IOTIME, BRW_IOSIZE"
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
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    bulk = Bulk.Bulk(fsrc['name'])
    bulk.getOSSs(fsrc['conn'])
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    bulk.setSteps(Steps)
    bulk.getData()
    bulk.getCPU()
    brwfs = BrwFS.BrwFS(fsrc['name'])
    brwfs.getOSSs(fsrc['conn'])
    brwfs.setSteps(Steps)
    if args.stat == 'BRW_IOSIZE':
        iosize_bins = np.array([4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576])
        brwfs.getBrwStats(fsrc['conn'], args.stat, bins=iosize_bins)
    else:
        brwfs.getBrwStats(fsrc['conn'], args.stat)
    brwfs.getData(args.stat)
    return(bulk, brwfs)

#*******************************************************************************
def do_action(args, bulk, brwfs):
    steps = bulk.Steps.Steps[1:]
    num_steps = len(steps)
    times = bulk.Steps.Diff
    # We don't want to distort the analysis by having some observation
    # intervals longer than the nominal five seconds, do divide by the
    # actual elapsed time to produce rates in MB/s, and Count/s
    if not args.ost is None:
        ost = bulk.getOST(ost=args.ost)
        if ost is None:
            print "we didn't find %s" % args.ost
        readMBpS  = ost.Read.Values[1:]
        readMBpS /= (1024*1024)
        writeMBpS = ost.Write.Values[1:]
        writeMBpS /= (1024*1024)
        # no cpu caclulation for one OST
        cpu       = np.zeros_like(readMBpS)
        brwost = brwfs.getOST(ost=ost.name)
        if brwost is None:
            print "we didn't find %s among the brw_stats" % ost.name
            return
        id = brwost.getStatId(args.stat)
        stat_bins = brwost.Bins[brwost.BrwIdDict[id]].Bins
        readHistpS  = np.diff(brwost.Read[id].Values)/times
        readHistpS[np.where(readHistpS.mask==True)] = 0
        writeHistpS = np.diff(brwost.Write[id].Values)/times
        writeHistpS[np.where(writeHistpS.mask==True)] = 0
    else:
        readMBpS  = bulk.Read.Values[1:]
        readMBpS /= (1024*1024)
        writeMBpS = bulk.Write.Values[1:]
        writeMBpS /= (1024*1024)
        cpu       = bulk.CPU.Values[1:]
        readHistpS  = None
        writeHistpS = None
        num_osts = 0
        np.set_printoptions(threshold='nan')
        for oss in bulk.OSSs:
            for ost in oss.OSTs:
                print ost.name
                brwost = brwfs.getOST(ost=ost.name)
                id = brwost.getStatId(args.stat)
                stat_bins = brwost.Bins[brwost.BrwIdDict[id]].Bins
                readpS  = np.diff(brwost.Read[id].Values)/times
                readpS[np.where(readpS.mask==True)] = 0
                if readHistpS is None:
                    readHistpS = np.zeros_like(readpS)
                print readpS
                readHistpS += readpS
                writepS = np.diff(brwost.Write[id].Values)/times
                writepS[np.where(writepS.mask==True)] = 0
                if writeHistpS is None:
                    writeHistpS = np.zeros_like(writepS)
                print writepS
                writeHistpS += writepS
                num_osts += 1
        if (num_osts == 0) or (readHistpS is None) or (writeHistpS is None):
            print "we didn't get anything for the brw_stats data"
            return
        readHistpS /= num_osts
        writeHistpS /= num_osts
    histSeries = np.transpose(np.vstack((readHistpS, writeHistpS)))
    if args.report == True:
        print "%d steps" % len(steps)
    num_bins = len(stat_bins)
    # Now we want to construct the two element distillation of histSeries
    distill = np.zeros((2*num_bins, 4), dtype=np.float64)
    distill[0:num_bins,0] = 1.0
    distill[0:num_bins,1] = stat_bins
    distill[num_bins:2*num_bins,2] = 1.0
    distill[num_bins:2*num_bins,3] = stat_bins
    A = np.matrix(histSeries)*np.matrix(distill)
    # This is the result of the two_element_model.py calculation:
    x = np.matrix([1.62589065e-03, 4.39766334e-09, 3.23092722e-03, 1.72900072e-09])
    yhat = A * np.transpose(x)
    if args.report == True:
        print "yhat:"
        print yhat
    factor = 1
    if args.hist == True:
        if np.max(yhat) > 1.0:
            factor = int(np.max(yhat)) + 1
        utilHist, utilBins = np.histogram(yhat*100.0, bins=100, range=(0.0, 100.0*factor))
        if args.report == True:
            print "utilBins:"
            print utilBins
            print "utilHist:"
            print utilHist
    if args.plot == "noplot":
        return
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, steps, readMBpS, 'r', label='read', Ave=False)
    Graph.timeSeries(ax, steps, writeMBpS, 'b', label='write', Ave=False)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    (handles, labels) = Graph.percent(ax, steps, yhat*100.0, 'k', format="-", label='pct FSU', Ave=False)
    if (not handles is None) and (not labels is None):
        plt.legend(handles, labels)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s File System Utilization" % (dayStr, bulk.name))
    if args.ybound is None:
        ax.set_ybound(lower=0)
    else:
        ax.set_ybound(lower=0, upper=args.ybound)
    if args.plot is None:
        plt.show()
    else:
        plt.savefig(args.plot)
    plt.cla()
    if args.hist == False:
        return
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.bar(utilBins[1:], utilHist, color='k')
    plt.xlabel('pct fs util')
    plt.ylabel('count')
    plt.title('Distribution of File System Utilization')
    if args.plot is None:
        plt.show()
    else:
        plt.savefig('hist_'+args.plot)
    plt.cla()
    return


#*******************************************************************************

if __name__ == "__main__":
    """
    iosize_model.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -H          Produce a histogram of the file system utilization
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

