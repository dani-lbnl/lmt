#!/usr/bin/env python
# brwfsdiff.py
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

import sys
import os
import argparse
import MySQLdb
import numpy as np
import numpy.ma as ma
import matplotlib as mpl
# If this is run from a cron job it is not a login process and
# has no X $DISPLAY in the environment. You can prevent pyplot
# from getting confused by telling matplotlib to use the 'Agg'
# backend. On the other hand, if you've already loaded pyplot
# or pylab then it's too late to use the mpl.use('Agg') and would
# generate an obnoxious warning if you try. N.B. The backend
# property is not case sensitive and get_backend() actually
# returns a lower case string.
backend = mpl.get_backend()
if (not 'DISPLAY' in os.environ) or (os.environ['DISPLAY'] == None):
#    print "Bulk: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt

from pyLMT import BrwFS, LMTConfig, Timestamp, TimeSteps, Graph

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-P', '--progress', action='store_true', default=False, help='Give an indication of progress on the work')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-s', '--stat', default=None, type=str, help='The name of one of the stats to show (default: show all)')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    if args.stat is None:
        print "Please provide a BRW statistic to examine"
        print "(one of {BRW_RPC, BRW_DISPAGES, BRW_DISBLOCKS, BRW_FRAG,"
        print "BRW_FLIGHT, BRW_IOTIME, BRW_IOSIZE})"
        return(None)
    if args.begin is None:
        print "Please provide a beginning timestamp in the format \"yyyy-mm-dd hh:mm:ss\""
        return(None)
    if args.end is None:
        print "Please provide a ending timestamp in the format \"yyyy-mm-dd hh:mm:ss\""
        return(None)
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         config - (file) The lmtrc config file telling how to get to the DB
         end - (string) As above giving the end of the data to be gathered.
         fs - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         progress - Print the name of each OST as you work through the list
         report - (boolean) Print out summary info about the analyzed operations
         read - Plot read data rate
         stat - Show histograms only for this statistic
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         write - Plot the write rate
    """
    # do_main will:
    # - process_configuration
    # - get the oss in question
    # - get the OSTs on it
    # - Process timestamps
    # - get the data
    # - return the oss
    bins = None
    if args.stat == "BRW_IOSIZE":
        bins = np.array([4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576])
    fsrc = LMTConfig.process_configuration(args)
    sie = Timestamp.calc_sie(args.begin)
    beginTimestamp = Timestamp.Timestamp(sie)
    endTimestamp   = Timestamp.Timestamp(sie + 5)
    beginTimestamp.no_later_than(fsrc['conn'])
    endTimestamp.no_earlier_than(fsrc['conn'])
    if beginTimestamp.ts_id >= endTimestamp.ts_id:
        print "observation: failed to get time stamp range for %s" % args.time
        exit(1)
    brwBegin = BrwFS.BrwFS(fsrc['dbname'])
    brwBegin.getOSSs(fsrc['conn'])
    if args.verbose == True:
        brwBegin.debug()
        brwBegin.debug(module="BrwFS")
        #oss.debug(module="Timestamp")
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    brwBegin.setSteps(Steps)
    brwBegin.getBrwStats(conn=fsrc['conn'], stat=args.stat, bins=bins)
    brwBegin.getDataSlice(stat=args.stat)
    if (brwBegin.Bins is None) or (len(brwBegin.Bins) == 0):
        print "test_BrwFS: Warning - No HistBins objects from OSS %s" % oss.name
        return
    if (brwBegin.Steps is None) or (brwBegin.Steps.steps() == 0):
        print "test_BrwFS: Warning - No steps from OSS %s" % oss.name
        return
    sie = Timestamp.calc_sie(args.end)
    beginTimestamp = Timestamp.Timestamp(sie)
    endTimestamp   = Timestamp.Timestamp(sie + 5)
    beginTimestamp.no_later_than(fsrc['conn'])
    endTimestamp.no_earlier_than(fsrc['conn'])
    if beginTimestamp.ts_id >= endTimestamp.ts_id:
        print "observation: failed to get time stamp range for %s" % args.time
        return(None)
    brwEnd = BrwFS.BrwFS(fsrc['dbname'])
    brwEnd.getOSSs(fsrc['conn'])
    if args.verbose == True:
        brwEnd.debug()
        brwEnd.debug(module="BrwFS")
        #oss.debug(module="Timestamp")
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    brwEnd.setSteps(Steps)
    brwEnd.getBrwStats(conn=fsrc['conn'], stat=args.stat, bins=bins)
    brwEnd.getDataSlice(stat=args.stat)
    if (brwEnd.Bins is None) or (len(brwEnd.Bins) == 0):
        print "test_BrwFS: Warning - No HistBins objects from OSS %s" % oss.name
        return
    if (brwEnd.Steps is None) or (brwEnd.Steps.steps() == 0):
        print "test_BrwFS: Warning - No steps from OSS %s" % oss.name
        return
    return(brwBegin, brwEnd)

#*******************************************************************************

def roundNumber(x):
    negative = False
    if x < 0:
        negative = True
        x = -x
    i = 0
    if x > 1:
        while x > 10:
            x /= 10
            i += 1
        x = int(round(x + 1))
        while i > 0:
            x *= 10
            i -= 1
    else:
        while x < 1.0:
            x *= 10
            i += 1
        x = float(round(x + 1))
        while i > 0:
            x /= 10
            i -= 1
    if negative == True:
        x = -x
    return( x )

#*******************************************************************************

def do_action(args, brwBegin, brwEnd):
    bins = None
    if args.stat == "BRW_IOSIZE":
        bins = np.array([4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576])
    mode = None
    if (args.read == True) and (args.write == False):
        mode = 'read'
    if (args.read == False) and (args.write == True):
        mode = 'write'
    read  = np.zeros(len(bins), dtype=np.float64)
    write = np.zeros(len(bins), dtype=np.float64)
    for e_oss in brwEnd.OSSs:
        for e_ost in e_oss.OSTs:
            if args.progress == True:
                print e_ost.name
            e_id = e_ost.getStatId("BRW_IOSIZE")
            num_masked = ma.count_masked(e_ost.Read[e_id].Values[:,0])
            if num_masked > 0:
                print "%s Read still has %d masked values at the end" % (e_ost.name, num_masked)
                continue
            b_ost = brwBegin.getOST(ost=e_ost.name)
            b_id = b_ost.getStatId("BRW_IOSIZE")
            num_masked = ma.count_masked(b_ost.Read[b_id].Values[:,0])
            if num_masked > 0:
                print "%s Read still has %d masked values at begin" % (e_ost.name, num_masked)
                continue
            read += e_ost.Read[e_id].Values[:,0] - b_ost.Read[b_id].Values[:,0]
            num_masked = ma.count_masked(e_ost.Write[e_id].Values[:,0])
            if num_masked > 0:
                print "%s Write still has %d masked values at the end" % (e_ost.name, num_masked)
                continue
            num_masked = ma.count_masked(b_ost.Write[b_id].Values[:,0])
            if num_masked > 0:
                print "%s Write still has %d masked values at begin" % (e_ost.name, num_masked)
                continue
            write += e_ost.Write[e_id].Values[:,0] - b_ost.Write[b_id].Values[:,0]
    if args.report == True:
        print "Read:"
        print read
        print "Write:"
        print write
    if args.plot == "noplot":
        return
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    width = 0.35
    x = np.arange(len(bins))
    offset=0.0
    ax.bar(x+offset, read, width=width, color='r', label='read')
    offset += width
    ax.bar(x+offset, write, width=width, color='b', label='write')
    ax.set_ylabel('Count')
    ax.set_xlabel('bytes')
    ax.set_title('BRW_IOSIZE histogram')
    ax.set_xticks(x+width)
    ax.set_xticklabels( bins, rotation=45, horizontalalignment='right' )
    ax.legend()
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( args.plot )
    plt.cla()



#*******************************************************************************

if __name__ == "__main__":
    """
    test_BrwOSS.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -o <oss>    The name of the OSS to examine
    -p <file>   File name of .png file for graph
    -P          Print the name of each OST as you work through the list
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -s <stat>   Show histograms only for the <stat> statistics
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph

    A rudimentary test for the BrwOSS module.

    """
    args = process_args(main=True)
    if not args is None:
        (brwBegin, brwEnd) = do_main(args)
        if not ((brwBegin is None) or (brwEnd is None)):
            do_action(args, brwBegin, brwEnd)

