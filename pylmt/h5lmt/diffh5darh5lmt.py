#!/usr/bin/env python
# diffh5darh5lmt.py
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
#   Produce a graph of read and write rates for the 24 hours given based on
# info from the darshan logs.

import os
import sys
import argparse
import MySQLdb
import numpy as np
import matplotlib.pyplot as plt
import time
import datetime
import h5py

from pyLMT import Graph

#*******************************************************************************
# Support for basic calling conventions
def process_args(main=False):
    """
    The command line arguments needed for operating the OST class as
    a simple script.
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    parser = argparse.ArgumentParser(description='Access to the Darshan log summary')
    parser.add_argument('-a', '--ave', action='store_true', default=False, help='Note the average values')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval hh:mm:ss (default - midnight at the beginning of the day)')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval hh:mm:ss (defaul - midnight at the end of the day)')
    parser.add_argument('-f', '--file', default=None, type=str, help='The hdf5 file (default: .')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Print out the read and write values')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************

def validate_args(args):
    if args.file is None:
        print "Please provide a file"
        return(None)
    return(args)

#*******************************************************************************

def doMain(args):
    darFile = h5py.File(args.file, 'r')
    StepsGroup = darFile['DarStepsGroup']
    StepsDataSet = StepsGroup['DarStepsDataSet']
    day=StepsDataSet.attrs['day']
    if args.begin is None:
        begin_str = "%s 00:00:00" % day
    else:
        begin_str = "%s %s" % (day, args.begin)
    if args.end is None:
        midnight_before = datetime.datetime.strptime(day, "%Y-%m-%d")
        midnight_after = midnight_before + datetime.timedelta(days=1)
        end_str = midnight_after.strftime("%Y-%m-%d 00:00:00")
    else:
        end_str = "%s %s" % (day, args.end)
    b_sie = int(time.mktime(time.strptime(begin_str, "%Y-%m-%d %H:%M:%S" )))
    e_sie = int(time.mktime(time.strptime(end_str, "%Y-%m-%d %H:%M:%S" )))
    fs=StepsDataSet.attrs['fs']
    try:
        host=StepsDataSet.attrs['host']
    except:
        host='hopper'
    scratchPath = "/project/projectdirs/pma/www/daily/" + day + "/hopper_scratch.h5lmt"
    scratchFile = h5py.File(scratchPath, 'r')
    scratch2Path = "/project/projectdirs/pma/www/daily/" + day + "/hopper_scratch2.h5lmt"
    scratch2File = h5py.File(scratch2Path, 'r')
    return(b_sie, e_sie, day, fs, host, darFile, scratchFile, scratch2File)

#*******************************************************************************

def find_sie(sie, dataSet):
    first = 0
    last = len(dataSet) - 1
    while first < last:
        mid = int((first+last)/2)
        if sie == dataSet[mid]:
            return(mid)
        if first == last -1:
            if ((dataSet[first] <= sie) and
                (dataSet[last] > sie)):
                return(first)
            if sie == dataSet[last]:
                return(last)
            print "brw_stats_model_h5lmt.find_sie(): Binary seach for %d failed at (%d, %d). Are there outof order timestamp entries?" % (sie, first, last)
            return(None)
        if sie < dataSet[mid]:
            last = mid
        else:
            first = mid

#*******************************************************************************

def ratesfromh5dar(args, b_sie, e_sie, darFile):
    StepsGroup = darFile['DarStepsGroup']
    StepsDataSet = StepsGroup['DarStepsDataSet']
    if (b_sie < StepsDataSet[0]) or (b_sie > StepsDataSet[-1]):
        print "The beginning timestamp %d is outside the date range from %d to %d" % (b_sie, StepsDataSet[0], StepsDataSet[-1])
        return
    if (e_sie < StepsDataSet[0]) or (e_sie > StepsDataSet[-1]):
        print "The ending timestamp %d is outside the date range from %d to %d" % (e_sie, StepsDataSet[0], StepsDataSet[-1])
        return
    fs=StepsDataSet.attrs['fs']
    b_index = find_sie(b_sie, StepsDataSet)
    e_index = find_sie(e_sie, StepsDataSet)
    #print "data from index %d to %d" % (b_index, e_index)
    read = np.zeros(e_index - b_index + 1)
    DarshanGroup = darFile['DarshanGroup']
    readIODataSet = DarshanGroup['ReadIODataSet']
    read = readIODataSet[b_index:e_index+1]
    #print "read:", readIODataSet[b_index:e_index+1]
    read /= (1024*1024)
    write = np.zeros(e_index - b_index + 1)
    writeIODataSet = DarshanGroup['WriteIODataSet']
    write = writeIODataSet[b_index:e_index+1]
    #print "write: ", writeIODataSet[b_index:e_index+1]
    write /= (1024*1024)
    if args.verbose == True:
        np.set_printoptions(threshold='nan')
        print "Darshan data for %s" % fs
        print "\tread:", read
        print "\twrite: ", write
    if args.report == True:
        print "Darshan data for %s" % fs
        print "\tave read: %f" % np.average(read)
        print "\tave write:%f" % np.average(write)
        print "\tmax read: %f" % np.max(read)
        print "\tmax write:%f" % np.max(write)
    return(StepsDataSet[b_index:e_index+1], read, write)

#*******************************************************************************

def ratesfromh5lmt(args, b_sie, e_sie, lmtFile):
    StepsGroup = lmtFile['FSStepsGroup']
    StepsDataSet = StepsGroup['FSStepsDataSet']
    if (b_sie < StepsDataSet[0]) or (b_sie > StepsDataSet[-1]):
        print "The beginning timestamp %d is outside the date range from %d to %d" % (b_sie, StepsDataSet[0], StepsDataSet[-1])
        return
    if (e_sie < StepsDataSet[0]) or (e_sie > StepsDataSet[-1]):
        print "The ending timestamp %d is outside the date range from %d to %d" % (e_sie, StepsDataSet[0], StepsDataSet[-1])
        return
    b_index = find_sie(b_sie, StepsDataSet)
    e_index = find_sie(e_sie, StepsDataSet)
    fs=StepsDataSet.attrs['fs']
    #print "data from index %d to %d" % (b_index, e_index)
    read = np.zeros(e_index - b_index + 1)
    ostReadGroup = lmtFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        #print "OST %s" % ost_name
        read += ostBulkReadDataSet[ost_index,b_index:e_index+1]
        #print "read:", ostBulkReadDataSet[ost_index,b_index:e_index+1]
        ost_index += 1
    read /= (1024*1024)
    write = np.zeros(e_index - b_index + 1)
    ostWriteGroup = lmtFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        #print "OST %s" % ost_name
        write += ostBulkWriteDataSet[ost_index,b_index:e_index+1]
        #print "write: ", ostBulkWriteDataSet[ost_index,b_index:e_index+1]
        ost_index += 1
    write /= (1024*1024)
    if args.verbose == True:
        np.set_printoptions(threshold='nan')
        print "LMT data for %s" % fs
        print "\tread:", read
        print "\twrite: ", write
    if args.report == True:
        print "LMT data for %s" % fs
        print "\tave read: %f" % np.average(read)
        print "\tave write:%f" % np.average(write)
        print "\tmax read: %f" % np.max(read)
        print "\tmax write:%f" % np.max(write)
    return(StepsDataSet[b_index:e_index+1], read, write)

#*******************************************************************************

def doAction(args, b_sie, e_sie, day, fs, host, darFile, scratchFile, scratch2File):
    darSteps, darRead, darWrite = ratesfromh5dar(args, b_sie, e_sie, darFile)
    scratchSteps, scratchRead, scratchWrite = ratesfromh5lmt(args, b_sie, e_sie, scratchFile)
    if len(darSteps) != len(scratchSteps):
        print "len(darSteps) = %d" % len(darSteps)
        print "len(scratchSteps) = %d" % len(scratchSteps)
        print "len(darRead) = %d" % len(darRead)
        print "len(scratchRead) = %d" % len(scratchRead)
        print "len(darWrite) = %d" % len(darWrite)
        print "len(scratchWrite) = %d" % len(scratchWrite)
        return
    if darSteps[0] != scratchSteps[0]:
        print "darSteps[0] = %d" % darSteps[0]
        print "scratchSteps[0] = %d" % scratchSteps[0]
        return
    scratch2Steps, scratch2Read, scratch2Write = ratesfromh5lmt(args, b_sie, e_sie, scratch2File)
    if len(darSteps) != len(scratch2Steps):
        print "len(darSteps) = %d" % len(darSteps)
        print "len(scratch2Steps) = %d" % len(scratch2Steps)
        print "len(darRead) = %d" % len(darRead)
        print "len(scratch2Read) = %d" % len(scratch2Read)
        print "len(darWrite) = %d" % len(darWrite)
        print "len(scratch2Write) = %d" % len(scratch2Write)
        return
    if darSteps[0] != scratch2Steps[0]:
        print "darSteps[0] = %d" % darSteps[0]
        print "scratch2Steps[0] = %d" % scratch2Steps[0]
        return
    if args.plot == "noplot":
        return
    steps = darSteps
    read = darRead - scratchRead - scratch2Read
    write = darWrite - scratchWrite - scratch2Write
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, steps, read, 'r', label='read', Ave=args.ave)
    Graph.timeSeries(ax, steps, write, 'b', label='write', Ave=args.ave)
    (handles, labels) = ax.get_legend_handles_labels()
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s Darshan minus LMT" % (day, fs))
    if args.ybound is None:
        ax.set_ybound(lower = -50000, upper = 50000)
    else:
        ax.set_ybound(lower = -args.ybound, upper = args.ybound)
    if (not handles is None) and (not labels is None):
        plt.legend(handles, labels)
    if args.plot is None:
        plt.show()
    else:
        plt.savefig(args.plot)
    plt.cla()
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    daily.py <opts>
    Options include:
    -f <file>   The HDF5 file to use
    -h          A help message
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        b_sie, e_sie, day, fs, host, darFile, scratchFile, scratch2File = doMain(args)
        if not ((b_sie is None) or
                (e_sie is None) or
                (day is None) or
                (fs is None) or
                (host is None) or
                (darFile is None) or
                (scratchFile is None) or
                (scratch2File is None)):
            doAction(args, b_sie, e_sie, day, fs, host, darFile, scratchFile, scratch2File)
            darFile.close()
            scratchFile.close()
            scratch2File.close()
