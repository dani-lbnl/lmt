#!/usr/bin/env python
# brwh5lmtdiff.py
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
#   Get the BRW histogram for the interval (summed over the obseravtions of
# the rates) from the HDF5 file for a given (host, files system, day) tuple.
# Produce a graph showing the histogram of the difference.

import os
import sys
import argparse
import MySQLdb
import numpy as np
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
if (not 'DISPLAY' in os.environ) or (os.environ['DISPLAY'] is None):
#    print "bulk: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import datetime
import h5py

from pyLMT import LMTConfig, Timestamp, TimeSteps, Graph, FS, BrwFS

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
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval hh:mm:ss (default - midnight at the beginning of the day)')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval hh:mm:ss (defaul - midnight at the end of the day)')
    parser.add_argument('-f', '--file', default=None, type=str, help='The hdf5 file (default: .')
    parser.add_argument('-o', '--ost', default=None, type=str, help='Name of the OST to examine')
    parser.add_argument('-p', '--plot', default=None, type=str, help='File to save plot in')
    parser.add_argument('-P', '--progress', action='store_true', default=False, help='Give an indication of progress on the work')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-s', '--stat', default=None, type=str, help='Name of the BRW statistic to examine')
    parser.add_argument('-S', '--save', default=None, type=str, help='HDF5 File to save summary data in')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************

def validate_args(args):
    if args.file is None:
        print "Please provide a file"
        return(None)
    if args.stat is None:
        args.stat = 'BRW_IOSIZE'
    if args.stat != "BRW_IOSIZE":
        print "Please provide a statistic for the analysis. Choose one of:"
        print "BRW_RPC, BRW_DISPAGES, BRW_DISBLOCKS, BRW_FRAG,"
        print "BRW_FLIGHT, BRW_IOTIME, BRW_IOSIZE"
        print "N.B. Only BRW_IOSIZE is supported for the -s <stat> option at this time"
        return(None)
    return(args)

#*******************************************************************************

def doMain(args):
    fsFile = h5py.File(args.file, 'r')
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    day=fsStepsDataSet.attrs['day']
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
    b_sie = Timestamp.calc_sie(begin_str)
    e_sie = Timestamp.calc_sie(end_str)
    return(b_sie, e_sie, fsFile)

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

def doAction(args, b_sie, e_sie, fsFile):
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    if (b_sie < fsStepsDataSet[0]) or (b_sie > fsStepsDataSet[-1]):
        print "The beginning timestamp %d is outside the date range from %d to %d" % (b_sie, fsStepsDataSet[0], fsStepsDataSet[-1])
        return
    if (e_sie < fsStepsDataSet[0]) or (e_sie > fsStepsDataSet[-1]):
        print "The ending timestamp %d is outside the date range from %d to %d" % (e_sie, fsStepsDataSet[0], fsStepsDataSet[-1])
        return
    b_index = find_sie(b_sie, fsStepsDataSet)
    e_index = find_sie(e_sie, fsStepsDataSet)
    if args.report == True:
        print "data from index %d to %d" % (b_index, e_index)
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    ostReadGroup = fsFile['OSTReadGroup']
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ostIosizeReadDataSet = ostReadGroup['OSTIosizeReadDataSet']
    if args.stat != ostIosizeReadDataSet.attrs['stat']:
        print "We should only be seeing BRW_IOSIZE statistics not %s" % ostIosizeReadDataSet.attrs['stat']
        return
    bins = ostIosizeReadDataSet.attrs['bins']
    ostIosizeWriteDataSet = ostWriteGroup['OSTIosizeWriteDataSet']
    readHistpS  = None
    writeHistpS = None
    ost_index = 0
    foundOST = False
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        if args.progress == True:
            print "OST %d: %s" % (ost_index, ost_name)
        if not args.ost is None:
            if args.ost == ost_name:
                foundOST = True
            else:
                ost_index += 1
                continue
        readpS = ostIosizeReadDataSet[ost_index,:,b_index:e_index+1]
        if readHistpS is None:
            readHistpS = np.zeros_like(readpS)
        #print readpS
        readHistpS += readpS
        writepS = ostIosizeWriteDataSet[ost_index,:,b_index:e_index+1]
        if writeHistpS is None:
            writeHistpS = np.zeros_like(writepS)
        #print writepS
        writeHistpS += writepS
        ost_index += 1
    if (not args.ost is None) and (foundOST == False):
        print "%s not found" % args.ost
        return
    if (ost_index == 0) or (readHistpS is None) or (writeHistpS is None):
        print "we didn't get anything for the brw_stats data"
        return
    readIosizeHist = np.sum(readHistpS, axis=1)
    writeIosizeHist = np.sum(writeHistpS, axis=1)
    if not args.save is None:
        fsFile = h5py.File(args.save, 'a')
        try:
            fsIosizeGroup = fsFile["FSIosizeGroup"]
        except KeyError:
            fsIosizeGroup = fsFile.create_group("FSIosizeGroup")
        try:
            fsIosizeBinsDataSet = fsIosizeGroup["FSIosizeBinsDataSet"]
            fsIosizeBinsDataSet = bins
        except KeyError:
            fsIosizeBinsDataSet = fsIosizeGroup.create_dataset("FSIosizeBinsDataSet", data=bins)
        try:
            fsIosizeReadDataSet = fsIosizeGroup["FSIosizeReadDataSet"]
            fsIosizeReadDataSet = readIosizeHist
        except KeyError:
            fsIosizeReadDataSet = fsIosizeGroup.create_dataset("FSIosizeReadDataSet", data=readIosizeHist)
        try:
            fsIosizeWriteDataSet = fsIosizeGroup["FSIosizeWriteDataSet"]
            fsIosizeWriteDataSet = writeIosizeHist
        except:
            fsIosizeWriteDataSet = fsIosizeGroup.create_dataset("FSIosizeWriteDataSet", data=writeIosizeHist)
        fsIosizeGroup.attrs['day'] = fsStepsDataSet.attrs['day']
        fsIosizeGroup.attrs['nextday'] = fsStepsDataSet.attrs['nextday']
        fsIosizeGroup.attrs['host'] = fsStepsDataSet.attrs['host']
        fsIosizeGroup.attrs['fs'] = fsStepsDataSet.attrs['fs']
        fsIosizeGroup.attrs['stat'] = ostIosizeReadDataSet.attrs['stat']
        fsIosizeGroup.attrs['bins'] = ostIosizeReadDataSet.attrs['bins']
        fsFile.close()
    if args.plot == "noplot":
        return
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    width = 0.35
    x = np.arange(len(bins))
    offset=0.0
    Graph.bar(ax, x, readIosizeHist, width=width, offset=offset, color='r', label='read')
    offset += width
    Graph.bar(ax, x, writeIosizeHist, width=width, offset=offset, color='b', label='write')
    offset += width
    ax.set_ylabel('Count')
    ax.set_xlabel('MB')
    ax.set_title('%s %s I/O size histogram' % (host, fs) )
    ax.set_xticks(x+width)
    ax.set_xticklabels( bins, rotation=45, horizontalalignment='right' )
    ax.legend()
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( args.plot )
    plt.cla()
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    daily.py <opts>
    Options include:
    -b <end>    Beginning seconds in epoch value
    -c <conf>   Path to configuration file
    -d <dir>    Optional directory in which to drop file data
    -e <end>    Ending seconds in epoch value
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -p          Print a progress update (show each OST as it is examined)
    -s <stat>   Query for <stat> (only BRW_IOSIZE is currently supported)
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        b_sie, e_sie, fsFile = doMain(args)
        if not ((fsFile is None) or (b_sie is None) or (e_sie is None)):
            doAction(args, b_sie, e_sie, fsFile)
            fsFile.close()
