#!/usr/bin/env python
# mdsfromh5lmt.py
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
#   Produce a graph of read and write rates with CPU utilization for the 24
# hours given.

import os
import sys
import argparse
import MySQLdb
import numpy as np
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
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--file', default=None, type=str, help='The hdf5 file (default: .')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
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
    if args.begin is None:
        print "Please provide a begin and end timestamp in the form 'yyyy-mm-dd hh-mm-ss'"
        return(None)
    if args.end is None:
        print "Please provide an end timestamp in the form 'yyyy-mm-dd hh-mm-ss' as well"
        return(None)
    return(args)

#*******************************************************************************

def doMain(args):
    fsFile = h5py.File(args.file, 'r')
    b_sie = Timestamp.calc_sie(args.begin)
    e_sie = Timestamp.calc_sie(args.end)
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
        print "The beginning timestamp %d is outside the date range from %d to %d" % (e_sie, fsStepsDataSet[0], fsStepsDataSet[-1])
        return
    b_index = find_sie(b_sie, fsStepsDataSet)
    e_index = find_sie(e_sie, fsStepsDataSet)
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    mds = np.zeros(e_index - b_index + 1)
    mdsOpsGroup = fsFile['MDSOpsGroup']
    mdsOpsDataSet = mdsOpsGroup['MDSOpsDataSet']
    op_index = 0
    for op_name in mdsOpsDataSet.attrs['OpNames']:
        mds += mdsOpsDataSet[op_index,b_index:e_index+1]
        op_index += 1
    mdsCPUGroup = fsFile['MDSCPUGroup']
    mdsCPUDataSet = mdsCPUGroup['MDSCPUDataSet']
    cpu = mdsCPUDataSet[b_index:e_index+1]
    np.set_printoptions(threshold='nan')
    #print "cpu: ", cpu
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet[b_index:e_index+1], mds, 'g', label='metadata', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$ops/sec$')
    (handles, labels) = Graph.percent(ax, fsStepsDataSet[b_index:e_index+1], cpu, color='k', label='% CPU', Ave=True)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s Metadata Operations" % (fsStepsDataSet.attrs['day'],
                                             fsStepsDataSet.attrs['fs']))
    ax.set_ybound(lower = 0, upper = 120000)
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
        b_sie, e_sie, fsFile = doMain(args)
        if not ((fsFile is None) or (b_sie is None) or (e_sie is None)):
            doAction(args, b_sie, e_sie, fsFile)
            fsFile.close()
