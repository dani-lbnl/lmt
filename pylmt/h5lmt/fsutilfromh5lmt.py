#!/usr/bin/env python
# Copyright University of California, 2011
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
#   Query the LMT DB for read and write rates and CPU utilization along with
# brw_stats data for a specific statistic. Construct an 'A' matrix from the
# brw_stats data, and a 'y' matrix from the rate and CPU data. Solve 'Ax = y'
# for the 'x' matrix. Print the 'x' matrix. Calculate the 'y\bar' in
# 'y\bar = Ax'. Plot the original 'y' and the 'y\bar'.

import os
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
import time
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
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval hh:mm:ss (default - midnight at the beginning of the day)')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval hh:mm:ss (defaul - midnight at the end of the day)')
    parser.add_argument('-f', '--file', default=None, type=str, help='The hdf5 file')
    parser.add_argument('-F', '--factor', default=None, type=float, help='Set the histogram x-axis (if any) upper bound to 100 percent times this value')
    parser.add_argument('-H', '--hist', action='store_true', default=False, help='Produce a histogram of the file system utilization')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-P', '--progress', action='store_true', default=False, help='Give an indication of progress on the work')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-s', '--stat', default=None, type=str, help='Name of the BRW statistic to examine')
    parser.add_argument('-S', '--save', default=None, type=str, help='HDF5 File to save summary data in')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the file system utilization (y-axis) upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    # By default do both read and write
    if (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    if args.stat is None:
        args.stat = 'BRW_IOSIZE'
    if args.stat != 'BRW_IOSIZE':
        print "Please provide a statistic for the analysis. Choose one of:"
        print "BRW_RPC, BRW_DISPAGES, BRW_DISBLOCKS, BRW_FRAG,"
        print "BRW_FLIGHT, BRW_IOTIME, BRW_IOSIZE"
        print "N.B. Only BRW_IOSIZE is currently supported. Sorry."
        return(None)
    if args.file is None:
        print "Please provide a file"
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
    b_sie = int(time.mktime(time.strptime(begin_str, "%Y-%m-%d %H:%M:%S" )))
    e_sie = int(time.mktime(time.strptime(end_str, "%Y-%m-%d %H:%M:%S" )))
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

def doHist(args, histSeries, yhat, fsStepsDataSet):
    """
    histSeries is the aray of observations, its shape is (num_steps, 2*num_bins).
    num_steps = len(yhat)
    This function will:
    - list all the fsu values outside the standard range
    - construct a histogram of the fus values
    - plot that histogram
    - save data summaries to an HDF5 file which will have:
      o the fsu time series
      o the bins into which that series was divided
      o the counts for each bin
      o the breakdown, for each fsu histogram bin, of the contibution to
        that fsu from each I/O size
    """
    (num_steps, num_sizes) = np.shape(histSeries)
    if args.factor is None:
        args.factor = 200.0
    if (np.max(yhat) > args.factor) and (args.report == True):
        indices, other = np.where(yhat > args.factor)
        for index in indices:
            print "index,fsu: %d, %f" % (index, yhat[index][0])
    utilHist, utilBins = np.histogram(yhat, bins=100, range=(0.0, args.factor))
    if args.report == True:
        print "utilBins:"
        print utilBins
        print "utilHist:"
        print utilHist
    if args.plot != "noplot":
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.bar(utilBins[1:], utilHist, color='k')
        plt.xlabel('FSU')
        plt.ylabel('count')
        plt.title('Distribution of File System Utilization')
        ax.set_xbound(lower=0, upper=args.factor)
        if args.plot is None:
            plt.show()
        else:
            plt.savefig('hist_'+args.plot)
    if args.save is None:
        return
    saveFile = h5py.File(args.save, 'a')
    try:
        fsUtilGroup = saveFile["FSUtilGroup"]
    except KeyError:
        fsUtilGroup = saveFile.create_group("FSUtilGroup")
    try:
        fsUtilDataSet = fsUtilGroup["FSUtilDataSet"]
    except:
        fsUtilDataSet = fsUtilGroup.create_dataset("FSUtilDataSet", data=yhat)
    try:
        fsUtilBinsDataSet = fsUtilGroup["FSUtilBinsDataSet"]
        fsUtilBinsDataSet = utilBins[1:]
    except KeyError:
        fsUtilBinsDataSet = fsUtilGroup.create_dataset("FSUtilBinsDataSet", data=utilBins[1:])
    try:
        fsUtilHistDataSet = fsUtilGroup["FSUtilHistDataSet"]
        fsUtilHistDataSet = utilHist
    except KeyError:
        fsUtilHistDataSet = fsUtilGroup.create_dataset("FSUtilHistDataSet", data=utilHist)
    fsUtilGroup.attrs['day'] = fsStepsDataSet.attrs['day']
    fsUtilGroup.attrs['nextday'] = fsStepsDataSet.attrs['nextday']
    fsUtilGroup.attrs['host'] = fsStepsDataSet.attrs['host']
    fsUtilGroup.attrs['fs'] = fsStepsDataSet.attrs['fs']
    # And save enough info to recapture the contribution from each I/O size to the
    # value in each bin.
    try:
        fsUtilContribDataSet = fsUtilGroup["FSUtilContribDataSet"]
    except:
        fsUtilContribDataSet = fsUtilGroup.create_dataset("FSUtilContribDataSet", shape=(num_sizes, 100))

    last_bin = 0.0
    for index in range(len(fsUtilBinsDataSet)):
        bin = fsUtilBinsDataSet[index]
        # Note that this is a degenerate call to np.where, so it returns a
        # two elelment list where the second element is empty, thus the comma.
        binIndices,extra = np.where(np.logical_and((yhat >= last_bin),(yhat < bin)))
        fsUtilContribDataSet[:,index] = np.sum(histSeries[binIndices,:], axis=0)
        last_bin = bin
    saveFile.close()

#*******************************************************************************

def do_action(args, b_sie, e_sie, fsFile):
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
    if b_index == None:
        print "brw_stats_model_h5lmt.do_action(): Failed to find timestamp index for %d" % b_sie
        return
    if e_index == None:
        print "brw_stats_model_h5lmt.do_action(): Failed to find timestamp index for %d" % e_sie
        return
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    readMBpS = np.zeros(e_index - b_index + 1)
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        readMBpS += ostBulkReadDataSet[ost_index,b_index:e_index+1]
        ost_index += 1
    readMBpS /= 1024*1024
    np.set_printoptions(threshold='nan')
    writeMBpS = np.zeros(e_index - b_index + 1)
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        writeMBpS += ostBulkWriteDataSet[ost_index,b_index:e_index+1]
        ost_index += 1
    writeMBpS /= 1024*1024
    ostIosizeReadDataSet = ostReadGroup['OSTIosizeReadDataSet']
    if args.stat != ostIosizeReadDataSet.attrs['stat']:
        print "We should only be seeing BRW_IOSIZE statistics not %s" % ostIosizeReadDataSet.attrs['stat']
        return
    bins = ostIosizeReadDataSet.attrs['bins']
    ostIosizeWriteDataSet = ostWriteGroup['OSTIosizeWriteDataSet']
    readHistpS  = None
    writeHistpS = None
    ost_index = 0
    if args.report == True:
        np.set_printoptions(threshold='nan')
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        if args.progress == True:
            print "OST %d: %s" % (ost_index, ost_name)
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
    if (ost_index == 0) or (readHistpS is None) or (writeHistpS is None):
        print "we didn't get anything for the brw_stats data"
        return
    readHistpS /= ost_index
    writeHistpS /= ost_index
    histSeries = np.transpose(np.vstack((readHistpS, writeHistpS)))
    if args.report == True:
        print "%d steps" % len(fsStepsDataSet)
    num_bins = len(bins)
    # Now we want to construct the two element distillation of histSeries
    distill = np.zeros((2*num_bins, 4), dtype=np.float64)
    distill[0:num_bins,0] = 1.0
    distill[0:num_bins,1] = bins
    distill[num_bins:2*num_bins,2] = 1.0
    distill[num_bins:2*num_bins,3] = bins
    A = np.matrix(histSeries)*np.matrix(distill)
    # This is the result of the two_element_model.py calculation:
    x = np.matrix([1.62589065e-03, 4.39766334e-09, 3.23092722e-03, 1.72900072e-09])
    yhat = A * np.transpose(x)
    yhat = np.array(yhat)
    yhat *= 100.0
    if args.report == True:
        print "yhat (percent):"
        print yhat
    if args.hist == True:
        doHist(args, histSeries, yhat, fsStepsDataSet)
    if args.plot == "noplot":
        return
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet[b_index:e_index+1], readMBpS, 'r', label='read', Ave=False)
    Graph.timeSeries(ax, fsStepsDataSet[b_index:e_index+1], writeMBpS, 'b', label='write', Ave=False)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    (handles, labels) = Graph.percent(ax, fsStepsDataSet[b_index:e_index+1], yhat, 'k', label='FSU', Ave=False, ybound=200.0)
    if (not handles is None) and (not labels is None):
        plt.legend(handles, labels)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(fsStepsDataSet[0]))
    plt.title("%s %s File System Utilization" % (dayStr, fs))
    if args.ybound is None:
        ax.set_ybound(lower=0, upper=50000)
    else:
        ax.set_ybound(lower=0, upper=args.ybound)
    if args.plot is None:
        plt.show()
    else:
        plt.savefig(args.plot)
    return


#*******************************************************************************

if __name__ == "__main__":
    """
    fsutilfromh5lmt.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -e <end>    Ending time stamp
    -f <file>   The name of the h5lmt file
    -F          Bounds for the histogram
    -h          A help message
    -H          Produce a histogram of the file system utilization
    -p <file>   File name of .png file for graph
    -P          Show the progress of the action
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -s <stat>   The BRW stats histogram to use (only BRW_IOSIZE is currently supported)
    -S <save>   Save summary data in this hdf5 file
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph
    -y (ybound> y-axis maximum value for FSU plot

    Rudimentary test for OST module.

    """
    args = process_args(main=True)
    if not args is None:
        b_sie, e_sie, fsFile = do_main(args)
        if not ((b_sie is None) or (e_sie is None) or (fsFile is None)):
            do_action(args, b_sie, e_sie, fsFile)
            fsFile.close()
