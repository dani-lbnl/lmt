#!/usr/bin/env python
# dailyh5lmt.py
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
#   Produce a daily report of graphs and text summaries from the data sequestered
# in a filesystem's hdf5 file.

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
    parser.add_argument('-f', '--file', default=None, type=str, help='The hdf5 file (default: .')
    parser.add_argument('-p', '--progress', action='store_true', default=False, help='Give an indication of progress on the work')
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
        exit(1)
    return(args)

#*******************************************************************************

def doMain(args):
    fsFile = h5py.File(args.file, 'r')
    return(fsFile)

#*******************************************************************************

def doRatePlot(args, fsFile):
    """
    The values in the h5lmt arrays are time series of observaitons
    of true rates in in MB/s. In order to aggregate you need to
    multiply by the interval lengths. Those are hard coded to
    five seconds in the current implementation, but that is not
    something to necessarily rely on going forward.
    """
    if args.progress == True:
        print "Rate plot"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    intervals = np.zeros(len(fsStepsDataSet))
    intervals[1:] = np.diff(fsStepsDataSet)
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    read = np.zeros(len(fsStepsDataSet))
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        read += ostBulkReadDataSet[ost_index,:]
        ost_index += 1
    read /= (1024*1024)
    AggregateRead = np.sum(read*intervals)
    write = np.zeros(len(fsStepsDataSet))
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        write += ostBulkWriteDataSet[ost_index,:]
        ost_index += 1
    write /= (1024*1024)
    AggregateWrite = np.sum(write*intervals)
    cpu = np.zeros(len(fsStepsDataSet))
    ossCPUGroup = fsFile['OSSCPUGroup']
    ossCPUDataSet = ossCPUGroup['OSSCPUDataSet']
    oss_index = 0
    for oss_name in ossCPUDataSet.attrs['OSSNames']:
        cpu += ossCPUDataSet[oss_index,:]
        oss_index += 1
    cpu /= oss_index
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet, read, 'r', label='read', Ave=True)
    Graph.timeSeries(ax, fsStepsDataSet, write, 'b', label='write', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    (handles, labels) = Graph.percent(ax, fsStepsDataSet, cpu, color='k', label='% CPU', Ave=True)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s aggregate I/O" % (fsStepsDataSet.attrs['day'],
                                       fsStepsDataSet.attrs['fs']))
    ax.set_ybound(lower = 0, upper = 50000)
    if (not handles is None) and (not labels is None):
        plt.legend(handles, labels)
    else:
        plt.legend()
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    plt.savefig(path+'/'+host+'_'+fs+"_bulkRateCPU.png")
    plt.cla()
    return(AggregateRead, AggregateWrite)

#*******************************************************************************

def doMDSPlot(args, fsFile):
    if args.progress == True:
        print "MDS plot"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    intervals = np.zeros(len(fsStepsDataSet))
    intervals[1:] = np.diff(fsStepsDataSet)
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    mds = np.zeros(len(fsStepsDataSet))
    mdsOpsGroup = fsFile['MDSOpsGroup']
    mdsOpsDataSet = mdsOpsGroup['MDSOpsDataSet']
    op_index = 0
    for op_name in mdsOpsDataSet.attrs['OpNames']:
        mds += mdsOpsDataSet[op_index,:]
        op_index += 1
    highVals, = np.where(mds > 100000)
    if len(highVals) > 0:
        print "Warning: Exceedingly high values reported for ", highVals
        print fsStepsDataSet[mds > 1000000]
        print mds[mds > 1000000]
    AggregateOps = np.sum(mds*intervals)
    cpu = np.zeros(len(fsStepsDataSet))
    mdsCPUGroup = fsFile['MDSCPUGroup']
    mdsCPUDataSet = mdsCPUGroup['MDSCPUDataSet']
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet, mds, 'b', label='mds', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$Ops/sec$')
    (handles, labels) = Graph.percent(ax, fsStepsDataSet, mdsCPUDataSet, color='k', label='% CPU', Ave=True)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s Metadata Operations" % (fsStepsDataSet.attrs['day'],
                                             fsStepsDataSet.attrs['fs']))
    ax.set_ybound(lower = 0, upper = 120000)
    if (not handles is None) and (not labels is None):
        plt.legend(handles, labels)
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    plt.savefig(path+'/'+host+'_'+fs+"_MDS+CPU.png")
    plt.cla()
    return(AggregateOps)

#*******************************************************************************

def doOSSCPUPlot(args, fsFile):
    if args.progress == True:
        print "OSS CPU plot"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    data = np.zeros(len(fsStepsDataSet))
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        data += ostBulkReadDataSet[ost_index,:]
        ost_index += 1
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        data += ostBulkWriteDataSet[ost_index,:]
        ost_index += 1
    cpu = np.zeros(len(fsStepsDataSet))
    ossCPUGroup = fsFile['OSSCPUGroup']
    ossCPUDataSet = ossCPUGroup['OSSCPUDataSet']
    oss_index = 0
    for oss_name in ossCPUDataSet.attrs['OSSNames']:
        cpu += ossCPUDataSet[oss_index,:]
        oss_index += 1
    cpu /= oss_index
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet, data, 'b', label='read plus write', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    (handles, labels) = Graph.percent(ax, fsStepsDataSet, cpu, color='k', label='% CPU', Ave=True)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s aggregate I/O" % (fsStepsDataSet.attrs['day'],
                                       fsStepsDataSet.attrs['fs']))
    ax.set_ybound(lower = 0, upper = 50000)
    if (not handles is None) and (not labels is None):
        plt.legend(handles, labels)
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    plt.savefig(path+'/'+host+'_'+fs+"_bulkBothCPU.png")
    plt.cla()
    return

#*******************************************************************************

def doPowerSpectrum(args, fsFile):
    if args.progress == True:
        print "Power spectrum plot"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    read = np.zeros(len(fsStepsDataSet))
    readHist, bins = np.histogram(read, bins=1000, range=(0.0, 2500.0))
    readHist[0] = 0.0
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        hist, bins = np.histogram(ostBulkReadDataSet[ost_index,:], bins=1000, range=(0.0, 2500.0))
        readHist += hist
        ost_index += 1
    readHist *= bins[:-1]
    write = np.zeros(len(fsStepsDataSet))
    writeHist, bins = np.histogram(write, bins=1000, range=(0.0, 2500.0))
    writeHist[0] = 0.0
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        hist, bins = np.histogram(ostBulkWriteDataSet[ost_index,:], bins=1000, range=(0.0, 2500.0))
        writeHist += hist
        ost_index += 1
    writeHist *= bins[:-1]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(bins[:-1], readHist, 'r-', drawstyle='steps', label='read')
    ax.plot(bins[:-1], writeHist, 'b-', drawstyle='steps', label='write')
    plt.xlabel(r'$MiB$')
    plt.ylabel(r'$MiB$')
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s per-OST Power Spectrum" % (fsStepsDataSet.attrs['day'],
                                       fsStepsDataSet.attrs['fs']))
    plt.legend()
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    plt.savefig(path+'/'+host+'_'+fs+"_power.png")
    plt.cla()
    return

#*******************************************************************************

def doFourier(args, fsFile):
    if args.progress == True:
        print "Fourier plot"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    read = np.zeros(len(fsStepsDataSet))
    readFft = np.fft.hfft(read)
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        readFft += np.fft.hfft(ostBulkReadDataSet[ost_index,:])
        ost_index += 1
    write = np.zeros(len(fsStepsDataSet))
    writeFft = np.fft.hfft(write)
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        writeFft += np.fft.hfft(ostBulkWriteDataSet[ost_index,:])
        ost_index += 1
    fig = plt.figure()
    x = range(len(readFft))
    ax = fig.add_subplot(111)
    ax.plot(x, readFft, 'r-', drawstyle='steps', label='read')
    ax.plot(x, writeFft, 'b-', drawstyle='steps', label='write')
    plt.xlabel('frequecy')
    plt.ylabel(r'$count$')
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s Fourier Transform" % (fsStepsDataSet.attrs['day'],
                                       fsStepsDataSet.attrs['fs']))
    plt.legend()
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    plt.savefig(path+'/'+host+'_'+fs+"_fourier.png")
    plt.cla()
    return

#*******************************************************************************

def autocorr(x):
    result = np.correlate(x, x, mode='full')
    return result[result.size/2:]

#*******************************************************************************

def doAutoCorrelation(args, fsFile):
    if args.progress == True:
        print "Auto-correlation plot"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    read = np.zeros(len(fsStepsDataSet))
    test = autocorr(read)
    readAutoCorr = np.zeros_like(test)
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        readAutoCorr += autocorr(ostBulkReadDataSet[ost_index,:])
        ost_index += 1
    write = np.zeros(len(fsStepsDataSet))
    test = autocorr(write)
    writeAutoCorr = np.zeros_like(test)
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        writeAutoCorr += autocorr(ostBulkWriteDataSet[ost_index,:])
        ost_index += 1
    fig = plt.figure()
    x = range(len(readAutoCorr))
    ax = fig.add_subplot(111)
    ax.plot(x[1:], readAutoCorr[1:], 'r-', drawstyle='steps', label='read')
    ax.plot(x[1:], writeAutoCorr[1:], 'b-', drawstyle='steps', label='write')
    plt.xlabel('separation')
    plt.ylabel(r'$correlation$')
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s Autocorreation" % (fsStepsDataSet.attrs['day'],
                                       fsStepsDataSet.attrs['fs']))
    plt.legend()
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    plt.savefig(path+'/'+host+'_'+fs+"_auto-cor.png")
    plt.cla()
    return

#*******************************************************************************

def doDailySummary(args, fsFile, AggregateRead, AggregateWrite):
    if args.progress == True:
        print "Daily summary data gathering"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    fs = fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    interval = fsStepsDataSet[-1] - fsStepsDataSet[0]
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    f = open(path+'/'+host+'_'+fs+'.log', 'w')
    f.write("\"%s\"\t%s\t%16.4lf\t%16.4lf\t%16.4f\t%16.4f\n" % (fsStepsDataSet.attrs['day']+' 00:00:00',
                                                              fsStepsDataSet[0],
                                                              AggregateRead/1024.0,
                                                              AggregateWrite/1024.0,
                                                              AggregateRead/(1024.0*interval),
                                                              AggregateWrite/(1024.0*interval)))
    f.close()
    return

#*******************************************************************************

def doDailyMDSSummary(args, fsFile, AggregateOps):
    if args.progress == True:
        print "Daily metadata summary data gathering"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    fs = fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    interval = fsStepsDataSet[-1] - fsStepsDataSet[0]
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    f = open(path+'/'+host+'_'+fs+'_mds.log', 'w')
    f.write("\"%s\"\t%s\t%16.4lf\t%16.4f\n" % (fsStepsDataSet.attrs['day']+' 00:00:00',
                                               fsStepsDataSet[0],
                                               AggregateOps,
                                               AggregateOps/interval))
    f.close()
    return

#*******************************************************************************

def doMissingPackets(args, fsFile):
    if args.progress == True:
        print "Missing packets plot"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    missing = np.zeros(len(fsStepsDataSet))
    ossCPUGroup = fsFile["OSSCPUGroup"]
    ossCPUDataSet = ossCPUGroup["OSSCPUDataSet"]
    fsMissingGroup = fsFile['FSMissingGroup']
    fsMissingDataSet = fsMissingGroup['FSMissingDataSet']
    Missing = np.zeros(len(fsStepsDataSet))
    oss_index = 0
    for oss_name in ossCPUDataSet.attrs["OSSNames"]:
        Missing += fsMissingDataSet[oss_index,:]
        oss_index += 1
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet, Missing, 'r', label='misssing', Ave=False, format='+')
    plt.xlabel('time')
    plt.ylabel(r'$count$')
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s Missing Packets" % (fsStepsDataSet.attrs['day'],
                                         fsStepsDataSet.attrs['fs']))
    ax.set_ybound(lower = 0, upper = 156)
    plt.legend()
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    plt.savefig(path+'/'+host+'_'+fs+"_Missing.png")
    plt.cla()
    return

#*******************************************************************************

def doAction(args, fsFile):
    (AggregateRead, AggregateWrite) = doRatePlot(args, fsFile)
    AggregateOps = doMDSPlot(args, fsFile)
    #doOSSCPUPlot(args, fsFile)
    doPowerSpectrum(args, fsFile)
    doFourier(args, fsFile)
    doAutoCorrelation(args, fsFile)
    doDailySummary(args, fsFile, AggregateRead, AggregateWrite)
    doDailyMDSSummary(args, fsFile, AggregateOps)
    doMissingPackets(args, fsFile)

#*******************************************************************************

if __name__ == "__main__":
    """
    daily.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -d <dir>    Optional directory in which to drop file data
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        fsFile = doMain(args)
        if not fsFile is None:
            doAction(args, fsFile)
            fsFile.close()
