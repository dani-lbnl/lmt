#!/usr/bin/env python
# h5lmt.py
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
#   Query the LMT DB for data from the interval and preserve the resulting
# objects as (one or more) hdf5 files.

import os
import sys
import argparse
import MySQLdb
import numpy as np
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
    those things required by the __man__ script.
    """
    # make new arguments for: args.osts, .osss, .mds, .iosize
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried if not the beginning of the file')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried if not the end of the file')
    parser.add_argument('-f', '--file', default=None, type=str, help='The name of the file previously initialized by h5lmt_init.py')
    parser.add_argument('-i', '--iosize', action='store_true', default=False, help='Process the BRW_IOSIZE data for the interval')
    parser.add_argument('-m', '--mds', action='store_true', default=False, help='Process the MDS data for the interval')
    parser.add_argument('-o', '--osss', action='store_true', default=False, help='Process the OSS data for the interval')
    parser.add_argument('-O', '--osts', action='store_true', default=False, help='Process the bulk OST data for the interval')
    parser.add_argument('-p', '--progress', action='store_true', default=False, help='Give an indication of progress on the work')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args,fsFile = validate_args(args)
    return(args, fsFile)

#*******************************************************************************
def validate_args(args):
    try:
        fsFile = h5py.File(args.file, 'r+')
    except:
        print "%s does not appear to exist. Do you need to use h5lmt_init.py?"
        return(None)
    fsStepsGroup = fsFile["FSStepsGroup"]
    fsStepsDataSet = fsStepsGroup["FSStepsDataSet"]
    args.day = fsStepsDataSet.attrs["day"]
    if args.begin is None:
        args.begin = args.day + ' 00:00:00'
    if args.end is None:
        args.end = fsStepsDataSet.attrs["nextday"] + '00:00:00'
    args.host = fsStepsDataSet.attrs["host"]
    args.fs = 'filesystem_' + fsStepsDataSet.attrs["fs"]
    # The timestamp processing still want to see the old 'index' value,
    # just subvert it
    args.index = None
    return(args, fsFile)

#*******************************************************************************
# callable main function for working interactively
def doMain(args):
    """
    It looks like it is possible to get an incomplete coverage of the set of time
    steps if you only get rate and brw_stats data for one OST. I should fix this
    in the base modules.
    """
    fsrc = LMTConfig.process_configuration(args)
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    fs = FS.FS(fsrc['name'])
    fs.getInfo(fsrc['conn'])
    fs.setSteps(Steps)
    brwfs = BrwFS.BrwFS(fsrc['name'])
    brwfs.getOSSs(fsrc['conn'])
    iosize_bins = np.array([4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576])
    brwfs.setSteps(Steps)
    brwfs.getBrwStats(fsrc['conn'], stat="BRW_IOSIZE", bins=iosize_bins)
    return(fs, brwfs)

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

def doSteps(args, fsFile, fs):
    """
    If we just call doSteps (i.e. no -i, -m, -o, or -O) then this
    will verify that the requested interval is in range or print an
    error message.
    """
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    if ((fs.begin.sie < fsStepsDataSet[0]) or (fs.begin.sie > fsStepsDataSet[-1])):
        print "begin timestamp %d is outside the range for the file" % fs.begin.sie
        return(None, None)
    if ((fs.end.sie < fsStepsDataSet[0]) or (fs.end.sie > fsStepsDataSet[-1])):
        print "end timestamp %d is outside the range for the file" % fs.end.sie
        return(None, None)
    begin_index = find_sie(fs.begin.sie, fsStepsDataSet)
    end_index   = find_sie(fs.end.sie, fsStepsDataSet)
    if args.verbose == True:
        print "Process data from index %d to %d" % (begin_index, end_index)
    return(begin_index, end_index)

#*******************************************************************************

def doOSTs(args, fsFile, fs, begin_index, end_index):
    """
    We may want to introduce partial OST coverage and a progress
    meter as well
    """
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet  = ostReadGroup['OSTBulkReadDataSet']
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet  = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        if args.progress == True:
            print "%s " % ost_name,
        ost = fs.Bulk.getOST(ost=ost_name)
        # N.B. Data comes back from the pyLMT interface as a time
        # series of observations of true rates in MB/s
        ost.getData(fs.conn)
        i = 0
        for index in range(begin_index, end_index+1):
            if fs.Steps.Steps[i] == fsStepsDataSet[index]:
                if ost.Missing[i] == 0:
                    #print "Missing: ost %d, index %d" % (ost_index, index)
                    ostBulkReadDataSet[ost_index,index] = ost.Read.Values[i]
                    ostBulkWriteDataSet[ost_index,index] = ost.Write.Values[i]
                i += 1
                continue
            if fs.Steps.Steps[i] < fsStepsDataSet[index]:
                print "Out of order step at fs.Steps.Steps = %d" % fs.Steps.Steps[i]
                i += 1
                continue
            # if fs.Steps.Steps[i] > fsStepsDataSet[index]:
            #     Don't do anything in particular, so i stays the same
            #     and index is incremented
        ost_index += 1
    if args.progress == True:
        print
    return

#*******************************************************************************

def doOSSs(args, fsFile, fs, begin_index, end_index):
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    ossCPUGroup = fsFile['OSSCPUGroup']
    ossCPUDataSet  = ossCPUGroup['OSSCPUDataSet']
    fsMissingGroup = fsFile['FSMissingGroup']
    fsMissingDataSet = fsMissingGroup['FSMissingDataSet']
    oss_index = 0
    for oss_name in ossCPUDataSet.attrs['OSSNames']:
        oss = fs.Bulk.getOSS(oss=oss_name)
        oss.getCPU()
        i = 0
        for index in range(begin_index, end_index+1):
            #print "data set %s, Steps %d" % (index, i)
            if fs.Steps.Steps[i] == fsStepsDataSet[index]:
                if oss.Missing[i] == 1:
                    #print "Missing: oss %d, index %d" % (oss_index, index)
                    fsMissingDataSet[oss_index,index] = 1
                else:
                    ossCPUDataSet[oss_index,index] = oss.CPU.Values[i]
                    fsMissingDataSet[oss_index,index] = 0
                i += 1
                continue
            if fs.Steps.Steps[i] < fsStepsDataSet[index]:
                print "Out of order step at fs.Steps.Steps = %d" % fs.Steps.Steps[i]
                i += 1
                continue
            fsMissingDataSet[oss_index,index] = 1
            #print "Missing: oss %d, index %d" % (oss_index, index)
        oss_index += 1
    return

#*******************************************************************************

def doMDS(args, fsFile, fs, begin_index, end_index):
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    mdsOpsGroup = fsFile["MDSOpsGroup"]
    mdsOpsDataSet = mdsOpsGroup["MDSOpsDataSet"]
    mdsCPUGroup = fsFile["MDSCPUGroup"]
    mdsCPUDataSet = mdsCPUGroup["MDSCPUDataSet"]
    fs.MDS.getData()
    fs.MDS.getCPU()
    op_index = 0
    for op_name in mdsOpsDataSet.attrs['OpNames']:
        i = 0
        op = fs.MDS.getOp(name=op_name)
        if op is None:
            continue
        for index in range(begin_index, end_index+1):
            if fs.Steps.Steps[i] < fsStepsDataSet[index]:
                print "Out of order step at fs.Steps.Steps = %d" % fs.Steps.Steps[i]
                while ((fs.Steps.Steps[i] < fsStepsDAtaSet[index]) and (i < len(fs.Steps.Steps))):
                    i += 1
            if fs.Steps.Steps[i] == fsStepsDataSet[index]:
                if op.Missing[i] == 0:
                    mdsOpsDataSet[op_index, index] = op.Values[i]
                i += 1
            # In this case we take no action if we have to skip 'index'
        op_index += 1
    i = 0
    for index in range(begin_index, end_index+1):
        if fs.Steps.Steps[i] < fsStepsDataSet[index]:
            while ((fs.Steps.Steps[i] < fsStepsDAtaSet[index]) and (i < len(fs.Steps.Steps))):
                i += 1
        if fs.Steps.Steps[i] == fsStepsDataSet[index]:
            mdsCPUDataSet[index] = fs.MDS.CPU.Values[i]
            i += 1
        # In this case we take no action if we have to skip 'index',
        # and we've alread reported out of order steps.
    return


#*******************************************************************************

def doIosize(args, fsFile, brwfs, begin_index, end_index):
    """
    We may want to introduce partial OST coverage and a progress
    meter as well
    """
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    # Since we manually take a diff below we're going to start after
    # the first step
    Steps = brwfs.Steps.Steps[1:]
    stepsDiff = brwfs.Steps.Diff
    ostReadGroup = fsFile['OSTReadGroup']
    ostIosizeReadDataSet  = ostReadGroup['OSTIosizeReadDataSet']
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostIosizeWriteDataSet  = ostWriteGroup['OSTIosizeWriteDataSet']
    ost_index = 0
    for ost_name in ostIosizeReadDataSet.attrs['OSTNames']:
        if args.progress == True:
            print "%s " % ost_name,
        i = 0
        ost = brwfs.getOST(ost=ost_name)
        if ost is None:
            continue
        ost.getData(conn=brwfs.conn, stat="BRW_IOSIZE")
        id = ost.getStatId("BRW_IOSIZE")
        Values = ost.Read[id].Values
        Values = np.diff(Values, axis=1)
        Values /= stepsDiff
        Values[np.where(Values.mask==True)] = 0.0
        for index in range(begin_index+1, end_index+1):
            if Steps[i] < fsStepsDataSet[index]:
                print "Out of order step at Steps[%d] = %d, fsStepsDataSet[%d] = %d" % (i, Steps[i], index, fsStepsDataSet[index])
                while ((Steps[i] < fsStepsDAtaSet[index]) and (i < len(Steps))):
                    i += 1
            if Steps[i] == fsStepsDataSet[index]:
                #print "OST %d: index %d, i %d" % (ost_index, index, i)
                ostIosizeReadDataSet[ost_index,:,index] = Values[:,i]
                i += 1
            # In this case we take no action if we have to skip 'index'
        Values = ost.Write[id].Values
        Values = np.diff(Values, axis=1)
        Values /= stepsDiff
        Values[np.where(Values.mask==True)] = 0.0
        i = 0
        for index in range(begin_index+1, end_index+1):
            if Steps[i] < fsStepsDataSet[index]:
                # We already mentioned it above
                while ((Steps[i] < fsStepsDAtaSet[index]) and (i < len(Steps))):
                    i += 1
            if Steps[i] == fsStepsDataSet[index]:
                ostIosizeWriteDataSet[ost_index,:,index] = Values[:,i]
                i += 1
            # In this case we take no action if we have to skip 'index'
        ost_index += 1
    if args.progress == True:
        print
    return

#*******************************************************************************

def doAction(args, fsFile, fs, brwfs):
    """
    We always create the HDF5 file in the context of a 24 hour interval, even if
    a given invocation selects for less than that interval. This will not support
    begin/end pairs that span midnight. The Steps array has the 24 hours of timestamps
    and the data set has room for all 24 hours of data, even if you only put
    a portion into it. Metadata for a series says what it's begin/end pair is.
    To be completely general, I should have a sequence of begin/end pairs and
    should combine those that are contiguous.
    """
    (begin_index, end_index) = doSteps(args, fsFile, fs)
    if (begin_index is None) or (end_index is None):
        return
    if args.osts == True:
        doOSTs(args, fsFile, fs, begin_index, end_index)
    if args.osss == True:
        doOSSs(args, fsFile, fs, begin_index, end_index)
    if args.mds == True:
        doMDS(args, fsFile, fs, begin_index, end_index)
    if args.iosize == True:
        doIosize(args, fsFile, brwfs, begin_index, end_index)
    fsFile.close()
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    h5lmt.py <opts>
    Produce an HDF5 data file with the day's observations
    Options include:
    -c <conf>   Path to configuration file
    -d <dir>    Directory below which to drop daily directory
    -D <day>    Day for wich to gether data
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -v          Print debug messages
    -V          Print the version and exit



    """
    (args, fsFile) = process_args(main=True)
    if not ((args is None) or (fsFile is None)):
        fs, brwfs = doMain(args)
        if not ((fs is None) or (brwfs is None)):
            doAction(args, fsFile, fs, brwfs)

