#!/usr/bin/env python
# h5lmt_init.py
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
# objects as an hdf5 file. In this revised version I am going to
# assume that there are exactly 24*60*12 + 1 observations from 00:00:00 one
# day to 00:00:00 the next, including the endpoints.
#   I'll build up the data in three or four stages.
# - first, initialize the whole <host>_<fs>.h5lmt file and give it inital
#   values everywhere for the first observation, corresponding to 00:00:00.
#   That assumes that it exists as the last values from the previous day's
#   file.
# - second, query and install values for timesteps, OSS CPU utilizaiton,
#   missing observations, and mds data, and mds cpu. This might be done for
#   a sub-interval of the day or it might be for all of it.
# - third, load data for the designated interval for the bulk OST read and
#   write data. I might be able to do all of them in a go, or I may need to
#   do a few at a time. During this process I can actually gather the
#   missing observations data. It may be that this can be folded in with
#   the second stage.
# - fourth, load data for the designated interval for the IOSIZE histograms.
#   If I do the whole day, this will probably need to be done a few OSTs
#   at a time. If it is done over smaller intervals I may be able to do
#   all OSTs in a go.
# h5lmt_init.py only does the initialization. It does not load any actual
# data values other than the appropriate seconds in epoch values for the
# Steps array. For now I am ignoring the problem with daylight savings time
# in local clocks.

import os
import sys
import argparse
import MySQLdb
import numpy as np
import datetime
import time
import h5py

from pyLMT import LMTConfig, Timestamp, TimeSteps, Graph, FS, BrwFS

#*******************************************************************************
# Support for basic calling conventions
def process_args(main=False):
    """
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-d', '--dir', default=None, type=str, help='The parent directory for the new directory containing the files (default: .')
    parser.add_argument('-D', '--day', default=None, type=str, help='The beginning of the time interval to be queried is at midnight of this day (default: today, format: yyyy-mm-dd')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The host and file system of interest (eg. "hopper_scratch"')
    parser.add_argument('-F', '--force', action='store_true', default=False, help='Noramally we won\'t overwrite an existing file')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if args.day is None:
        d1 = datetime.date.today()
        args.day = d1.strftime("%Y-%m-%d")
    else:
        try:
            d1 = datetime.datetime.strptime(args.day, "%Y-%m-%d")
        except ValueError:
            print "%s is not in the format '%%Y-%%m-%%d'" % args.day
            return(None)
    if args.dir is None:
        args.dir = '.'
    args.begin = args.day + ' 00:00:00'
    d2 = d1 + datetime.timedelta(days=1)
    args.end = d2.strftime("%Y-%m-%d 00:00:00")
    #args.end = d1.strftime("%Y-%m-%d 00:10:00")
    return(args)

#*******************************************************************************

# callable main function for working interactively
def doMain(args):
    """
    """
    fsrc = LMTConfig.process_configuration(args)
    args.host = fsrc['host']
    #beginSie = Timestamp.calc_sie(args.begin)
    #endSie = Timestamp.calc_sie(args.end)
    beginSie = int(time.mktime(time.strptime(args.begin, "%Y-%m-%d %H:%M:%S" )))
    endSie = int(time.mktime(time.strptime(args.end, "%Y-%m-%d %H:%M:%S" )))
    fs = FS.FS(fsrc['name'])
    fs.getInfo(fsrc['conn'])
    return(fs, beginSie, endSie)

#*******************************************************************************
def doAction(args, fs, beginSie, endSie):
    """
    We always create the HDF5 file in the context of a 24 hour interval.
    This will not support begin/end pairs that span midnight. The Steps
    array has the 24 hours of timestamps and the data set has room for all
    24 hours of data. Metadata for a series says what it's begin/end pair is.
    """
    day_string = args.begin[:10]
    data_dir = "%s/%s" % (args.dir, day_string)
    try:
        os.makedirs("%s" % data_dir, 0755)
    except OSError:
        pass
    iosize_bins = np.array([4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576])
    num_bins = len(iosize_bins)
    num_steps = 24*60*12 + 1
    num_osts = fs.Bulk.getNumOSTs()
    num_osss = fs.Bulk.getNumOSSs()
    fsName = "%s/%s_%s.h5lmt" % (data_dir, args.host, fs.name)
    if args.force == False:
        try:
            fsFile = h5py.File(fsName, 'w-')
        except:
            print "It looks like %s exists. Do you really want to overwrite it? Use -F (--force) if so." % fsName
            return
    else:
        fsFile = h5py.File(fsName, 'w')
    fsStepsGroup = fsFile.create_group("FSStepsGroup")
    Steps = np.array(np.arange(beginSie, endSie+5, 5), dtype=np.int32)
    fsStepsDataSet = fsStepsGroup.create_dataset("FSStepsDataSet", data=Steps, dtype=np.int32)
    fsStepsDataSet.attrs['day'] = args.begin[:10]
    fsStepsDataSet.attrs['nextday'] = args.end[:10]
    fsStepsDataSet.attrs['host'] = args.host
    fsStepsDataSet.attrs['fs'] = fs.name
    ostReadGroup = fsFile.create_group("OSTReadGroup")
    ostBulkReadDataSet  = ostReadGroup.create_dataset("OSTBulkReadDataSet", shape=(num_osts, num_steps), dtype=np.float64)
    ostIosizeReadDataSet  = ostReadGroup.create_dataset("OSTIosizeReadDataSet", shape=(num_osts, num_bins, num_steps), dtype=np.float64)
    ostIosizeReadDataSet.attrs['stat'] = "BRW_IOSIZE"
    ostIosizeReadDataSet.attrs['bins'] = iosize_bins
    ostWriteGroup = fsFile.create_group("OSTWriteGroup")
    ostBulkWriteDataSet  = ostWriteGroup.create_dataset("OSTBulkWriteDataSet", shape=(num_osts, num_steps), dtype=np.float64)
    ostIosizeWriteDataSet  = ostWriteGroup.create_dataset("OSTIosizeWriteDataSet", shape=(num_osts, num_bins, num_steps), dtype=np.float64)
    ostIosizeWriteDataSet.attrs['stat'] = "BRW_IOSIZE"
    ostIosizeWriteDataSet.attrs['bins'] = iosize_bins
    ossCPUGroup = fsFile.create_group("OSSCPUGroup")
    ossCPUDataSet  = ossCPUGroup.create_dataset("OSSCPUDataSet", shape=(num_osss, num_steps), dtype=np.float64)
    fsMissingGroup = fsFile.create_group("FSMissingGroup")
    fsMissingDataSet = fsMissingGroup.create_dataset("FSMissingDataSet", shape=(num_osss, num_steps), dtype=np.int32)
    oss_names = []
    ost_names = []
    oss_index = 0
    ost_index = 0
    for oss in fs.Bulk.OSSs:
        oss_names.append(oss.name)
        for ost in oss.OSTs:
            ost_names.append(ost.name)
            ost_index += 1
        oss_index += 1
    if ost_index == 0:
        print "we didn't see any OSTs"
        return
    ossCPUDataSet.attrs['OSSNames'] = oss_names
    ostBulkReadDataSet.attrs['OSTNames'] = ost_names
    ostBulkWriteDataSet.attrs['OSTNames'] = ost_names
    ostIosizeReadDataSet.attrs['OSTNames'] = ost_names
    ostIosizeWriteDataSet.attrs['OSTNames'] = ost_names
    num_ops = fs.MDS.getNumOps()
    mdsOpsGroup = fsFile.create_group("MDSOpsGroup")
    mdsOpsDataSet = mdsOpsGroup.create_dataset("MDSOpsDataSet", shape=(num_ops, num_steps), dtype=np.float64)
    mdsCPUGroup = fsFile.create_group("MDSCPUGroup")
    mdsCPUDataSet = mdsCPUGroup.create_dataset("MDSCPUDataSet", data=np.zeros(num_steps), dtype=np.float64)
    op_index = 0
    op_names = []
    for op in fs.MDS.Ops:
        op_names.append(op.name)
        op_index += 1
    if op_index == 0:
        print "We didn't get any metadata ops"
        return
    mdsOpsDataSet.attrs['OpNames'] = op_names
    # Now see if you can initialize from the end of yesterday
    d1 = datetime.datetime.strptime(args.day, "%Y-%m-%d")
    d2 = d1 - datetime.timedelta(days=1)
    yesterday = d2.strftime("%Y-%m-%d")
    yesterdayFileName = "%s/%s/%s_%s.h5lmt" % (args.dir, yesterday, args.host, fs.name)
    try:
        yesterdayFile = h5py.File(yesterdayFileName, 'r')
    except:
        # If we don't initialize these first values what is the consequence?
        fsFile.close()
        return

    # So we have a file from yesterday
    yesterdayStepsGroup = yesterdayFile["FSStepsGroup"]
    yesterdayStepsDataSet = yesterdayStepsGroup["FSStepsDataSet"]
    # But does it actually end at midnight today?
    if fsStepsDataSet[0] != yesterdayStepsDataSet[-1]:
        return
    yesterdayostReadGroup = yesterdayFile["OSTReadGroup"]
    yesterdayostBulkReadDataSet  = yesterdayostReadGroup["OSTBulkReadDataSet"]
    ostBulkReadDataSet[:,0] = yesterdayostBulkReadDataSet[:,-1]
    yesterdayostIosizeReadDataSet  = yesterdayostReadGroup["OSTIosizeReadDataSet"]
    ostIosizeReadDataSet[:,:,0] = yesterdayostIosizeReadDataSet[:,:,-1]
    yesterdayostWriteGroup = yesterdayFile["OSTWriteGroup"]
    yesterdayostBulkWriteDataSet  = yesterdayostWriteGroup["OSTBulkWriteDataSet"]
    ostBulkWriteDataSet[:,0] = yesterdayostBulkWriteDataSet[:,-1]
    yesterdayostIosizeWriteDataSet  = yesterdayostWriteGroup["OSTIosizeWriteDataSet"]
    ostIosizeWriteDataSet[:,:,0] = yesterdayostIosizeWriteDataSet[:,:,-1]
    yesterdayossCPUGroup = yesterdayFile["OSSCPUGroup"]
    yesterdayossCPUDataSet  = yesterdayossCPUGroup["OSSCPUDataSet"]
    ossCPUDataSet[:,0] = yesterdayossCPUDataSet[:,-1]
    yesterdayfsMissingGroup = yesterdayFile["FSMissingGroup"]
    yesterdayfsMissingDataSet = yesterdayfsMissingGroup["FSMissingDataSet"]
    fsMissingDataSet[0] = yesterdayfsMissingDataSet[-1]
    yesterdaymdsOpsGroup = yesterdayFile["MDSOpsGroup"]
    yesterdaymdsOpsDataSet = yesterdaymdsOpsGroup["MDSOpsDataSet"]
    mdsOpsDataSet[:,0] = yesterdaymdsOpsDataSet[:,-1]
    yesterdaymdsCPUGroup = yesterdayFile["MDSCPUGroup"]
    yesterdaymdsCPUDataSet = yesterdaymdsCPUGroup["MDSCPUDataSet"]
    mdsCPUDataSet[0] = yesterdaymdsCPUDataSet[-1]
    yesterdayFile.close()
    fsFile.close()
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    h5lmt_init.py <opts>
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
    args = process_args(main=True)
    if not args is None:
        fs, beginSie, endSie = doMain(args)
        if not ((fs is None) or (beginSie is None) or (endSie is None)):
            doAction(args, fs, beginSie, endSie)

