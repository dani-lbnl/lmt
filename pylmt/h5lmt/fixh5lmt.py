#!/usr/bin/env python
# fixh5lmt.py
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
#   Occasionally, the h5lmt file gets created with a short 'steps' array.
# This makes subsequent processing fail with size mismatch errors.
# This utilitiy creates a new h5lmt file and copies over what it can
# salvage from the broken one. It assumes the broken files are called
# wrongsize_scratch2?.h5lmt and are in the current working directory.

import os
import sys
import argparse
import numpy as np
import datetime
import time
import h5py

#*******************************************************************************
# Support for basic calling conventions
def process_args(main=False):
    """
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The name of the file system to fix')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if args.fs is None:
        return(None)
    return(args)

#*******************************************************************************

# callable main function for working interactively
def doMain(args):
    """
    """
    file = 'hopper_%s.h5lmt' % args.fs
    wrong = 'wrongsize_%s.h5lmt' % args.fs
    try:
        fsFile = h5py.File(file, 'w-')
    except:
        print "The file %s should not exist" % file
        return(None, None)
    try:
        wrongFile = h5py.File(wrong, 'r')
    except:
        print "The file %s should exist" % wrong
        return(None, None)
    return(fsFile, wrongFile)

#*******************************************************************************
def doAction(args, fsFile, wrongFile):
    """
    """
    wrongStepsGroup = wrongFile['FSStepsGroup']
    wrongStepsDataSet = wrongStepsGroup['FSStepsDataSet']
    wrongostReadGroup = wrongFile['OSTReadGroup']
    wrongostBulkReadDataSet = wrongostReadGroup['OSTBulkReadDataSet']
    wrongostIosizeReadDataSet = wrongostReadGroup['OSTIosizeReadDataSet']
    wrongostWriteGroup = wrongFile['OSTWriteGroup']
    wrongostBulkWriteDataSet = wrongostWriteGroup['OSTBulkWriteDataSet']
    wrongostIosizeWriteDataSet = wrongostWriteGroup['OSTIosizeWriteDataSet']
    wrongossCPUGroup = wrongFile['OSSCPUGroup']
    wrongossCPUDataSet = wrongossCPUGroup['OSSCPUDataSet']
    wrongMissingGroup = wrongFile['FSMissingGroup']
    wrongMissingDataSet = wrongMissingGroup['FSMissingDataSet']
    wrongmdsOpsGroup = wrongFile['MDSOpsGroup']
    wrongmdsOpsDataSet = wrongmdsOpsGroup['MDSOpsDataSet']
    wrongmdsCPUGroup = wrongFile['MDSCPUGroup']
    wrongmdsCPUDataSet = wrongmdsCPUGroup['MDSCPUDataSet']
    iosize_bins = np.array([4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576])
    num_bins = len(iosize_bins)
    num_steps = 24*60*12 + 1
    num_osts = 156
    num_osss = 26
    fsStepsGroup = fsFile.create_group("FSStepsGroup")
    dayStr = os.path.basename(os.path.abspath('.'))
    beginSie = int(time.mktime(time.strptime(dayStr, "%Y-%m-%d")))
    endSie = beginSie + 24*60*60
    Steps = np.array(np.arange(beginSie, endSie+5, 5), dtype=np.int32)
    fsStepsDataSet = fsStepsGroup.create_dataset("FSStepsDataSet", data=Steps, dtype=np.int32)
    fsStepsDataSet.attrs['day'] = wrongStepsDataSet.attrs['day']
    fsStepsDataSet.attrs['nextday'] = wrongStepsDataSet.attrs['nextday']
    fsStepsDataSet.attrs['host'] = wrongStepsDataSet.attrs['host']
    fsStepsDataSet.attrs['fs'] = wrongStepsDataSet.attrs['fs']
    ostReadGroup = fsFile.create_group("OSTReadGroup")
    ostBulkReadDataSet  = ostReadGroup.create_dataset("OSTBulkReadDataSet", shape=(num_osts, num_steps), data=wrongostBulkReadDataSet, dtype=np.float64)
    ostIosizeReadDataSet  = ostReadGroup.create_dataset("OSTIosizeReadDataSet", shape=(num_osts, num_bins, num_steps), data=wrongostIosizeReadDataSet, dtype=np.float64)
    ostIosizeReadDataSet.attrs['stat'] = "BRW_IOSIZE"
    ostIosizeReadDataSet.attrs['bins'] = iosize_bins
    ostWriteGroup = fsFile.create_group("OSTWriteGroup")
    ostBulkWriteDataSet  = ostWriteGroup.create_dataset("OSTBulkWriteDataSet", shape=(num_osts, num_steps), data=wrongostBulkWriteDataSet, dtype=np.float64)
    ostIosizeWriteDataSet  = ostWriteGroup.create_dataset("OSTIosizeWriteDataSet", shape=(num_osts, num_bins, num_steps), data=wrongostIosizeWriteDataSet, dtype=np.float64)
    ostIosizeWriteDataSet.attrs['stat'] = "BRW_IOSIZE"
    ostIosizeWriteDataSet.attrs['bins'] = iosize_bins
    ossCPUGroup = fsFile.create_group("OSSCPUGroup")
    ossCPUDataSet  = ossCPUGroup.create_dataset("OSSCPUDataSet", shape=(num_osss, num_steps), data=wrongossCPUDataSet, dtype=np.float64)
    fsMissingGroup = fsFile.create_group("FSMissingGroup")
    fsMissingDataSet = fsMissingGroup.create_dataset("FSMissingDataSet", shape=(num_osss, num_steps), data=wrongMissingDataSet, dtype=np.int32)
    oss_names = []
    ost_names = []
    for oss_name in wrongossCPUDataSet.attrs['OSSNames']:
        oss_names.append(oss_name)
    for ost_name in wrongostBulkReadDataSet.attrs['OSTNames']:
        ost_names.append(ost_name)
    ossCPUDataSet.attrs['OSSNames'] = oss_names
    ostBulkReadDataSet.attrs['OSTNames'] = ost_names
    ostBulkWriteDataSet.attrs['OSTNames'] = ost_names
    ostIosizeReadDataSet.attrs['OSTNames'] = ost_names
    ostIosizeWriteDataSet.attrs['OSTNames'] = ost_names
    (num_ops, count_steps) = np.shape(wrongmdsOpsDataSet)
    mdsOpsGroup = fsFile.create_group("MDSOpsGroup")
    mdsOpsDataSet = mdsOpsGroup.create_dataset("MDSOpsDataSet", shape=(num_ops, num_steps), data=wrongmdsOpsDataSet, dtype=np.float64)
    mdsCPUGroup = fsFile.create_group("MDSCPUGroup")
    mdsCPUDataSet = mdsCPUGroup.create_dataset("MDSCPUDataSet", data=wrongmdsCPUDataSet, dtype=np.float64)
    op_names = []
    for op_name in wrongmdsOpsDataSet.attrs['OpNames']:
        op_names.append(op_name)
    mdsOpsDataSet.attrs['OpNames'] = op_names
    fsFile.close()
    wrongFile.close()
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
        fsFile, wrongFile = doMain(args)
        if not ((fsFile is None) or (wrongFile is None)):
            doAction(args, fsFile, wrongFile)

