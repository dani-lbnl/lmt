#!/usr/bin/env python
# mdsaggregatecheck.py
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
#   Produce just the summary of Metadata Operations as is
# done in the dailyfromh5lmt.py.

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
    fsFile = h5py.File(args.file, 'w')
    return(fsFile)

#*******************************************************************************

def doMDSData(args, fsFile):
    if args.progress == True:
        print "MDS plot"
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
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
        mdsOpsDataSet[op_index,mdsOpsDataSet[op_index,:] > 1000000] = 0
        mds += mdsOpsDataSet[op_index,:]
        op_index += 1
    AggregateOps = np.sum(mds)
    interval = fsStepsDataSet[-1] - fsStepsDataSet[0]
    print "%d/%d" % (AggregateOps, interval)
    highVals, = np.where(mds > 100000)
    for index in  highVals:
        print mdsOpsDataSet[:,index]
    print fsStepsDataSet[mds > 1000000]
    #np.set_printoptions(threshold='nan')
    #print mdsOpsDataSet[0,:]
    return(AggregateOps)

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

def doAction(args, fsFile):
    AggregateOps = doMDSData(args, fsFile)
    #doDailyMDSSummary(args, fsFile, AggregateOps)

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
