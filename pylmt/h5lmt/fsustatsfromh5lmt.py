#!/usr/bin/env python
# fsustatsfromh5lmt.py
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
#   Summarize the day's data rates so you can get an average, a standard
# deviation a median and a maximum (might as well toss in a minimum).
# Include the count, sum, and sumsq so you can aggregate multiple days.

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
    parser.add_argument('-f', '--file', default=None, type=str, help='The hdf5 file')
    parser.add_argument('-s', '--stat', default=None, type=str, help='Name of the BRW statistic to examine')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
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
    return(fsFile)

#*******************************************************************************

def do_action(args, fsFile):
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ostIosizeReadDataSet = ostReadGroup['OSTIosizeReadDataSet']
    if args.stat != ostIosizeReadDataSet.attrs['stat']:
        print "We should only be seeing BRW_IOSIZE statistics not %s" % ostIosizeReadDataSet.attrs['stat']
        return
    bins = ostIosizeReadDataSet.attrs['bins']
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostIosizeWriteDataSet = ostWriteGroup['OSTIosizeWriteDataSet']
    readHistpS  = None
    writeHistpS = None
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        readpS = ostIosizeReadDataSet[ost_index,:,:]
        if readHistpS is None:
            readHistpS = np.zeros_like(readpS)
        #print readpS
        readHistpS += readpS
        writepS = ostIosizeWriteDataSet[ost_index,:,:]
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
    count = len(yhat)
    sum   = np.sum(yhat)
    sumsq = np.sum(yhat*yhat)
    average = np.average(yhat)
    median = np.median(yhat)
    stddev = np.std(yhat)
    minimum = np.min(yhat)
    maximum = np.max(yhat)
    if sum <= 0.0:
        return
    print (("yhat: count=%d, sum = %f, sumsq=%f, ave=%f, " +
            "med=%f, sdev=%f, min=%f, max=%f") %
           (count, sum, sumsq, average,
            median, stddev, minimum, maximum))
    return


#*******************************************************************************

if __name__ == "__main__":
    """
    iosize_model.py <opts>
    Options include:
    -f <fs>     The h5lmt file to use
    -h          A help message
    -v          Print debug messages
    -V          Print the version and exit

    Generate summary statistics for a day's File System Utilization
    calculations.

    """
    args = process_args(main=True)
    if not args is None:
        fsFile = do_main(args)
        if not fsFile is None:
            do_action(args, fsFile)
            fsFile.close()
