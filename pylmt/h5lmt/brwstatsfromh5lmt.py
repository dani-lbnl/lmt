#!/usr/bin/env python
# brwstatsfromh5lmt.py
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

import os
import sys
import argparse
import MySQLdb
import numpy as np
import matplotlib.pyplot as plt
import datetime
import h5py

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

def doAction(args, fsFile):
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    try:
        ostIosizeReadDataSet = ostReadGroup['OSTIosizeReadDataSet']
    except:
        print "No OSTIosizeReadDataSet in OSTReadGroup"
        exit(1)
    bins = ostIosizeReadDataSet.attrs['bins']
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostIosizeWriteDataSet = ostWriteGroup['OSTIosizeWriteDataSet']
    readHistpS  = None
    writeHistpS = None
    ost_index = 0
    # This should probably be done with a sum accross the zeroth axis
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
    #print "shape of transpose(readHistpS) is %s" % str(np.shape(np.transpose(np.matrix(readHistpS))))
    #print "shape of transpose(bins) is %s" % str(np.shape(np.transpose(np.matrix(bins))))
    read = np.transpose(np.matrix(readHistpS))*np.transpose(np.matrix(bins))
    read = np.array(read)/(1024*1024)
    count = len(read)
    sum   = np.sum(read)
    sumsq = np.sum(read*read)
    average = np.average(read)
    median = np.median(read)
    stddev = np.std(read)
    minimum = np.min(read)
    maximum = np.max(read)
    if sum <= 0.0:
        return
    print (("read: count=%d, sum = %f, sumsq=%f, ave=%f, " +
            "med=%f, sdev=%f, min=%f, max=%f") %
           (count, sum, sumsq, average,
            median, stddev, minimum, maximum))
    write = np.transpose(np.matrix(writeHistpS))*np.transpose(np.matrix(bins))
    write = np.array(write)/(1024*1024)
    count = len(write)
    sum   = np.sum(write)
    sumsq = np.sum(write*write)
    average = np.average(write)
    median = np.median(write)
    stddev = np.std(write)
    minimum = np.min(write)
    maximum = np.max(write)
    print (("write: count=%d, sum = %f, sumsq=%f, ave=%f, " +
            "med=%f, sdev=%f, min=%f, max=%f") %
           (count, sum, sumsq, average,
            median, stddev, minimum, maximum))
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    brwstatsfromh5lmt.py <opts>
    Options include:
    -f <file>   The HDF5 file to use
    -h          A help message
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        fsFile = doMain(args)
        if not fsFile is None:
            doAction(args, fsFile)
            fsFile.close()
