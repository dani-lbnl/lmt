#!/usr/bin/env python
# dayh5lmtrate.py
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
    read = np.zeros(len(fsStepsDataSet))
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        read += ostBulkReadDataSet[ost_index,:]
        ost_index += 1
    write = np.zeros(len(fsStepsDataSet))
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        write += ostBulkWriteDataSet[ost_index,:]
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
    Graph.timeSeries(ax, fsStepsDataSet, read, 'r', label='read', Ave=True)
    Graph.timeSeries(ax, fsStepsDataSet, write, 'b', label='write', Ave=True)
    (handles, labels) = Graph.percent(ax, fsStepsDataSet, cpu, color='k', label='% CPU', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s aggregate I/O" % (fsStepsDataSet.attrs['day'],
                                       fsStepsDataSet.attrs['fs']))
    ax.set_ybound(lower = 0, upper = 50000)
    if (not handles is None) and (not labels is None):
        plt.legend(handles, labels)
    path  = os.path.dirname(args.file)
    if path == "":
        path = "."
    plt.savefig(path+'/'+host+'_'+fs+"_bulkRateCPU.png")
    plt.cla()
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    dayh5lmtrate.py <opts>
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
