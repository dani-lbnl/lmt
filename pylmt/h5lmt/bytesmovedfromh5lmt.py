#!/usr/bin/env python
# bytesmovedfromh5lmt.py
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
#   count how many bytes were moved in the interval for the fs as a whole,
# for each OSS, and for each ost.

import os
import sys
import argparse
import MySQLdb
import numpy as np
import matplotlib.pyplot as plt
import time
import datetime
import h5py

from pyLMT import Graph
from ost_map import scratch2_ost2oss, scratch2_oss2ost, scratch2_ost2raid, scratch2_raid2ost

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
    parser.add_argument('-c', '--complete', default=0.99, type=float, help='The fractional amount of I/O that marks nominal completion')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval hh:mm:ss (default - midnight at the end of the day)')
    parser.add_argument('-f', '--file', default=None, type=str, help='The hdf5 file (default: .')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************

def validate_args(args):
    if args.file is None:
        print "Please provide a file"
        return(None)
    if (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    return(args)

#*******************************************************************************

def doMain(args):
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

def doAction(args, b_sie, e_sie, fsFile):
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
    stepsDiff = np.zeros_like(fsStepsDataSet[b_index:e_index+1], dtype=np.int32)
    stepsDiff[1:] = np.diff(fsStepsDataSet[b_index:e_index+1])
    #print "data from index %d to %d" % (b_index, e_index)
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    duration = fsStepsDataSet[-1] - fsStepsDataSet[0]
    if args.read == True:
        ost_read = {}
        ost_read_indices = {}
        oss_read = {}
        oss_read_agg = {}
        raid_read = {}
        raid_read_agg = {}
        all_read = 0.0
        ost_read_ar = np.zeros(156, dtype=np.float64)
        ost_read_thresh = np.zeros(156, dtype=np.int32)
        oss_read_ar = np.zeros(26, dtype=np.float64)
        oss_read_thresh = np.zeros(26, dtype=np.float64)
        for oss_name, value in scratch2_oss2ost.iteritems():
            oss_read[oss_name] = 0.0
        raid_read_ar = np.zeros(26, dtype=np.float64)
        raid_read_thresh = np.zeros(26, dtype=np.float64)
        for oss_name, value in scratch2_oss2ost.iteritems():
            oss_read[oss_name] = 0.0
        for raid_name, value in scratch2_raid2ost.iteritems():
            raid_read[raid_name] = 0.0
        ostReadGroup = fsFile['OSTReadGroup']
        ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
        ost_index = 0
        ost_names = ostBulkReadDataSet.attrs['OSTNames']
        #ost_names.sort()
        for ost_name in ost_names:
            ost_read_indices[ost_name] = ost_index
            #print "OST %s is on OSS %s" % (ost_name, scratch2_ost2oss[ost_name])
            bytes = ostBulkReadDataSet[ost_index,b_index:e_index+1]*stepsDiff
            oss_name = scratch2_ost2oss[ost_name]
            if not oss_name in oss_read_agg:
                oss_read_agg[oss_name] = bytes
            else:
                oss_read_agg[oss_name] += bytes
            raid_name = scratch2_ost2raid[ost_name]
            if not raid_name in raid_read_agg:
                raid_read_agg[raid_name] = bytes
            else:
                raid_read_agg[raid_name] += bytes
            bytesCum = np.cumsum(bytes)
            ost_read[ost_name] = bytesCum[-1]/(1024.0*1024.0)
            ost_read_thresh[ost_index] = len(np.where(bytesCum < args.complete*bytesCum[-1])[0])
            #print "OST %s reached %f completion of reads at step %d" % (ost_name, args.complete, ost_read_thresh[ost_index])
            oss_read[oss_name] += ost_read[ost_name]
            raid_read[raid_name] += ost_read[ost_name]
            all_read += ost_read[ost_name]
            ost_index += 1
        oss_index = 0
        for oss_name, bytes in oss_read_agg.iteritems():
            bytesCum = np.cumsum(bytes)
            oss_read_thresh[oss_index] = len(np.where(bytesCum < args.complete*bytesCum[-1])[0])
            oss_index += 1
        raid_index = 0
        for raid_name, bytes in raid_read_agg.iteritems():
            bytesCum = np.cumsum(bytes)
            raid_read_thresh[raid_index] = len(np.where(bytesCum < args.complete*bytesCum[-1])[0])
            raid_index += 1
    if args.write == True:
        ost_write = {}
        ost_write_indices = {}
        oss_write = {}
        oss_write_agg = {}
        raid_write = {}
        raid_write_agg = {}
        all_write = 0.0
        ost_write_ar = np.zeros(156, dtype=np.float64)
        ost_write_thresh = np.zeros(156, dtype=np.int32)
        oss_write_ar = np.zeros(26, dtype=np.float64)
        oss_write_thresh = np.zeros(26, dtype=np.float64)
        for oss_name, value in scratch2_oss2ost.iteritems():
            oss_write[oss_name] = 0.0
        raid_write_ar = np.zeros(26, dtype=np.float64)
        raid_write_thresh = np.zeros(26, dtype=np.float64)
        for raid_name, value in scratch2_raid2ost.iteritems():
            raid_write[raid_name] = 0.0
        ostWriteGroup = fsFile['OSTWriteGroup']
        ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
        ost_index = 0
        ost_names = ostBulkWriteDataSet.attrs['OSTNames']
        #ost_names.sort()
        for ost_name in ost_names:
            ost_write_indices[ost_name] = ost_index
            #print "OST %s is on OSS %s" % (ost_name, scratch2_ost2oss[ost_name])
            bytes = ostBulkWriteDataSet[ost_index,b_index:e_index+1]*stepsDiff
            oss_name = scratch2_ost2oss[ost_name]
            if not oss_name in oss_write_agg:
                oss_write_agg[oss_name] = bytes
            else:
                oss_write_agg[oss_name] += bytes
            raid_name = scratch2_ost2raid[ost_name]
            if not raid_name in raid_write_agg:
                raid_write_agg[raid_name] = bytes
            else:
                raid_write_agg[raid_name] += bytes
            bytesCum = np.cumsum(bytes)
            ost_write[ost_name] = bytesCum[-1]/(1024.0*1024.0)
            ost_write_thresh[ost_index] = len(np.where(bytesCum < args.complete*bytesCum[-1])[0])
            #print "OST %s reached %f completion of writes at step %d" % (ost_name, args.complete, ost_write_thresh[ost_index])
            oss_write[scratch2_ost2oss[ost_name]] += ost_write[ost_name]
            raid_write[scratch2_ost2raid[ost_name]] += ost_write[ost_name]
            all_write += ost_write[ost_name]
            ost_index += 1
        oss_index = 0
        for oss_name, bytes in oss_write_agg.iteritems():
            bytesCum = np.cumsum(bytes)
            oss_write_thresh[oss_index] = len(np.where(bytesCum < args.complete*bytesCum[-1])[0])
            oss_index += 1
        raid_index = 0
        for raid_name, bytes in raid_write_agg.iteritems():
            bytesCum = np.cumsum(bytes)
            raid_write_thresh[raid_index] = len(np.where(bytesCum < args.complete*bytesCum[-1])[0])
            raid_index += 1

    ossCPUGroup = fsFile['OSSCPUGroup']
    ossCPUDataSet = ossCPUGroup['OSSCPUDataSet']
    oss_index = 0
    if args.read == True:
        print "read: %f" % all_read
    if args.write == True:
        print "write: %f" % all_write
    for oss_name in ossCPUDataSet.attrs['OSSNames']:
        if args.read == True:
            print "OSS %s read: %f TiB, %d sec" % (oss_name, oss_read[oss_name], oss_read_thresh[oss_index]*5)
        if args.write == True:
            print "OSS %s write: %f TiB, %d sec" % (oss_name, oss_write[oss_name], oss_write_thresh[oss_index]*5)
        for ost_name in scratch2_oss2ost[oss_name]:
            if args.read == True:
                ost_index = ost_read_indices[ost_name]
                print "\tOST %s read: %f GiB, %d sec" % (ost_name, ost_read[ost_name], ost_read_thresh[ost_index]*5)
                ost_read_ar[ost_index] = ost_read[ost_name]/(1024.0)
            if args.write == True:
                ost_index = ost_write_indices[ost_name]
                print "\tOST %s write: %f GiB, %d sec" % (ost_name, ost_write[ost_name], ost_write_thresh[ost_index]*5)
                ost_write_ar[ost_index] = ost_write[ost_name]/(1024.0)
        if args.read == True:
            oss_read_ar[oss_index] = oss_read[oss_name]/(1024.0*1024.0)
        if args.write == True:
            oss_write_ar[oss_index] = oss_write[oss_name]/(1024.0*1024.0)
        oss_index += 1
    raid_index = 0
    for raid_name, value in scratch2_raid2ost.iteritems():
        if args.read == True:
            print "RAID %s read: %f TiB, %d sec" % (raid_name, raid_read[raid_name], raid_read_thresh[raid_index]*5)
        if args.write == True:
            print "RAID %s write: %f TiB, %d sec" % (raid_name, raid_write[raid_name], raid_write_thresh[raid_index]*5)
        for ost_name in scratch2_raid2ost[raid_name]:
            if args.read == True:
                ost_index = ost_read_indices[ost_name]
                print "\tOST %s read: %f GiB, %d sec" % (ost_name, ost_read[ost_name], ost_read_thresh[ost_index]*5)
                ost_read_ar[ost_index] = ost_read[ost_name]/(1024.0)
            if args.write == True:
                ost_index = ost_write_indices[ost_name]
                print "\tOST %s write: %f GiB, %d sec" % (ost_name, ost_write[ost_name], ost_write_thresh[ost_index]*5)
                ost_write_ar[ost_index] = ost_write[ost_name]/(1024.0)
        if args.read == True:
            raid_read_ar[raid_index] = raid_read[raid_name]/(1024.0*1024.0)
        if args.write == True:
            raid_write_ar[raid_index] = raid_write[raid_name]/(1024.0*1024.0)
        raid_index += 1
    if args.read == True:
        ost_min_read_gb = np.min(ost_read_ar)
        ost_max_read_gb = np.max(ost_read_ar)
        oss_min_read_tb = np.min(oss_read_ar)
        oss_max_read_tb = np.max(oss_read_ar)
        ost_max_gb = ost_max_read_gb
        oss_max_tb = oss_max_read_tb
        raid_min_read_tb = np.min(raid_read_ar)
        raid_max_read_tb = np.max(raid_read_ar)
        raid_max_tb = raid_max_read_tb
    if args.write == True:
        ost_min_write_gb = np.min(ost_write_ar)
        ost_max_write_gb = np.max(ost_write_ar)
        oss_min_write_tb = np.min(oss_write_ar)
        oss_max_write_tb = np.max(oss_write_ar)
        if args.read == True:
            if ost_max_gb < ost_max_write_gb:
                ost_max_gb = ost_max_write_gb
            if oss_max_tb < oss_max_write_tb:
                oss_max_tb = oss_max_write_tb
        else:
            ost_max_gb = ost_max_write_gb
            oss_max_tb = oss_max_write_tb
        raid_min_write_tb = np.min(raid_write_ar)
        raid_max_write_tb = np.max(raid_write_ar)
        if args.read == True:
            if raid_max_tb < raid_max_write_tb:
                raid_max_tb = raid_max_write_tb
        else:
            raid_max_tb = raid_max_write_tb
    #print "oss max tb = %f" % oss_max_tb
    if args.read == True:
        print "min, max OST GB read %f, %f" % (ost_min_read_gb, ost_max_read_gb)
    if args.write == True:
        print "min, max OST GB writen %f, %f" % (ost_min_write_gb, ost_max_write_gb)
    if args.read == True:
        print "min, max OSS TB read %f, %f" % (oss_min_read_tb, oss_max_read_tb)
    if args.write == True:
        print "min, max OSS TB writen %f, %f" % (oss_min_write_tb, oss_max_write_tb)
    if args.read == True:
        print "min, max RAID TB read %f, %f" % (raid_min_read_tb, raid_max_read_tb)
    if args.write == True:
        print "min, max RAID TB writen %f, %f" % (raid_min_write_tb, raid_max_write_tb)
    if args.read == True:
        ost_read_hist,ost_read_bins = np.histogram(ost_read_ar, bins=20, range=(0.0, ost_max_gb))
        oss_read_hist,oss_read_bins = np.histogram(oss_read_ar, bins=20, range=(0.0, oss_max_tb))
        raid_read_hist,raid_read_bins = np.histogram(raid_read_ar, bins=20, range=(0.0, raid_max_tb))
    if args.write == True:
        ost_write_hist,ost_write_bins = np.histogram(ost_write_ar, bins=20, range=(0.0, ost_max_gb))
        oss_write_hist,oss_write_bins = np.histogram(oss_write_ar, bins=20, range=(0.0, oss_max_tb))
        raid_write_hist,raid_write_bins = np.histogram(raid_write_ar, bins=20, range=(0.0, raid_max_tb))
    if args.plot == "noplot":
        return
    fig = plt.figure(1)
    ax1 = fig.add_subplot(111)
    if args.read == True:
        width=0.35*ost_max_gb/len(ost_read_bins[1:])
    else:
        width=0.35*ost_max_gb/len(ost_write_bins[1:])
    if args.read == True:
        ax1.bar(ost_read_bins[1:], ost_read_hist, width=width, color='r', label='read')
    if args.write == True:
        ax1.bar(ost_write_bins[1:] + width, ost_write_hist, width=width, color='b', label='write')
    plt.xlabel('GB')
    plt.ylabel('count')
    plt.title('Distribution of OST GB moved')
    ax1.set_xbound(lower=0, upper=ost_max_gb)
    if args.plot is None:
        plt.show(1)
    else:
        plt.savefig("ost_" + args.plot)
    fig = plt.figure(2)
    ax2 = fig.add_subplot(111)
    if args.read == True:
        ax2.scatter(ost_read_ar, ost_read_thresh*5, color='r', label='read')
    if args.write == True:
        ax2.scatter(ost_write_ar, ost_write_thresh*5, color='b', label='write')
    plt.xlabel('GB')
    plt.ylabel('sec')
    plt.title('OST data moved versus time to %4.2f completion' % args.complete)
    ax2.set_xbound(lower=0, upper=ost_max_gb)
    if args.plot is None:
        plt.show(2)
    else:
        plt.savefig("ost_moved_vs_completion_" + args.plot)
    fig = plt.figure(3)
    ax3 = fig.add_subplot(111)
    if args.read == True:
        width=0.35*oss_max_tb/len(oss_read_bins[1:])
    else:
        width=0.35*oss_max_tb/len(oss_write_bins[1:])
    if args.read == True:
        ax3.bar(oss_read_bins[1:], oss_read_hist, width=width, color='r', label='read')
    if args.write == True:
        ax3.bar(oss_write_bins[1:] + width, oss_write_hist, width=width, color='b', label='write')
    plt.xlabel('TB')
    plt.ylabel('count')
    plt.title('Distribution of OSS TB moved')
    ax3.set_xbound(lower=0, upper=oss_max_tb)
    if args.plot is None:
        plt.show(3)
    else:
        plt.savefig("oss_" + args.plot)
    fig = plt.figure(4)
    ax4 = fig.add_subplot(111)
    if args.read == True:
        ax4.scatter(oss_read_ar, oss_read_thresh*5, color='r', label='read')
    if args.write == True:
        ax4.scatter(oss_write_ar, oss_write_thresh*5, color='b', label='write')
    plt.xlabel('TB')
    plt.ylabel('sec')
    plt.title('OSS data moved versus time to %4.2f completion' % args.complete)
    ax4.set_xbound(lower=0, upper=oss_max_tb)
    if args.plot is None:
        plt.show(4)
    else:
        plt.savefig("oss_moved_vs_completion_" + args.plot)
    fig = plt.figure(5)
    ax5 = fig.add_subplot(111)
    if args.read == True:
        width=0.35*raid_max_tb/len(raid_read_bins[1:])
    else:
        width=0.35*raid_max_tb/len(raid_write_bins[1:])
    if args.read == True:
        ax5.bar(raid_read_bins[1:], raid_read_hist, width=width, color='r', label='read')
    if args.write == True:
        ax5.bar(raid_write_bins[1:] + width, raid_write_hist, width=width, color='b', label='write')
    plt.xlabel('TB')
    plt.ylabel('count')
    plt.title('Distribution of RAID TB moved')
    ax5.set_xbound(lower=0, upper=raid_max_tb)
    if args.plot is None:
        plt.show(5)
    else:
        plt.savefig("raid_" + args.plot)
    fig = plt.figure(6)
    ax6 = fig.add_subplot(111)
    if args.read == True:
        ax6.scatter(raid_read_ar, raid_read_thresh*5, color='r', label='read')
    if args.write == True:
        ax6.scatter(raid_write_ar, raid_write_thresh*5, color='b', label='write')
    plt.xlabel('TB')
    plt.ylabel('sec')
    plt.title('RAID data moved versus time to %4.2f completion' % args.complete)
    ax6.set_xbound(lower=0, upper=raid_max_tb)
    if args.plot is None:
        plt.show(6)
    else:
        plt.savefig("raid_moved_vs_completion_" + args.plot)
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
