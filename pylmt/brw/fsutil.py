#!/usr/bin/env python
# fsutil.py
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
#   Get the brw_stats IOSIZE histogram from the hdf5 file. Display or
# save the graph, and/or show the data.

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
    parser.add_argument('-a', '--add', default=None, type=str, help='An optional second hdf5 file to add to the --file')
    parser.add_argument('-f', '--file', default=None, type=str, help='The hdf5 file to graph, report on, or accumulate to')
    parser.add_argument('-p', '--plot', default=None, type=str, help='File to save plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze the IOSIZE stats distribution')
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
        return(None)
    return(args)

#*******************************************************************************

def doMain(args):
    if args.add is None:
        try:
            fsFile = h5py.File(args.file, 'r')
        except IOError:
            print "fsutil.py.doMain(): Failed to open %s" % args.file
            return(None)
        addFile = None
    else:
        try:
            addFile = h5py.File(args.add, 'r')
        except IOError:
            print "fsutil.py.doMain(): Failed to open %s" % args.add
            return(None)
        fsFile = h5py.File(args.file, 'a')
    return(fsFile, addFile)

#*******************************************************************************

# From http://stackoverflow.com/questions/10650089/matplotlib-draw-pie-chart-with-wedge-breakdown-into-barchart
def little_pie(breakdown,location,size):
    breakdown = [0] + list(np.cumsum(breakdown)* 1.0 / sum(breakdown))
    for i in xrange(len(breakdown)-1):
        x = [0] + np.cos(np.linspace(2 * np.pi * breakdown[i], 2 * np.pi *
                          breakdown[i+1], 20)).tolist()
        y = [0] + np.sin(np.linspace(2 * np.pi * breakdown[i], 2 * np.pi *
                          breakdown[i+1], 20)).tolist()
        xy = zip(x,y)
        plt.scatter( location[0], location[1], marker=(xy,0), s=size, facecolor=
               ['gold','yellow', 'yellow','orange', 'orange','red',
                'red','purple','indigo'][i%9])

#*******************************************************************************

# From http://stackoverflow.com/questions/10650089/matplotlib-draw-pie-chart-with-wedge-breakdown-into-barchart
def low_res_pie(breakdown,location,size):
    breakdown = [0] + list(np.cumsum(breakdown)* 1.0 / sum(breakdown))
    for i in xrange(len(breakdown)-1):
        x = [0] + np.cos(np.linspace(2 * np.pi * breakdown[i], 2 * np.pi *
                          breakdown[i+1], 20)).tolist()
        y = [0] + np.sin(np.linspace(2 * np.pi * breakdown[i], 2 * np.pi *
                          breakdown[i+1], 20)).tolist()
        xy = zip(x,y)
        plt.scatter( location[0], location[1], marker=(xy,0), s=size, facecolor=
               ['gold','indigo'][i%2])

#*******************************************************************************

def plotHist(args, group, utilBIns, utilHist):
    host = group.attrs['host']
    fs = group.attrs['fs']
    day = group.attrs['day']
    nextday = group.attrs['nextday']
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    ax.bar(utilBIns, utilHist, color='k')
    ax.set_ylabel('Count')
    ax.set_xlabel('FSU')
    ax.set_title('%s %s file system utilization\n%s to %s' % (host, fs, day, nextday) )
    ax.set_xbound(lower=utilBIns[0], upper=utilBIns[-1])
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( args.plot )

#*******************************************************************************

def plotPie(args, group, utilHIst, utilContrib, annotations=False):
    host = group.attrs['host']
    fs = group.attrs['fs']
    day = group.attrs['day']
    nextday = group.attrs['nextday']
    # This is the per-operation effect of Reads of a given I/O size
    R = np.identity(9)*[4, 8, 16, 32, 64, 128, 256, 512, 1024]*1024*4.39766334e-09 + np.identity(9)*1.62589065e-03
    # and Writes
    W = np.identity(9)*[4, 8, 16, 32, 64, 128, 256, 512, 1024]*1024*1.72900072e-09 + np.identity(9)*3.23092722e-03
    # and gathered together so they can act on utilContrib
    RW = np.transpose(np.matrix(np.hstack((R, W))))
    # This should end up with a shape of (9, 100) giving the relative composition
    # of each fsutil histogram bin of the nine I/O sizes.
    Cont = np.array(np.transpose(np.transpose(np.matrix(utilContrib))*RW))
    # Now in order to combine multiple histogram bins you have to normalize
    # by how many counts appeared in each bin.
    Norm = np.sum(Cont, axis=0)
    Normalizer, = np.where(Norm > 0.0)
    Cont[:, Normalizer] /= Norm[Normalizer]
    if annotations == True:
        # Just fake up a pie chart for the I/O sizes so we can get a legend
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.pie(Cont[:,0],
               labels=['4k', '8k', '16k', '32k', '64k', '128k', '256k', '512k', '1m'],
               colors=['gold','yellow', 'yellow','orange', 'orange','red',
                       'red','purple','indigo'])
        handles,labels = ax.get_legend_handles_labels()
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    x = np.empty((3, 9))
    x[0,:] = Cont[:, 0]*utilHIst[0]
    if annotations == True:
        little_pie(x[0,:], (1,1), 600)
        ax.text(1.15, 1.0, '< 2',
                horizontalalignment='left',
                verticalalignment='top')
    x[1,:] = np.sum(Cont[:, 1:15]*utilHIst[1:15], axis=1)
    if annotations == True:
        little_pie(x[1,:], (-1,-1), 600)
        ax.text(-1.15, -1.0, '< 30',
                horizontalalignment='right',
                verticalalignment='bottom')
    x[2,:] = np.sum(Cont[:, 15:]*utilHIst[15:], axis=1)
    if annotations == True:
        little_pie(x[2,:], (1,-1), 600)
        ax.text(1.15, -1.0, '> 30',
                horizontalalignment='left',
                verticalalignment='bottom')
    ax.pie(np.sum(x, axis=1),
           explode=[0.1, 0.1, 0.1],
           labels=['< 2','< 30','> 30'],
           autopct="%.2f%%",
           labeldistance=0.8)
    ax.set_title('%s %s\n%s to %s' % (host, fs, day, nextday) )
    if annotations == True:
        plt.legend(handles, labels, loc=(-0.15,0.5))
        ax.text(-1.16, -0.05, 'I/O size\ncontribution',
                horizontalalignment='right',
                verticalalignment='top')
    if args.plot is None:
        plt.show()
    else:
        if annotations == True:
            plt.savefig( 'pie_' + args.plot )
        else:
            plt.savefig( 'pienx_' + args.plot )
    plt.cla()

#*******************************************************************************

def plotPieLowRes(args, group, utilHist, utilContrib, annotations=False):
    host = group.attrs['host']
    fs = group.attrs['fs']
    day = group.attrs['day']
    nextday = group.attrs['nextday']
    # This is the per-operation effect of Reads of a given I/O size
    R = np.identity(9)*[4, 8, 16, 32, 64, 128, 256, 512, 1024]*1024*4.39766334e-09 + np.identity(9)*1.62589065e-03
    # and Writes
    W = np.identity(9)*[4, 8, 16, 32, 64, 128, 256, 512, 1024]*1024*1.72900072e-09 + np.identity(9)*3.23092722e-03
    # and gathered together so they can act on utilContrib
    RW = np.transpose(np.matrix(np.hstack((R, W))))
    # This should end up with a shape of (9, 100) giving the relative composition
    # of each fsutil histogram bin of the nine I/O sizes.
    Cont = np.array(np.transpose(np.transpose(np.matrix(utilContrib))*RW))
    # Now in order to combine multiple histogram bins you have to normalize
    # by how many counts appeared in each bin.
    Norm = np.sum(Cont, axis=0)
    Normalizer, = np.where(Norm > 0.0)
    Cont[:, Normalizer] /= Norm[Normalizer]
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    # Per I/O Size bin breakdown
    x = np.empty((3, 9))
    x[0,:] = Cont[:, 0]*utilHist[0]
    if annotations == True:
        # Group into good/bad where only 1M I/Os are good
        y = np.empty((3, 2))
        y[0,0] = np.sum(x[0,0:-1])
        y[0,1] = x[0,-1]
        low_res_pie(y[0,:], (1,1), 600)
        ax.text(1.15, 1.0, '< 2',
                horizontalalignment='left',
                verticalalignment='top')
    x[1,:] = np.sum(Cont[:, 1:15]*utilHist[1:15], axis=1)
    if annotations == True:
        y[1,0] = np.sum(x[1,0:-1])
        y[1,1] = x[1,-1]
        low_res_pie(y[1,:], (-1,-1), 600)
        ax.text(-1.15, -1.0, '< 30',
                horizontalalignment='right',
                verticalalignment='bottom')
    x[2,:] = np.sum(Cont[:, 15:]*utilHist[15:], axis=1)
    if annotations == True:
        y[2,0] = np.sum(x[2,0:-1])
        y[2,1] = x[2,-1]
        low_res_pie(y[2,:], (1,-1), 600)
        ax.text(1.15, -1.0, '> 30',
                horizontalalignment='left',
                verticalalignment='bottom')
    ax.pie(np.sum(x, axis=1),
           explode=[0.1, 0.1, 0.1],
           labels=['< 2','< 30','> 30'],
           autopct="%.2f%%",
           labeldistance=0.8)
    ax.set_title('%s %s\n%s to %s' % (host, fs, day, nextday) )
    if args.plot is None:
        plt.show()
    else:
        if annotations == True:
            plt.savefig( 'low_res_pie_' + args.plot )
        else:
            plt.savefig( 'low_res_pienx_' + args.plot )
    plt.cla()

#*******************************************************************************

def plotGoodBadPie(args, group, utilHist, utilContrib):
    host = group.attrs['host']
    fs = group.attrs['fs']
    day = group.attrs['day']
    nextday = group.attrs['nextday']
    # This is the per-operation effect of Reads of a given I/O size
    R = np.identity(9)*[4, 8, 16, 32, 64, 128, 256, 512, 1024]*1024*4.39766334e-09 + np.identity(9)*1.62589065e-03
    # and Writes
    W = np.identity(9)*[4, 8, 16, 32, 64, 128, 256, 512, 1024]*1024*1.72900072e-09 + np.identity(9)*3.23092722e-03
    # and gathered together so they can act on utilContrib
    RW = np.transpose(np.matrix(np.hstack((R, W))))
    # This should end up with a shape of (9, 100) giving the relative composition
    # of each fsutil histogram bin of the nine I/O sizes.
    Cont = np.array(np.transpose(np.transpose(np.matrix(utilContrib))*RW))
    # Now in order to combine multiple histogram bins you have to normalize
    # by how many counts appeared in each bin.
    Norm = np.sum(Cont, axis=0)
    Normalizer, = np.where(Norm > 0.0)
    Cont[:, Normalizer] /= Norm[Normalizer]
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    # Per I/O Size bin breakdown
    x = np.empty((3, 9))
    x[0,:] = Cont[:, 0]*utilHist[0]
    x[1,:] = np.sum(Cont[:, 1:15]*utilHist[1:15], axis=1)
    x[2,:] = np.sum(Cont[:, 15:]*utilHist[15:], axis=1)
    y = np.empty(2)
    y[0] = np.sum(np.sum(x[:,0:-1], axis=0))
    y[1] = np.sum(x[:,-1])
    ax.pie(y,
           explode=[0.0, 0.0],
           labels=['bad','good'],
           colors=['gold','indigo'],
           autopct="%.2f%%",
           labeldistance=0.8)
    ax.set_title('%s %s\n%s to %s' % (host, fs, day, nextday) )
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( 'good_bad_pie_' + args.plot )
    plt.cla()

#*******************************************************************************

def doAction(args, fsFile, addFile):
    if not addFile is None:
        try:
            addUtilGroup = addFile["FSUtilGroup"]
        except KeyError:
            print "fsutil.py.doAction(): no FSUtilGroup in %s. Aborting" % args.add
            return
        try:
            addUtilBinsDataSet = addUtilGroup["FSUtilBinsDataSet"]
        except KeyError:
            print "fsutil.py.doAction(): no FSUtilBinsDataSet in %s. Aborting" % args.add
            return
        try:
            addUtilHistDataSet = addUtilGroup["FSUtilHistDataSet"]
        except KeyError:
            print "fsutil.py.doAction(): no FSUtilHistDataSet in %s. Aborting" % args.add
            return
        for attr in ('day', 'nextday', 'host', 'fs'):
            if not attr in addUtilGroup.attrs:
                print "fsutil.py.doAction(): The '%s' attribute is not in the %s FSUtilGroup" % (attr, args.add)
                return
        try:
            addUtilContribDataSet = addUtilGroup["FSUtilContribDataSet"]
        except KeyError:
            print "fsutil.py.doAction(): no FSUtilContribDataSet in %s. Aborting" % args.add
            return
        try:
            fsUtilGroup = fsFile["FSUtilGroup"]
        except KeyError:
            fsUtilGroup = fsFile.create_group("FSUtilGroup")
        try:
            fsUtilBinsDataSet = fsUtilGroup["FSUtilBinsDataSet"]
            for bin in fsUtilBinsDataSet:
                if not bin in addUtilBinsDataSet:
                    print "fsutil.py.doAction(): %f in %s but not %s. Aborting" % (bin, args.file, args.add)
                    return
            for bin in addUtilBinsDataSet:
                if not bin in fsUtilBinsDataSet:
                    print "fsutil.py.doAction(): %f in %s but not %s. Aborting" % (bin, args.add, args.file)
                    return
        except KeyError:
            fsUtilBinsDataSet = fsUtilGroup.create_dataset("FSUtilBinsDataSet", data=addUtilBinsDataSet)
        try:
            fsUtilHistDataSet = fsUtilGroup["FSUtilHistDataSet"]
            fsUtilHistDataSet[:] += addUtilHistDataSet[:]
        except KeyError:
            fsUtilHistDataSet = fsUtilGroup.create_dataset("FSUtilHistDataSet", data=addUtilHistDataSet)
        if 'day' in fsUtilGroup.attrs:
            if fsUtilGroup.attrs['day'] > addUtilGroup.attrs['day']:
                fsUtilGroup.attrs['day'] = addUtilGroup.attrs['day']
        else:
            fsUtilGroup.attrs['day'] = addUtilGroup.attrs['day']
        if 'nextday' in fsUtilGroup.attrs:
            if fsUtilGroup.attrs['nextday'] < addUtilGroup.attrs['nextday']:
                fsUtilGroup.attrs['nextday'] = addUtilGroup.attrs['nextday']
        else:
            fsUtilGroup.attrs['nextday'] = addUtilGroup.attrs['nextday']
        for attr in ('host', 'fs'):
            if (attr in fsUtilGroup.attrs) and not (fsUtilGroup.attrs[attr] == addUtilGroup.attrs[attr]):
                print "fsutil.py.doAction(): %s does not match between %s and %s. Aborting" % (attr, args.add, args.file)
                return
            else:
                fsUtilGroup.attrs[attr] = addUtilGroup.attrs[attr]
        try:
            fsUtilContribDataSet = fsUtilGroup["FSUtilContribDataSet"]
            fsUtilContribDataSet[:,:] = addUtilContribDataSet[:,:]
        except:
            fsUtilContribDataSet = fsUtilGroup.create_dataset("FSUtilContribDataSet", data=addUtilContribDataSet)
        # end of accumulating args.add into args.file
    else:
        try:
            fsUtilGroup = fsFile["FSUtilGroup"]
        except KeyError:
            print "fsutil.py.doAction(): no FSUtilGroup in %s. Aborting" % args.file
            return
        try:
            fsUtilBinsDataSet = fsUtilGroup["FSUtilBinsDataSet"]
        except KeyError:
            print "fsutil.py.doAction(): no FSUtilBinsDataSet in %s. Aborting" % args.file
            return
        try:
            fsUtilHistDataSet = fsUtilGroup["FSUtilHistDataSet"]
        except KeyError:
            print "fsutil.py.doAction(): no FSUtilHistDataSet in %s. Aborting" % args.file
            return
        for attr in ('day', 'nextday', 'host', 'fs'):
            if not attr in fsUtilGroup.attrs:
                print "fsutil.py.doAction(): The '%s' attribute is not in the %s FSUtilGroup" % (attr, args.file)
                return
        try:
            # this has shape (18, 100): nine read I/O sizes then nine for writes
            # 100 histogram bins up to fsu = 200%
            fsUtilContribDataSet = fsUtilGroup["FSUtilContribDataSet"]
        except KeyError:
            print "fsutil.py.doAction(): no FSUtilContribDataSet in %s. Aborting" % args.file
            return
    if args.report == True:
        np.set_printoptions(threshold='nan')
        for attr in ('day', 'nextday', 'host', 'fs'):
            print "%s = %s" % (attr, fsUtilGroup.attrs[attr])
        print 'bins:', fsUtilBinsDataSet[:]
        print 'hist:', fsUtilHistDataSet[:]
    if args.plot == "noplot":
        return
    plotGoodBadPie(args, fsUtilGroup, fsUtilHistDataSet, fsUtilContribDataSet)
    plotHist(args, fsUtilGroup, fsUtilBinsDataSet, fsUtilHistDataSet)
    plotPie(args, fsUtilGroup, fsUtilHistDataSet, fsUtilContribDataSet, annotations=False)
    plotPie(args, fsUtilGroup, fsUtilHistDataSet, fsUtilContribDataSet, annotations=True)
    plotPieLowRes(args, fsUtilGroup, fsUtilHistDataSet, fsUtilContribDataSet, annotations=False)
    plotPieLowRes(args, fsUtilGroup, fsUtilHistDataSet, fsUtilContribDataSet, annotations=True)
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    daily.py <opts>
    Options include:
    -a <add>    An optional second hdf5 file to add to the --file
    -f <file>   The hdf5 file to graph, report on, or accumulate to
    -h          A help message
    -p <plot>   File to save plot in
    -r          Summariaze the IOSIZE stats distribution
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        fsFile, addFile = doMain(args)
        if not fsFile is None:
            doAction(args, fsFile, addFile)
            fsFile.close()
            if not addFile is None:
                addFile.close()
