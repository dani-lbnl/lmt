#!/usr/bin/env python
# fsiosize.py
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
            print "fisiosize.py.doMain(): Failed to open %s" % args.file
            return(None)
        addFile = None
    else:
        try:
            addFile = h5py.File(args.add, 'r')
        except IOError:
            print "fisiosize.py.doMain(): Failed to open %s" % args.add
            return(None)
        fsFile = h5py.File(args.file, 'a')
    return(fsFile, addFile)

#*******************************************************************************

def doAction(args, fsFile, addFile):
    if not addFile is None:
        try:
            addIosizeGroup = addFile["FSIosizeGroup"]
        except KeyError:
            print "fsiosize.py.doAction(): no FSIosizeGroup in %s. Aborting" % args.add
            return
        try:
            addIosizeBinsDataSet = addIosizeGroup["FSIosizeBinsDataSet"]
        except KeyError:
            print "fsiosize.py.doAction(): no FSIosizeBinsDataSet in %s. Aborting" % args.add
            return
        try:
            addIosizeReadDataSet = addIosizeGroup["FSIosizeReadDataSet"]
        except KeyError:
            print "fsiosize.py.doAction(): no FSIosizeReadDataSet in %s. Aborting" % args.add
            return
        try:
            addIosizeWriteDataSet = addIosizeGroup["FSIosizeWriteDataSet"]
        except:
            print "fsiosize.py.doAction(): no FSIosizeWriteDataSet in %s. Aborting" % args.add
            return
        for attr in ('day', 'nextday', 'host', 'fs', 'stat', 'bins'):
            if not attr in addIosizeGroup.attrs:
                print "fsiosize.py.doAction(): The '%s' attribute is not in the %s fsIosizeGroup" % (attr, args.add)
                return
        try:
            fsIosizeGroup = fsFile["FSIosizeGroup"]
        except KeyError:
            fsIosizeGroup = fsFile.create_group("FSIosizeGroup")
        try:
            fsIosizeBinsDataSet = fsIosizeGroup["FSIosizeBinsDataSet"]
            for bin in fsIosizeBinsDataSet:
                if not bin in addIosizeBinsDataSet:
                    print "fsiosize.py.doAction(): %f in %s but not %s. Aborting" % (bin, args.file, args.add)
                    return
            for bin in addIosizeBinsDataSet:
                if not bin in fsIosizeBinsDataSet:
                    print "fsiosize.py.doAction(): %f in %s but not %s. Aborting" % (bin, args.add, args.file)
                    return
        except KeyError:
            fsIosizeBinsDataSet = fsIosizeGroup.create_dataset("FSIosizeBinsDataSet", data=addIosizeBinsDataSet)
        try:
            fsIosizeReadDataSet = fsIosizeGroup["FSIosizeReadDataSet"]
            fsIosizeReadDataSet[:] += addIosizeReadDataSet[:]
        except KeyError:
            fsIosizeReadDataSet = fsIosizeGroup.create_dataset("FSIosizeReadDataSet", data=addIosizeReadDataSet)
        try:
            fsIosizeWriteDataSet = fsIosizeGroup["FSIosizeWriteDataSet"]
            fsIosizeWriteDataSet[:] += addIosizeWriteDataSet[:]
        except:
            fsIosizeWriteDataSet = fsIosizeGroup.create_dataset("FSIosizeWriteDataSet", data=addIosizeWriteDataSet)
        if 'day' in fsIosizeGroup.attrs:
            if fsIosizeGroup.attrs['day'] > addIosizeGroup.attrs['day']:
                fsIosizeGroup.attrs['day'] = addIosizeGroup.attrs['day']
        else:
            fsIosizeGroup.attrs['day'] = addIosizeGroup.attrs['day']
        if 'nextday' in fsIosizeGroup.attrs:
            if fsIosizeGroup.attrs['nextday'] < addIosizeGroup.attrs['nextday']:
                fsIosizeGroup.attrs['nextday'] = addIosizeGroup.attrs['nextday']
        else:
            fsIosizeGroup.attrs['nextday'] = addIosizeGroup.attrs['nextday']
        for attr in ('host', 'fs', 'stat'):
            if (attr in fsIosizeGroup.attrs) and not (fsIosizeGroup.attrs[attr] == addIosizeGroup.attrs[attr]):
                print "fsiosize.py.doAction(): %s does not match between %s and %s. Aborting" % (attr, args.add, args.file)
                return
            else:
                fsIosizeGroup.attrs[attr] = addIosizeGroup.attrs[attr]
            if 'bins' in fsIosizeGroup.attrs:
                for bin in fsIosizeGroup.attrs['bins']:
                    if not bin in addIosizeGroup.attrs['bins']:
                        print "fsiosize.py.doAction(): bin %s in %s but not %s. Aborting" % (bin, args.file, args.add)
                        return
                for bin in addIosizeGroup.attrs['bins']:
                    if not bin in fsIosizeGroup.attrs['bins']:
                        print "fsiosize.py.doAction(): bin %s in %s but not %s. Aborting" % (bin, args.add, args.file)
                        return
            else:
                fsIosizeGroup.attrs['bins'] = addIosizeGroup.attrs['bins']
        # end of accumulating args.add into args.file
    else:
        try:
            fsIosizeGroup = fsFile["FSIosizeGroup"]
        except KeyError:
            print "fsiosize.py.doAction(): no FSIosizeGroup in %s. Aborting" % args.file
            return
        try:
            fsIosizeBinsDataSet = fsIosizeGroup["FSIosizeBinsDataSet"]
        except KeyError:
            print "fsiosize.py.doAction(): no FSIosizeBinsDataSet in %s. Aborting" % args.file
            return
        try:
            fsIosizeReadDataSet = fsIosizeGroup["FSIosizeReadDataSet"]
        except KeyError:
            print "fsiosize.py.doAction(): no FSIosizeReadDataSet in %s. Aborting" % args.file
            return
        try:
            fsIosizeWriteDataSet = fsIosizeGroup["FSIosizeWriteDataSet"]
        except:
            print "fsiosize.py.doAction(): no FSIosizeWriteDataSet in %s. Aborting" % args.file
            return
        for attr in ('day', 'nextday', 'host', 'fs', 'stat', 'bins'):
            if not attr in fsIosizeGroup.attrs:
                print "fsiosize.py.doAction(): The '%s' attribute is not in the %s fsIosizeGroup" % (attr, args.file)
                return
    if args.report == True:
        np.set_printoptions(threshold='nan')
        for attr in ('day', 'nextday', 'host', 'fs', 'stat', 'bins'):
            print "%s = %s" % (attr, fsIosizeGroup.attrs[attr])
        print 'bins:', fsIosizeBinsDataSet[:]
        print 'read:', fsIosizeReadDataSet[:]
        print 'write', fsIosizeWriteDataSet[:]
    if args.plot == "noplot":
        return
    bins = fsIosizeGroup.attrs['bins']
    host = fsIosizeGroup.attrs['host']
    fs = fsIosizeGroup.attrs['fs']
    day = fsIosizeGroup.attrs['day']
    nextday = fsIosizeGroup.attrs['nextday']
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    width = 0.35
    x = np.arange(len(bins))
    offset=0.0
    Graph.bar(ax, x, fsIosizeReadDataSet, width=width, offset=offset, color='r', label='read')
    offset += width
    Graph.bar(ax, x, fsIosizeWriteDataSet, width=width, offset=offset, color='b', label='write')
    offset += width
    ax.set_ylabel('Count')
    ax.set_xlabel('Bytes')
    ax.set_title('%s %s I/O sizes, %s to %s' % (host, fs, day, nextday) )
    ax.set_xticks(x+width)
    ax.set_xticklabels(bins, rotation=45, horizontalalignment='right' )
    ax.legend()
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( args.plot )
    plt.cla()
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
            if not args.add is None:
                addFile.close()
