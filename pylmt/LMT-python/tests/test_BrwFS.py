#!/bin/env python
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

import sys
import argparse
import MySQLdb
import numpy as np

from pyLMT import BrwFS, LMTConfig, Timestamp, Graph

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-s', '--stat', default=None, type=str, help='The name of one of the stats to show (default: show all)')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         config - (file) The lmtrc config file telling how to get to the DB
         end - (string) As above giving the end of the data to be gathered.
         fs - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         report - (boolean) Print out summary info about the analyzed operations
         read - Plot read data rate
         stat - Show histograms only for this statistic
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         write - Plot the write rate
    """
    # do_main will:
    # - process_configuration
    # - get the oss in question
    # - get the OSTs on it
    # - Process timestamps
    # - get the data
    # - return the oss
    fsrc = LMTConfig.process_configuration(args)
    brwfs = BrwFS.BrwFS(fsrc['dbname'])
    brwfs.getOSSs(fsrc['conn'])
    if args.verbose == True:
        brwfs.debug()
        brwfs.debug(module="BrwFS")
        #oss.debug(module="Timestamp")
    brwfs.getBrwStats(fsrc['conn'], args.stat)
    (begin_ts, end_ts) = Timestamp.process_timestamps(args, fsrc)
    brwfs.getData(begin_ts, end_ts, args.stat)
    if (brwfs.Bins is None) or (len(brwfs.Bins) == 0):
        print "test_BrwFS: Warning - No HistBins objects from OSS %s" % oss.name
        return
    if (brwfs.Steps is None) or (brwfs.Steps.steps() == 0):
        print "test_BrwFS: Warning - No steps from OSS %s" % oss.name
        return
    return(brwfs)

#*******************************************************************************

def roundNumber(x):
    negative = False
    if x < 0:
        negative = True
        x = -x
    i = 0
    if x > 1:
        while x > 10:
            x /= 10
            i += 1
        x = int(round(x + 1))
        while i > 0:
            x *= 10
            i -= 1
    else:
        while x < 1.0:
            x *= 10
            i += 1
        x = float(round(x + 1))
        while i > 0:
            x /= 10
            i -= 1
    if negative == True:
        x = -x
    return( x )

#*******************************************************************************

def do_action(args, brwfs):
    mode = None
    if (args.read == True) and (args.write == False):
        mode = 'read'
    if (args.read == False) and (args.write == True):
        mode = 'write'
    if args.report == True:
        for oss in brwfs.OSSs:
            for ost in oss.OSTs:
                ost.report(mode)
            oss.report(mode)
        brwfs.report(mode)
    if args.plot == "noplot":
        return
    for Bins in brwfs.Bins:
        yMax = 0
        PlotsArray2 = []
        for oss in brwfs.OSSs:
            PlotsArray = []
            for ost in oss.OSTs:
                Plots = []
                if args.read == True:
                    values = (ost.Read[Bins.id].Values[:,-1] -
                              ost.Read[Bins.id].Values[:,0])
                    max = np.max(values)
                    if yMax < max: yMax = max
                    Plots.append({'oss':oss.name,
                                  'ost':ost.name,
                                  'values': values,
                                  'label':'read',
                                  'color':'r'})
                if args.write == True:
                    values = (ost.Write[Bins.id].Values[:,-1] -
                              ost.Write[Bins.id].Values[:,0])
                    max = np.max(values)
                    if yMax < max: yMax = max
                    Plots.append({'oss':oss.name,
                                  'ost':ost.name,
                                  'values': values,
                                  'label':'write',
                                  'color':'b'})
                PlotsArray.append(Plots)
            PlotsArray2.append(PlotsArray)
        yMax = roundNumber(yMax)
        Graph.BrwOST_hist_array2(Bins.name, Bins.Bins, Bins.units, PlotsArray2, args.plot, yMax=yMax)

#*******************************************************************************

if __name__ == "__main__":
    """
    test_BrwOSS.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -o <oss>    The name of the OSS to examine
    -p <file>   File name of .png file for graph
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -s <stat>   Show histograms only for the <stat> statistics
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph

    A rudimentary test for the BrwOSS module.

    """
    args = process_args(main=True)
    if not args is None:
        brwfs = do_main(args)
        if not brwfs is None:
            do_action(args, brwfs)

