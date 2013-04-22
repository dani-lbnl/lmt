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

from pyLMT import BrwOSS, LMTConfig, Timestamp, Graph

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-o', '--oss', default=None, type=str, help='Name of the OSS to examine')
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
    if args.oss is None:
        print "test_BrwOSS: Please provide an OSS"
        return(None)
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
         oss - (string) The hostname of the OSS to be analyzed.
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
    try:
        cursor = fsrc['conn'].cursor(MySQLdb.cursors.DictCursor)
        query = "SELECT * FROM OSS_INFO"
        cursor.execute (query)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        cursor.close()
        print "test_BrwOSS: Error %d: %s" % (e.args[0], e.args[1])
        return
    BO = None
    for row in rows:
        if row["HOSTNAME"] == args.oss:
            BO = BrwOSS.BrwOSS(fsrc['name'], args.oss)
            break
    cursor.close()
    if BO is None:
        print "test_BrwOSS: %s not found" % args.oss
        return
    if args.verbose == True:
        BO.debug()
        BO.debug(module="BrwOST")
        #oss.debug(module="Timestamp")
    BO.getOSTs(fsrc['conn'])
    BO.getBrwStats(fsrc['conn'], args.stat)
    (begin_ts, end_ts) = Timestamp.process_timestamps(args, fsrc)
    BO.getData(begin_ts, end_ts, args.stat)
    if (BO.Bins is None) or (len(BO.Bins) == 0):
        print "test_BrwOSS: Warning - No HistBins objects from OSS %s" % oss.name
        return
    if (BO.Steps is None) or (BO.Steps.steps() == 0):
        print "test_BrwOSS: Warning - No steps from OSS %s" % oss.name
        return
    return(BO)

#*******************************************************************************

def do_action(args, BO):
    mode = None
    if (args.read == True) and (args.write == False):
        mode = 'read'
    if (args.read == False) and (args.write == True):
        mode = 'write'
    if args.report == True:
        for o in BO.OSTs:
            o.report(mode)
        BO.report(mode)
    if args.plot == "noplot":
        return
    for Bins in BO.Bins:
        PlotsArray = []
        for o in BO.OSTs:
            Plots = []
            if args.read == True:
                Plots.append({'ost':o.name,
                              'values': (o.Read[Bins.id].Values[:,-1] -
                                         o.Read[Bins.id].Values[:,0]),
                              'label':'read',
                              'color':'r'})
            if args.write == True:
                Plots.append({'ost':o.name,
                              'values': (o.Write[Bins.id].Values[:,-1] -
                                         o.Write[Bins.id].Values[:,0]),
                              'label':'write',
                              'color':'b'})
            PlotsArray.append(Plots)
        Graph.BrwOST_hist_array(Bins.name, Bins.Bins, Bins.units, PlotsArray, args.plot)

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
        BO = do_main(args)
        if not BO is None:
            do_action(args, BO)

