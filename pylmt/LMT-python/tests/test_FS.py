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

import argparse

from pyLMT import FS, LMTConfig, Timestamp
from pyLMT.tests import test_Bulk, test_MDS

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-C', '--cpu', action='store_true', default=False, help='Plot the CPU utilization')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-m', '--metadata', action='store_true', default=False, help='Do metadata rather than bulk I/O data')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-x', '--x_correlate', action='store_true', default=False, help='Scatter plot of CPU utilization vs. -o <oss>')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    args.spectrum = False
    args.plotSdevs = False
    args.mask = None
    args.show_ops = False
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         config - (file) The lmtrc config file telling how to get to the DB
         cpu - (boolean) Get CPU utilization data and potentially plot it
         end - (string) As above giving the end of the data to be gathered.
         FS - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         metadata - (boolean) Do metadata rather than bulk I/O data
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         report - (boolean) Print out summary info about the analyzed operations
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         x_correlate - (boolean) plot the ops versus the CPU utilization rather than
                         the ops versus time.
         ybound - (float) Use the given value as the maximum of the y-acis

    You can work with a data set interactively by entering ipython at the command line
    and carrying out this sequence of preparatory steps:
from pyLMT import FS
from pyLMT.tests import test_FS
args = FS.process_args()
args.begin = '2012-01-12'
args.end = '2012-01-13'
args.cpu = True
args.FS = 'filesystem_scratch'
fs = test_FS.do_main(args)

    It may take as much as half an hour to load up 24 hours worth of data,
    but once you have it in hand you can explore the data set interactively
    without having to go back and query the LMT DB over and over.
    """
    # do_main will:
    # - process_configuration
    # - get the OSSs and metadata ops
    # - Process timestamps
    # - get the bulk data
    # - get the metadata
    # - return the FS object
    fsrc = LMTConfig.process_configuration(args)
    fs = FS.FS(fsrc['name'])
    if args.verbose == True:
        fs.debug()
    fs.getInfo(fsrc['conn'])
    (begin_ts, end_ts) = Timestamp.process_timestamps(args, fsrc)
    fs.getData(begin_ts,
               end_ts)
    if (args.cpu == True) or (args.x_correlate == True):
        fs.getCPU()
    return(fs)

#*******************************************************************************
def do_action(args, fs):
    if args.metadata == False:
        args.read = False
        args.write = False
        args.both = False
        if (fs.Bulk == None) or (fs.Bulk.haveData == False):
            return
        test_Bulk.do_action(args, fs.Bulk)
    else:
        args.mds_op = ["all"]
        args.hilite_op = None
        if (fs.MDS == None) or (fs.MDS.haveData == False):
            return
        test_MDS.do_action(args, fs.MDS)

#*******************************************************************************

if __name__ == "__main__":
    """
    test_FS.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -C          Plot the CPU utilization
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -m          Show metadata rather than bulk I/O data
    -p <file>   File name of .png file for graph
    -r          Print a report of statistics
    -v          Print debug messages
    -V          Print the version and exit
    -x          Cross correlate with the CPU utilization info
    -y <ymax>   Maximum value of the y-axis

    This module supports pulling information on metadata and bulk I/O performance
    of a given file system from the LMT DB.
    """
    args = process_args(main=True)
    if not args is None:
        fs = do_main(args)
        if not fs is None:
            do_action(args, fs)

