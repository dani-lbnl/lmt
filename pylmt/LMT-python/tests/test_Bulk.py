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

from pyLMT import Bulk, LMTConfig, Timestamp, TimeSteps, Graph

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-B', '--both', action='store_true', default=False, help='Plot the sum of the read and write rates')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-C', '--cpu', action='store_true', default=False, help='Plot the CPU utilization')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-m', '--spectrum', action='store_true', default=False, help='Plot the cross-correlation spectrum')
    parser.add_argument('-M', '--mask', default=None, type=str, help='Filter out samples of the spectrum based on the mask. eg. "left=0.0,right=0.1" only shows the values with CPU utilization up  to 10%. "bottom=0.0,top=0.1" does the same for data rate')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-P', '--plotSdevs', action='store_true', default=False, help='Calculate and plot the standard deviation across OSTs at each timestep')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    parser.add_argument('-x', '--x_correlate', action='store_true', default=False, help='Scatter plot of CPU utilization vs. -o <oss>')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (args.both == True) and ((args.read == True) or (args.write == True)):
        print "Bulk: Try doing either -B (both) or one or both of -R (read) and -W (write)"
        return None
    if (args.both == False) and (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    if not args.mask is None:
        args.cpu = True
    if args.spectrum == True:
        args.x_correlate = True
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         both - Add together the read and wtites in the plot
         config - (file) The lmtrc config file telling how to get to the DB
         cpu - plot CPU utilization
         end - (string) As above giving the end of the data to be gathered.
         FS - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         mask - (key=value:keys in {mincpu, maxcpu, minval, maxval}) mask values outside the given range
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         plotSdevs (boolean) Calculate and plot the standard deviation across OSTs at each timestep
         report - (boolean) Print out summary info about the analyzed operations
         read - Plot read data rate
         spectrum - Plot the cross-correlation spectrum
         frac - Filter out samples of the spectrum below this fraction of the max
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         write - Plot the write rate
         x_correlate - (boolean) plot the ops versus the CPU utilization rather than
                         the ops versus time.
         ybound - (float) Use the given value as the maximum of the y-acis
    """
    # do_main will:
    # - process_configuration
    # - get all the OSSs which gets their OSTs as well
    # - Process timestamps
    # - get the data including CPU utilization if asked
    # - return the Bulk object
    fsrc = LMTConfig.process_configuration(args)
    bulk = Bulk.Bulk(fsrc['name'])
    if args.verbose == True:
        bulk.debug()
    bulk.getOSSs(fsrc['conn'])
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    bulk.setSteps(Steps)
    if (bulk.Steps is None) or (bulk.Steps.steps() == 0):
        print "Bulk: Warning - No steps from FS %s" % bulk.name
        return(None)
    bulk.getData()
    if (args.cpu == True) or (args.x_correlate == True):
        bulk.getCPU()
    return(bulk)

#*******************************************************************************

def do_action(args, bulk):
    if not args.mask is None:
        bulk.doMask(args.mask)
    if args.report == True:
        bulk.report()
    if args.plot == "noplot":
        return
    if args.both == True:
        mode = 'Both'
    elif args.read == args.write:
        mode = None
    elif args.read == True:
        mode = 'Read'
    else:
        mode = 'Write'
    if args.spectrum == True:
        Graph.bulk_composite_spectrum(bulk, mode, args.plot, args.ybound)
        return
    if args.x_correlate == True:
        if not args.mask is None:
            for oss in bulk.OSSs:
                oss.doMask(args.mask)
        Graph.bulk_composite_xcorr(bulk, mode=mode, plot=args.plot, ybound=args.ybound)
        return
    if args.plotSdevs == True:
        Graph.bulk_sdevs(bulk, mode, args.plot, ybound=args.ybound)
        return
    Graph.bulk_plot(bulk, mode=mode, plot=args.plot, ybound=args.ybound)

#*******************************************************************************

if __name__ == "__main__":
    """
    test_Bulk.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -B          Show the results of read and writes added together
    -c <conf>   Path to configuration file
    -C          Plot the CPU utilization
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -m          Plot the cross correlation spectrum
    -M <mask>   Filter out samples of the spectrum based on this mask
                <mask> is a string of key=values pairs with keys:
                left, right, top, bottom
    -p <file>   File name of .png file for graph
    -P          Calculate and plot the standard deviation across OSTs at each timestep
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph
    -x          Cross correlate with the CPU utilization info
    -y <ymax>   Maximum value of the y-axis

    Rudimentary test for the Bulk module.

    """
    args = process_args(main=True)
    if not args is None:
        bulk = do_main(args)
        if not bulk is None:
            do_action(args, bulk)

