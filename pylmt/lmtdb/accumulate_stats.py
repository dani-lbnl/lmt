#!/usr/bin/env python
# accumulate_stats.py
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
#   parse one *_stats.report file and add it to another. Create the
# target if it does not already exist.

import os
import sys
import argparse
import numpy as np
import datetime
import time
import readline

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
    parser.add_argument('-a', '--add', default=None, type=str, help='The target file to use')
    parser.add_argument('-f', '--file', default=None, type=str, help='The source file to use')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************

def validate_args(args):
    if args.file is None:
        print "Please provide a source file (-f)"
    return(args)

#*******************************************************************************

def parse_stats(stats_line):
    stats = {}
    stats_list = stats_line.split(',')
    for stat_item in stats_list:
        try:
            pair = stat_item.split('=')
        except:
            print "Failed to get key, value pair from %s" % stats_item
        if len(pair) != 2:
            print "didn't get key and value from %s" % stats_item
            return(None, None)
        key = pair[0].strip(" ")
        value = pair[1].strip(" ")
        if key == 'count':
            value = int(value)
        else:
            value = float(value)
        stats[key] = value
    return(stats)

#*******************************************************************************

def show_stats(stats):
    for key,val in stats.iteritems():
        print "stats[%s] = %s" % (key, str(stats[key]))

#*******************************************************************************

def parse_file(file):
    r_stats = None
    w_stats = None
    lines = file.read().split('\n')
    for line in lines:
        if len(line) == 0:
            continue
        line_els = line.split(':')
        if len(line_els) != 2:
            print "didn't get io and stats from %s" % line
            return(None, None)
        io = line_els[0]
        stats = line_els[1]
        if io == 'read':
            r_stats = parse_stats(stats)
        elif io == 'write':
            w_stats = parse_stats(stats)
        else:
            print "Failed to recognize %s" % io
    return(r_stats, w_stats)

#*******************************************************************************

def doMain(args):
    new = False
    r_stats = {}
    w_stats = {}
    try:
        source = open(args.file, 'r')
    except:
        print "failed to open %s" % args.file
        return(None, None)
    (r_file, w_file) = parse_file(source)
    source.close()
    if (r_file is None) or (w_file is None):
        print "Failed to get values from %s" % args.file
        return(None, None)
    if not os.access(args.add, os.R_OK):
        new = True
        r_stats['count'] = r_file['count']
        w_stats['count'] = w_file['count']
        r_stats['sum'] = r_file['sum']
        w_stats['sum'] = w_file['sum']
        r_stats['sumsq'] = r_file['sumsq']
        w_stats['sumsq'] = w_file['sumsq']
        r_stats['ave'] = r_file['ave']
        w_stats['ave'] = w_file['ave']
        r_stats['med'] = r_file['med']
        w_stats['med'] = w_file['med']
        r_stats['sdev'] = r_file['sdev']
        w_stats['sdev'] = w_file['sdev']
        r_stats['min'] = r_file['min']
        w_stats['min'] = w_file['min']
        r_stats['max'] = r_file['max']
        w_stats['max'] = w_file['max']
    else:
        try:
            target = open(args.add, 'r')
        except:
            print "Failed to open %s" % args.add
            return(None, None)
        (r_add, w_add) = parse_file(target)
        target.close()
        for field in ('count', 'sum', 'sumsq'):
            r_stats[field] = r_file[field] + r_add[field]
            w_stats[field] = w_file[field] + w_add[field]
        if (r_file['count'] <= 0) or (w_file['count'] <= 0):
            print "Problem with source file count"
            return(None, None)
        r_stats['ave'] = r_stats['sum']/r_stats['count']
        w_stats['ave'] = w_stats['sum']/w_stats['count']
        r_stats['med'] = (r_file['med']*r_file['count'] + r_add['med']*r_add['count'])/r_stats['count']
        w_stats['med'] = (w_file['med']*w_file['count'] + w_add['med']*w_add['count'])/w_stats['count']
        r_stats['sdev'] = np.sqrt((r_stats['sumsq']/r_stats['count']) - (r_stats['ave']*r_stats['ave']))
        w_stats['sdev'] = np.sqrt((w_stats['sumsq']/w_stats['count']) - (w_stats['ave']*w_stats['ave']))
        if r_file['min'] < r_add['min']:
            r_stats['min'] = r_file['min']
        else:
            r_stats['min'] = r_add['min']
        if w_file['min'] < w_add['min']:
            w_stats['min'] = w_file['min']
        else:
            w_stats['min'] = w_add['min']
        if r_file['max'] > r_add['max']:
            r_stats['max'] = r_file['max']
        else:
            r_stats['max'] = r_add['max']
        if w_file['max'] > w_add['max']:
            w_stats['max'] = w_file['max']
        else:
            w_stats['max'] = w_add['max']
    return(r_stats, w_stats)

#*******************************************************************************
# See also ratestatsfromh5lmt.py
def doAction(args, r_stats, w_stats):
    target = open(args.add, 'w')
    target.write(("read: count=%d, sum = %f, sumsq=%f, ave=%f, " +
                  "med=%f, sdev=%f, min=%f, max=%f\n") %
                 (r_stats['count'], r_stats['sum'], r_stats['sumsq'], r_stats['ave'],
                  r_stats['med'], r_stats['sdev'], r_stats['min'], r_stats['max']))
    target.write(("write: count=%d, sum = %f, sumsq=%f, ave=%f, " +
                  "med=%f, sdev=%f, min=%f, max=%f\n") %
                 (w_stats['count'], w_stats['sum'], w_stats['sumsq'], w_stats['ave'],
                  w_stats['med'], w_stats['sdev'], w_stats['min'], w_stats['max']))
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    accumulate_stats.py <opts>
    Options include:
    -d <day>    The date for the figure (yyyy-mm-dd)
    -f <file>   The daily.data file to use
    -p <plot>   the file to save the plot in
    -h          A help message
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        (r_stats, w_stats) = doMain(args)
        if not ((r_stats is None) or (w_stats is None)):
            doAction(args, r_stats, w_stats)
