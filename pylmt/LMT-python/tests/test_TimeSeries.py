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

from pyLMT import LMTConfig, Timestamp, TimeSteps, TimeSeries

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    '''
    Nothing to do
    '''
    return(args)

#*******************************************************************************
def do_main(args):
    '''
    '''
    fsrc = LMTConfig.process_configuration(args)
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    try:
        cursor = fsrc['conn'].cursor(MySQLdb.cursors.DictCursor)
        query = "select TS_ID,TIMESTAMP from TIMESTAMP_INFO where TIMESTAMP >= '"
        query += beginTimestamp.timestr
        query += "' and TIMESTAMP <= '"
        query += endTimestamp.timestr
        query += "'"
        cursor.execute (query)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        cursor.close()
        print "TimeSeries: Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
    Series = TimeSeries.TimeSeries("random", "none")
    Steps = TimeSteps.TimeSteps()
    for row in rows:
        Steps.examine(row['TIMESTAMP'], Timestamp.calc_sie(row['TIMESTAMP']), row['TS_ID'])
    cursor.close()
    Steps.register()
    Series.setSteps(Steps)
    count = Steps.steps()
    data = np.random.random_sample(count)
    counter = 0
    for i in range(count):
        try:
            sie = Steps.getSie(index=i)
        except:
            print "TimeSeries: Out of range index"
            sys.exit(1)
        counter += data[i]
        if data[i] > 0.97:
            counter = 0
        if data[i] > 0.03:
            Series.register(sie, counter)
        else:
            print "dropping datum %d" % i
    n = Series.interpolate()
    if n == 0:
        print "No data"
        return(None)
    Series.differential()
    Series.stats()
    return(Series)

#*******************************************************************************
def do_action(args, Series):
    Series.show()

#*******************************************************************************


if __name__ == "__main__":
    """
    test_TimeSeries.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -v          Print debug messages
    -V          Print the version and exit

    A rudimentary test of the TimeSeries module.

    """
    args = process_args(main=True)
    if not args is None:
        Series = do_main(args)
        if not Series is None:
            do_action(args, Series)
