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
import MySQLdb

from pyLMT import LMTConfig, Timestamp, TimeSteps, Graph, CPU

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-m', '--mds', action='store_true', default=False, help='Show the CPU utilization for the MDS')
    parser.add_argument('-o', '--oss', default=None, type=str, help='Show the CPU utilization for <oss>')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    '''
    We only want we Server at a time
    '''
    if (args.mds == True) and (args.oss != None):
        print "CPU.validate_args(): Warning - only showing MDS CPU utilization, not for %s" % args.oss
        args.oss = None
    if (args.mds == False) and (args.oss == None):
        args.mds = True
    return(args)

#*******************************************************************************
def do_main(args):
    '''
    '''
    fsrc = LMTConfig.process_configuration(args)
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    if args.mds == True:
        query = "SELECT TIMESTAMP_INFO.TS_ID,TIMESTAMP,PCT_CPU FROM TIMESTAMP_INFO,MDS_DATA WHERE "
        query += "TIMESTAMP_INFO.TS_ID=MDS_DATA.TS_ID AND "
    else:
        query = "SELECT TIMESTAMP_INFO.TS_ID,TIMESTAMP,PCT_CPU FROM TIMESTAMP_INFO,OSS_DATA,OSS_INFO WHERE "
        query += "OSS_DATA.OSS_ID=OSS_INFO.OSS_ID and OSS_INFO.HOSTNAME='"
        query += args.oss + "' AND "
        query += "TIMESTAMP_INFO.TS_ID=OSS_DATA.TS_ID AND "
    query += "TIMESTAMP_INFO.TIMESTAMP >= '"
    query += beginTimestamp.timestr
    query += "' AND TIMESTAMP_INFO.TIMESTAMP <= '"
    query += endTimestamp.timestr
    query += "'"
    try:
        cursor = fsrc['conn'].cursor(MySQLdb.cursors.DictCursor)
        if args.verbose == True:
            print "\t%s" % query
        cursor.execute (query)
    except MySQLdb.Error, e:
        cursor.close()
        print "CPU.get_ops_data_from_db: Error %d: %s\n%s" % (e.args[0], e.args[1], query)
        return(None)
    rows = cursor.fetchall()
    # N.B. The Python type returned by the MySQL "TIMESTAMP" field is
    # "datetime.datetime"
    Steps = TimeSteps.TimeSteps()
    for row in rows:
        Steps.examine(row['TIMESTAMP'], Timestamp.calc_sie(row['TIMESTAMP']), row['TS_ID'])
    Steps.register()
    if args.mds == True:
        cpu = CPU.CPU("mds")
    else:
        cpu = CPU.CPU(args.oss)
    cpu.setSteps(Steps)
    for row in rows:
        cpu.register(Steps.getSie(timestamp=row['TIMESTAMP']), float(row['PCT_CPU']))
    cursor.close()
    n = cpu.interpolate()
    if n == 0:
        print "CPU: Warning - No data"
        return(None)
    cpu.stats()

    return(cpu)

#*******************************************************************************
def do_action(args, cpu):
    if args.verbose == True:
        cpu.show()
    cpu.header()
    cpu.report()
    if args.plot == "noplot":
        return
    if cpu.Steps.steps() == 0:
        print "CPU.do_action(): Warning - No steps"
        return
    Graph.CPU_plot(cpu, args.plot)

#*******************************************************************************


if __name__ == "__main__":
    """
    test_CPU.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -m          Show the CPU utilization for the MDS
    -o <oss>    Show the CPU utilization for <oss>
    -p <file>   file name of .png file for graph
    -v          Print debug messages
    -V          Print the version and exit
    -y <ymax>   Maximum value of the y-axis

    Rudimentary test for CPU module.

    """
    args = process_args(main=True)
    if not args is None:
        cpu = do_main(args)
        if not cpu is None:
            do_action(args, cpu)
