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

from pyLMT import Operation, LMTConfig, Timestamp, TimeSteps, Graph

#*******************************************************************************
# Support for basic calling conventions
def process_args(main=False):
    """
    The command line arguments needed for operating the operation class as
    a simple script.
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-m', '--mds_op', default=None, type=str, help='Name of the operation to examine')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-s', '--show_ops', action='store_true', default=False, help='Print the list of ops in the DB')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (((args.begin == None) and (args.end != None)) or
        ((args.begin != None) and (args.end == None))):
        print "Operation.validate_args(): Please provide both a begin and an end argument (or neither for the default)"
        return(None)
    if (args.mds_op == None) and (args.show_ops == False):
        print "Operation.validate_args(): Ether you need to supply an MDS Operation or use the show_ops option"
        return(None)
    return(args)

#*******************************************************************************
# callable main function for working interactively
def do_main(args):
    """
    """
    fsrc = LMTConfig.process_configuration(args)
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    try:
        cursor = fsrc['conn'].cursor(MySQLdb.cursors.DictCursor)
        query = "SELECT * FROM OPERATION_INFO"
        cursor.execute (query)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        cursor.close()
        print "Operation: Error %d: %s" % (e.args[0], e.args[1])
        return(None)
    Ops = []
    OpsDict = {}
    for row in rows:
        OpsDict[row['OPERATION_NAME']] = len(Ops)
        Ops.append(Operation.Operation(name=row['OPERATION_NAME'],
                             units=row['UNITS']))
        if args.show_ops == True:
            print row['OPERATION_ID'],row['OPERATION_NAME'],row['UNITS']
    cursor.close()
    if args.show_ops == True:
        return(None)
    if not args.mds_op in OpsDict:
        print "Operation: No %s in Operations" % args.mds_op
        return(None)
    op = Ops[OpsDict[args.mds_op]]
    query = "SELECT * FROM OPERATION_INFO,TIMESTAMP_INFO,MDS_OPS_DATA WHERE "
    query += "OPERATION_INFO.OPERATION_ID=MDS_OPS_DATA.OPERATION_ID "
    query += "AND OPERATION_INFO.OPERATION_NAME='"
    query += args.mds_op + "' "
    query += "AND TIMESTAMP_INFO.TS_ID=MDS_OPS_DATA.TS_ID AND TIMESTAMP_INFO.TIMESTAMP >= '"
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
        print "Operation.get_ops_data_from_db: Error %d: %s\n%s" % (e.args[0], e.args[1], query)
        return(None)
    rows = cursor.fetchall()
    # N.B. The Python type returned by the MySQL "TIMESTAMP" field is
    # "datetime.datetime"
    Steps = TimeSteps.TimeSteps()
    if args.verbose == True:
        Steps.debug()
    for row in rows:
        Steps.examine(row['TIMESTAMP'], Timestamp.calc_sie(row['TIMESTAMP']), row['TS_ID'])
    Steps.register()
    op.setSteps(Steps)
    for row in rows:
        op.register(Steps.getSie(timestamp=row['TIMESTAMP']), float(row['SAMPLES']))
    cursor.close()
    n = op.interpolate()
    if n == 0:
        print "Operation: Warning - No data"
        return(None)
    op.stats()
    if args.verbose == True:
        op.show()
    return(op)

#*******************************************************************************
def do_action(args, op):
    op.report()
    if args.plot == "noplot":
        return
    Graph.Operation_plot(op, args.plot, args.ybound)

#*******************************************************************************


if __name__ == "__main__":
    """
    test_Operation.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -m <mds_op> The MDS operation to examine
    -p <file>   file name of .png file for graph
    -v          Print debug messages
    -V          Print the version and exit
    -y <ymax>   Maximum value of the y-axis

    This module supports pulling Operation data from the LMT DB. It has to do
    all the MDS.py style setup for the actual printing and/or display of the
    graph.

    2011-10-25
    - version 0.1

    """
    args = process_args(main=True)
    if not args is None:
        op = do_main(args)
        if not op is None:
            do_action(args, op)
