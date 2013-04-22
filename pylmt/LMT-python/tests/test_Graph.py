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

import os
import argparse
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
if (not 'DISPLAY' in os.environ) or (os.environ['DISPLAY'] == None):
#    print "Bulk: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt

from pyLMT import Graph

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-n', '--num', default=100, type=int, help='How many values to graph')
    parser.add_argument('-p', '--plot', default=None, type=str, help='Name of file for plot')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if args.num <= 0:
        return(None)
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.num - the number of points
    """
    x = range(args.num)
    y = np.random.random(args.num)
    pair = (x, y)
    return(pair)

#*******************************************************************************
def do_action(args, pair):
    """
    I need to generate appropriate content here.
    """
    (x, y) = pair
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.scatter(ax, x, y, 'b', label="just a dumb scatter plot")
    plt.xlabel('x')
    plt.ylabel(r'y')
    plt.title('x versus y')
    if args.plot == None:
        plt.show()
    else:
        plt.savefig(args.plot)

#*******************************************************************************

if __name__ == "__main__":
    """
    test_Graph.py <opts>
    Options include:
    -h       A help message
    -n <num> The number of values to plot
    This module supports plotting graphs using numpy and matplotlib.

    Modest test module for Graph.py

    """
    args = process_args(main=True)
    if not args is None:
        pair = do_main(args)
        if not pair is None:
            do_action(args, pair)
