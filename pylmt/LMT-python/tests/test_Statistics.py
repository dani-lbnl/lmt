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
import numpy as np
from pyLMT import Statistics

#*******************************************************************************
def process_args(main=False):
    parser = argparse.ArgumentParser(description='Generic series of values')
    parser.add_argument('-l', '--length', default=1, type=int, help='The number of values to pu in the Statistics')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    '''
    The args.length value should be a positive integer.
    '''
    if (type(args.length) != int) or (args.length <= 0):
        print "test_Statistics.validate_args(): Error - The length needs to be a positive integer"
        return(None)
    return(args)

#*******************************************************************************
def do_main(args):
    '''
    '''
    data = np.random.random_sample(args.length)
    Stats = Statistics.Statistics(data)
    return(Stats)

#*******************************************************************************
def do_action(args, Stats):
    Stats.show()

#*******************************************************************************

if __name__ == "__main__":
    """
    Test_Statistics.py <opts>
    Options include:
    -h          A help message
    -l <length> The number of values over which to calculate Statistics
    -v          Print debug messages
    -V          Print the version and exit

    A rudimentary test for the Statistics class.
    array.

    """
    args = process_args(main=True)
    Stats = do_main(args)
    do_action(args, Stats)
