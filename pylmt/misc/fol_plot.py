#!/usr/bin/env python
# fol_plot.py
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
#
#   Read in previously caluculated follows data and plot a graph.

import sys
import argparse
import os
import re
import Follows

parser = argparse.ArgumentParser(description='Access LMT data to do the follows calculation')
parser.add_argument('-m', '--month', default=None, type=str, help='Aggregate data for the days in the given month into the month\'s directory')
parser.add_argument('-n', '--num', default=10, type=int, help='How many ranges to break the follows calculation into')
parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
args = parser.parse_args()
for n in range(args.num):
    follows_file = args.month + ("/follows_%04d.data" % n)
    low  = n*2500/args.num
    high = (n+1)*2500/args.num
    F = Follows.Follows(low=low, high=high)
    F.load(follows_file)
    F.plot(args.month + ("/follows_%04d.png" % n) )
#    F.plot()

