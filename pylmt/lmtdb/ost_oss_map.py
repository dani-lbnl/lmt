#!/usr/bin/env python
# ost_oss_map.py
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
#   count how many bytes were moved in the interval for the fs as a whole,
# for each OSS, and for each ost.

import os
import sys
import argparse
import MySQLdb
import numpy as np
import matplotlib.pyplot as plt
import time
import datetime

scratch2_ost2oss = {}
scratch2_ost2oss["scratch2-OST0000"] = "lustre2-oss001"
scratch2_ost2oss["scratch2-OST0001"] = "lustre2-oss002"
scratch2_ost2oss["scratch2-OST0002"] = "lustre2-oss003"
scratch2_ost2oss["scratch2-OST0003"] = "lustre2-oss004"
scratch2_ost2oss["scratch2-OST0004"] = "lustre2-oss005"
scratch2_ost2oss["scratch2-OST0005"] = "lustre2-oss006"
scratch2_ost2oss["scratch2-OST0006"] = "lustre2-oss007"
scratch2_ost2oss["scratch2-OST0007"] = "lustre2-oss008"
scratch2_ost2oss["scratch2-OST0008"] = "lustre2-oss009"
scratch2_ost2oss["scratch2-OST0009"] = "lustre2-oss010"
scratch2_ost2oss["scratch2-OST000a"] = "lustre2-oss011"
scratch2_ost2oss["scratch2-OST000b"] = "lustre2-oss012"
scratch2_ost2oss["scratch2-OST000c"] = "lustre2-oss013"
scratch2_ost2oss["scratch2-OST000d"] = "lustre2-oss014"
scratch2_ost2oss["scratch2-OST000e"] = "lustre2-oss015"
scratch2_ost2oss["scratch2-OST000f"] = "lustre2-oss016"
scratch2_ost2oss["scratch2-OST0010"] = "lustre2-oss017"
scratch2_ost2oss["scratch2-OST0011"] = "lustre2-oss018"
scratch2_ost2oss["scratch2-OST0012"] = "lustre2-oss019"
scratch2_ost2oss["scratch2-OST0013"] = "lustre2-oss020"
scratch2_ost2oss["scratch2-OST0014"] = "lustre2-oss021"
scratch2_ost2oss["scratch2-OST0015"] = "lustre2-oss022"
scratch2_ost2oss["scratch2-OST0016"] = "lustre2-oss023"
scratch2_ost2oss["scratch2-OST0017"] = "lustre2-oss024"
scratch2_ost2oss["scratch2-OST0018"] = "lustre2-oss025"
scratch2_ost2oss["scratch2-OST0019"] = "lustre2-oss026"
scratch2_ost2oss["scratch2-OST001a"] = "lustre2-oss001"
scratch2_ost2oss["scratch2-OST001b"] = "lustre2-oss002"
scratch2_ost2oss["scratch2-OST001c"] = "lustre2-oss003"
scratch2_ost2oss["scratch2-OST001d"] = "lustre2-oss004"
scratch2_ost2oss["scratch2-OST001e"] = "lustre2-oss005"
scratch2_ost2oss["scratch2-OST001f"] = "lustre2-oss006"
scratch2_ost2oss["scratch2-OST0020"] = "lustre2-oss007"
scratch2_ost2oss["scratch2-OST0021"] = "lustre2-oss008"
scratch2_ost2oss["scratch2-OST0022"] = "lustre2-oss009"
scratch2_ost2oss["scratch2-OST0023"] = "lustre2-oss010"
scratch2_ost2oss["scratch2-OST0024"] = "lustre2-oss011"
scratch2_ost2oss["scratch2-OST0025"] = "lustre2-oss012"
scratch2_ost2oss["scratch2-OST0026"] = "lustre2-oss013"
scratch2_ost2oss["scratch2-OST0027"] = "lustre2-oss014"
scratch2_ost2oss["scratch2-OST0028"] = "lustre2-oss015"
scratch2_ost2oss["scratch2-OST0029"] = "lustre2-oss016"
scratch2_ost2oss["scratch2-OST002a"] = "lustre2-oss017"
scratch2_ost2oss["scratch2-OST002b"] = "lustre2-oss018"
scratch2_ost2oss["scratch2-OST002c"] = "lustre2-oss019"
scratch2_ost2oss["scratch2-OST002d"] = "lustre2-oss020"
scratch2_ost2oss["scratch2-OST002e"] = "lustre2-oss021"
scratch2_ost2oss["scratch2-OST002f"] = "lustre2-oss022"
scratch2_ost2oss["scratch2-OST0030"] = "lustre2-oss023"
scratch2_ost2oss["scratch2-OST0031"] = "lustre2-oss024"
scratch2_ost2oss["scratch2-OST0032"] = "lustre2-oss025"
scratch2_ost2oss["scratch2-OST0033"] = "lustre2-oss026"
scratch2_ost2oss["scratch2-OST0034"] = "lustre2-oss001"
scratch2_ost2oss["scratch2-OST0035"] = "lustre2-oss002"
scratch2_ost2oss["scratch2-OST0036"] = "lustre2-oss003"
scratch2_ost2oss["scratch2-OST0037"] = "lustre2-oss004"
scratch2_ost2oss["scratch2-OST0038"] = "lustre2-oss005"
scratch2_ost2oss["scratch2-OST0039"] = "lustre2-oss006"
scratch2_ost2oss["scratch2-OST003a"] = "lustre2-oss007"
scratch2_ost2oss["scratch2-OST003b"] = "lustre2-oss008"
scratch2_ost2oss["scratch2-OST003c"] = "lustre2-oss009"
scratch2_ost2oss["scratch2-OST003d"] = "lustre2-oss010"
scratch2_ost2oss["scratch2-OST003e"] = "lustre2-oss011"
scratch2_ost2oss["scratch2-OST003f"] = "lustre2-oss012"
scratch2_ost2oss["scratch2-OST0040"] = "lustre2-oss013"
scratch2_ost2oss["scratch2-OST0041"] = "lustre2-oss014"
scratch2_ost2oss["scratch2-OST0042"] = "lustre2-oss015"
scratch2_ost2oss["scratch2-OST0043"] = "lustre2-oss016"
scratch2_ost2oss["scratch2-OST0044"] = "lustre2-oss017"
scratch2_ost2oss["scratch2-OST0045"] = "lustre2-oss018"
scratch2_ost2oss["scratch2-OST0046"] = "lustre2-oss019"
scratch2_ost2oss["scratch2-OST0047"] = "lustre2-oss020"
scratch2_ost2oss["scratch2-OST0048"] = "lustre2-oss021"
scratch2_ost2oss["scratch2-OST0049"] = "lustre2-oss022"
scratch2_ost2oss["scratch2-OST004a"] = "lustre2-oss023"
scratch2_ost2oss["scratch2-OST004b"] = "lustre2-oss024"
scratch2_ost2oss["scratch2-OST004c"] = "lustre2-oss025"
scratch2_ost2oss["scratch2-OST004d"] = "lustre2-oss026"
scratch2_ost2oss["scratch2-OST004e"] = "lustre2-oss001"
scratch2_ost2oss["scratch2-OST004f"] = "lustre2-oss002"
scratch2_ost2oss["scratch2-OST0050"] = "lustre2-oss003"
scratch2_ost2oss["scratch2-OST0051"] = "lustre2-oss004"
scratch2_ost2oss["scratch2-OST0052"] = "lustre2-oss005"
scratch2_ost2oss["scratch2-OST0053"] = "lustre2-oss006"
scratch2_ost2oss["scratch2-OST0054"] = "lustre2-oss007"
scratch2_ost2oss["scratch2-OST0055"] = "lustre2-oss008"
scratch2_ost2oss["scratch2-OST0056"] = "lustre2-oss009"
scratch2_ost2oss["scratch2-OST0057"] = "lustre2-oss010"
scratch2_ost2oss["scratch2-OST0058"] = "lustre2-oss011"
scratch2_ost2oss["scratch2-OST0059"] = "lustre2-oss012"
scratch2_ost2oss["scratch2-OST005a"] = "lustre2-oss013"
scratch2_ost2oss["scratch2-OST005b"] = "lustre2-oss014"
scratch2_ost2oss["scratch2-OST005c"] = "lustre2-oss015"
scratch2_ost2oss["scratch2-OST005d"] = "lustre2-oss016"
scratch2_ost2oss["scratch2-OST005e"] = "lustre2-oss017"
scratch2_ost2oss["scratch2-OST005f"] = "lustre2-oss018"
scratch2_ost2oss["scratch2-OST0060"] = "lustre2-oss019"
scratch2_ost2oss["scratch2-OST0061"] = "lustre2-oss020"
scratch2_ost2oss["scratch2-OST0062"] = "lustre2-oss021"
scratch2_ost2oss["scratch2-OST0063"] = "lustre2-oss022"
scratch2_ost2oss["scratch2-OST0064"] = "lustre2-oss023"
scratch2_ost2oss["scratch2-OST0065"] = "lustre2-oss024"
scratch2_ost2oss["scratch2-OST0066"] = "lustre2-oss025"
scratch2_ost2oss["scratch2-OST0067"] = "lustre2-oss026"
scratch2_ost2oss["scratch2-OST0068"] = "lustre2-oss001"
scratch2_ost2oss["scratch2-OST0069"] = "lustre2-oss002"
scratch2_ost2oss["scratch2-OST006a"] = "lustre2-oss003"
scratch2_ost2oss["scratch2-OST006b"] = "lustre2-oss004"
scratch2_ost2oss["scratch2-OST006c"] = "lustre2-oss005"
scratch2_ost2oss["scratch2-OST006d"] = "lustre2-oss006"
scratch2_ost2oss["scratch2-OST006e"] = "lustre2-oss007"
scratch2_ost2oss["scratch2-OST006f"] = "lustre2-oss008"
scratch2_ost2oss["scratch2-OST0070"] = "lustre2-oss009"
scratch2_ost2oss["scratch2-OST0071"] = "lustre2-oss010"
scratch2_ost2oss["scratch2-OST0072"] = "lustre2-oss011"
scratch2_ost2oss["scratch2-OST0073"] = "lustre2-oss012"
scratch2_ost2oss["scratch2-OST0074"] = "lustre2-oss013"
scratch2_ost2oss["scratch2-OST0075"] = "lustre2-oss014"
scratch2_ost2oss["scratch2-OST0076"] = "lustre2-oss015"
scratch2_ost2oss["scratch2-OST0077"] = "lustre2-oss016"
scratch2_ost2oss["scratch2-OST0078"] = "lustre2-oss017"
scratch2_ost2oss["scratch2-OST0079"] = "lustre2-oss018"
scratch2_ost2oss["scratch2-OST007a"] = "lustre2-oss019"
scratch2_ost2oss["scratch2-OST007b"] = "lustre2-oss020"
scratch2_ost2oss["scratch2-OST007c"] = "lustre2-oss021"
scratch2_ost2oss["scratch2-OST007d"] = "lustre2-oss022"
scratch2_ost2oss["scratch2-OST007e"] = "lustre2-oss023"
scratch2_ost2oss["scratch2-OST007f"] = "lustre2-oss024"
scratch2_ost2oss["scratch2-OST0080"] = "lustre2-oss025"
scratch2_ost2oss["scratch2-OST0081"] = "lustre2-oss026"
scratch2_ost2oss["scratch2-OST0082"] = "lustre2-oss001"
scratch2_ost2oss["scratch2-OST0083"] = "lustre2-oss002"
scratch2_ost2oss["scratch2-OST0084"] = "lustre2-oss003"
scratch2_ost2oss["scratch2-OST0085"] = "lustre2-oss004"
scratch2_ost2oss["scratch2-OST0086"] = "lustre2-oss005"
scratch2_ost2oss["scratch2-OST0087"] = "lustre2-oss006"
scratch2_ost2oss["scratch2-OST0088"] = "lustre2-oss007"
scratch2_ost2oss["scratch2-OST0089"] = "lustre2-oss008"
scratch2_ost2oss["scratch2-OST008a"] = "lustre2-oss009"
scratch2_ost2oss["scratch2-OST008b"] = "lustre2-oss010"
scratch2_ost2oss["scratch2-OST008c"] = "lustre2-oss011"
scratch2_ost2oss["scratch2-OST008d"] = "lustre2-oss012"
scratch2_ost2oss["scratch2-OST008e"] = "lustre2-oss013"
scratch2_ost2oss["scratch2-OST008f"] = "lustre2-oss014"
scratch2_ost2oss["scratch2-OST0090"] = "lustre2-oss015"
scratch2_ost2oss["scratch2-OST0091"] = "lustre2-oss016"
scratch2_ost2oss["scratch2-OST0092"] = "lustre2-oss017"
scratch2_ost2oss["scratch2-OST0093"] = "lustre2-oss018"
scratch2_ost2oss["scratch2-OST0094"] = "lustre2-oss019"
scratch2_ost2oss["scratch2-OST0095"] = "lustre2-oss020"
scratch2_ost2oss["scratch2-OST0096"] = "lustre2-oss021"
scratch2_ost2oss["scratch2-OST0097"] = "lustre2-oss022"
scratch2_ost2oss["scratch2-OST0098"] = "lustre2-oss023"
scratch2_ost2oss["scratch2-OST0099"] = "lustre2-oss024"
scratch2_ost2oss["scratch2-OST009a"] = "lustre2-oss025"
scratch2_ost2oss["scratch2-OST009b"] = "lustre2-oss026"

scratch2_oss2ost = {}
scratch2_oss2ost["lustre2-oss001"] = ["scratch2-OST0000", "scratch2-OST001a", "scratch2-OST0034", "scratch2-OST004e", "scratch2-OST0068", "scratch2-OST0082"]
scratch2_oss2ost["lustre2-oss002"] = ["scratch2-OST0001", "scratch2-OST001b", "scratch2-OST0035", "scratch2-OST004f", "scratch2-OST0069", "scratch2-OST0083"]
scratch2_oss2ost["lustre2-oss003"] = ["scratch2-OST0002", "scratch2-OST001c", "scratch2-OST0036", "scratch2-OST0050", "scratch2-OST006a", "scratch2-OST0084"]
scratch2_oss2ost["lustre2-oss004"] = ["scratch2-OST0003", "scratch2-OST001d", "scratch2-OST0037", "scratch2-OST0051", "scratch2-OST006b", "scratch2-OST0085"]
scratch2_oss2ost["lustre2-oss005"] = ["scratch2-OST0004", "scratch2-OST001e", "scratch2-OST0038", "scratch2-OST0052", "scratch2-OST006c", "scratch2-OST0086"]
scratch2_oss2ost["lustre2-oss006"] = ["scratch2-OST0005", "scratch2-OST001f", "scratch2-OST0039", "scratch2-OST0053", "scratch2-OST006d", "scratch2-OST0087"]
scratch2_oss2ost["lustre2-oss007"] = ["scratch2-OST0006", "scratch2-OST0020", "scratch2-OST003a", "scratch2-OST0054", "scratch2-OST006e", "scratch2-OST0088"]
scratch2_oss2ost["lustre2-oss008"] = ["scratch2-OST0007", "scratch2-OST0021", "scratch2-OST003b", "scratch2-OST0055", "scratch2-OST006f", "scratch2-OST0089"]
scratch2_oss2ost["lustre2-oss009"] = ["scratch2-OST0008", "scratch2-OST0022", "scratch2-OST003c", "scratch2-OST0056", "scratch2-OST0070", "scratch2-OST008a"]
scratch2_oss2ost["lustre2-oss010"] = ["scratch2-OST0009", "scratch2-OST0057", "scratch2-OST0023", "scratch2-OST003d", "scratch2-OST0071", "scratch2-OST008b"]
scratch2_oss2ost["lustre2-oss011"] = ["scratch2-OST000a", "scratch2-OST0024", "scratch2-OST003e", "scratch2-OST0058", "scratch2-OST0072", "scratch2-OST008c"]
scratch2_oss2ost["lustre2-oss012"] = ["scratch2-OST003f", "scratch2-OST0059", "scratch2-OST008d", "scratch2-OST000b", "scratch2-OST0025", "scratch2-OST0073"]
scratch2_oss2ost["lustre2-oss013"] = ["scratch2-OST000c", "scratch2-OST0026", "scratch2-OST0040", "scratch2-OST005a", "scratch2-OST0074", "scratch2-OST008e"]
scratch2_oss2ost["lustre2-oss014"] = ["scratch2-OST000d", "scratch2-OST0027", "scratch2-OST0041", "scratch2-OST005b", "scratch2-OST0075", "scratch2-OST008f"]
scratch2_oss2ost["lustre2-oss015"] = ["scratch2-OST000e", "scratch2-OST0028", "scratch2-OST0042", "scratch2-OST005c", "scratch2-OST0076", "scratch2-OST0090"]
scratch2_oss2ost["lustre2-oss016"] = ["scratch2-OST000f", "scratch2-OST0029", "scratch2-OST0043", "scratch2-OST005d", "scratch2-OST0077", "scratch2-OST0091"]
scratch2_oss2ost["lustre2-oss017"] = ["scratch2-OST0010", "scratch2-OST002a", "scratch2-OST0044", "scratch2-OST005e", "scratch2-OST0078", "scratch2-OST0092"]
scratch2_oss2ost["lustre2-oss018"] = ["scratch2-OST0011", "scratch2-OST002b", "scratch2-OST0045", "scratch2-OST005f", "scratch2-OST0079", "scratch2-OST0093"]
scratch2_oss2ost["lustre2-oss019"] = ["scratch2-OST0012", "scratch2-OST002c", "scratch2-OST0046", "scratch2-OST0060", "scratch2-OST007a", "scratch2-OST0094"]
scratch2_oss2ost["lustre2-oss020"] = ["scratch2-OST0047", "scratch2-OST0013", "scratch2-OST002d", "scratch2-OST0061", "scratch2-OST007b", "scratch2-OST0095"]
scratch2_oss2ost["lustre2-oss021"] = ["scratch2-OST0014", "scratch2-OST002e", "scratch2-OST0048", "scratch2-OST0062", "scratch2-OST007c", "scratch2-OST0096"]
scratch2_oss2ost["lustre2-oss022"] = ["scratch2-OST0015", "scratch2-OST002f", "scratch2-OST0049", "scratch2-OST0063", "scratch2-OST007d", "scratch2-OST0097"]
scratch2_oss2ost["lustre2-oss023"] = ["scratch2-OST0016", "scratch2-OST0030", "scratch2-OST004a", "scratch2-OST0064", "scratch2-OST007e", "scratch2-OST0098"]
scratch2_oss2ost["lustre2-oss024"] = ["scratch2-OST0017", "scratch2-OST0031", "scratch2-OST004b", "scratch2-OST0065", "scratch2-OST007f", "scratch2-OST0099"]
scratch2_oss2ost["lustre2-oss025"] = ["scratch2-OST0018", "scratch2-OST0032", "scratch2-OST004c", "scratch2-OST0066", "scratch2-OST0080", "scratch2-OST009a"]
scratch2_oss2ost["lustre2-oss026"] = ["scratch2-OST0019", "scratch2-OST0033", "scratch2-OST004d", "scratch2-OST0067", "scratch2-OST0081", "scratch2-OST009b"]
