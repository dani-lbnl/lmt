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
# create the LMT-python-<ver> distribution

VERSION = 0.2.0
PACKAGE = LMT-python-${VERSION}
BUILD = ${HOME}/src/python/build

egg: 
	rm -rf ${BUILD}/${PACKAGE}*
	mkdir -p ${BUILD}/${PACKAGE}/pyLMT
	mkdir -p ${BUILD}/${PACKAGE}/tests
	cp MANIFEST.in ${BUILD}/${PACKAGE}
	cp README ${BUILD}/${PACKAGE}
	cp site.cfg ${BUILD}/${PACKAGE}
	cp pyLMT/*.py ${BUILD}/${PACKAGE}/pyLMT
	cp tests/*.py ${BUILD}/${PACKAGE}/tests
	cp setup.py ${BUILD}/${PACKAGE}
	cp ez_setup.py ${BUILD}/${PACKAGE}
	cd  ${BUILD}/${PACKAGE}
	pushd ${BUILD}; tar cvfz ${PACKAGE}.tgz ${PACKAGE}; popd
