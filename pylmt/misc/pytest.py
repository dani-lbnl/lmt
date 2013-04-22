#!/usr/bin/env python
# Copyright University of California, 2012
# author: Andrew Uselton, Lawrence Berekeley National Lab, acuselton@lbl.gov
#
# pytest.py
#   Figure out the environemt of the python in question. Especially,
# as when invoked without a login shell.

import sys
for path in sys.path:
    print path

from pyLMT import Timestamp

