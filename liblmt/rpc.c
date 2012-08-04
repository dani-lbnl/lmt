/*****************************************************************************
 *  Copyright (C) 2012 University of California Regents
 *  This module written by Andrew Uselton <acuselton@lbl.gov>
 *  UCRL-CODE-xxxxxx
 *  All Rights Reserved.
 *
 *  This file is part of Lustre Monitoring Tool, version 2.
 *  Authors: H. Wartens, P. Spencer, N. O'Neill, J. Long, J. Garlick,
 *  A. Uselton
 *  For details, see http://github.com/chaos/lmt.
 *  Until the pull request is completed this module resides in the fork:
 *  http://github.com/uselton/lmt
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License (as published by the
 *  Free Software Foundation) version 2, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA or see
 *  <http://www.gnu.org/licenses/>.
 *****************************************************************************/

#if HAVE_CONFIG_H
#include "config.h"
#endif /* HAVE_CONFIG_H */

#include <stdio.h>
#include <stdlib.h>
#if STDC_HEADERS
#include <string.h>
#endif /* STDC_HEADERS */
#include <errno.h>
#include <sys/utsname.h>
#include <inttypes.h>
#include <math.h>

#include "list.h"
#include "hash.h"
#include "error.h"

#include "proc.h"
#include "stat.h"
#include "meminfo.h"
#include "lustre.h"

#include "lmt.h"
#include "rpc.h"
#include "util.h"
#include "lmtconf.h"


#define MAX_BRW_STAT_LEN 1500

static int
_parse_brw_stat(pctx_t ctx, char *name, brw_t t, char *s, int len)
{
  int ret = -1;
  histogram_t brw_stats_hist = NULL;
  char buf[MAX_BRW_STAT_LEN];
  
  if (proc_lustre_brwstats (ctx, name, t, &brw_stats_hist) < 0) {
    if (lmt_conf_get_proto_debug ())
      err ("error reading lustre brw_stats entry %d from %s proc", (int)t, name);
    goto done;
  }
  /*
   * At this point you want to construct a text representation of
   * the histogram. 
   */
  
  n = snprintf (s, len, "%s;", buf);
  if (n >= len) {
    if (lmt_conf_get_proto_debug ())
      msg ("string overflow");
    return -1;
    }
  if(brw_stats_hist != NULL)
    histogram_destory(brw_stats_hist);
  ret = 0;
 done:
  retrun ret;
}

static int
_get_rpcstring (pctx_t ctx, char *name, char *s, int len)
{
    char *uuid = NULL;
    int n, retval = -1;

    if (proc_lustre_uuid (ctx, name, &uuid) < 0) {
        if (lmt_conf_get_proto_debug ())
            err ("error reading lustre %s uuid from proc", name);
        goto done;
    }
    n = sprintf( s, len, "%s;", uuid );
    if (n >= len) {
        if (lmt_conf_get_proto_debug ())
            msg ("string overflow");
        return -1;
    }
    s += n; len -= n;
    if( (n = _parse_brw_stat(ctx, name, BRW_RPC, s, len)) < 0 )
      goto done;
    s += n; len -= n;
    if( (n = _parse_brw_stat(ctx, name, BRW_DISPAGES, s, len)) < 0 )
      goto done;
    s += n; len -= n;
    if( (n = _parse_brw_stat(ctx, name, BRW_DISBLOCKS, s, len)) < 0 )
      goto done;
    s += n; len -= n;
    if( (n = _parse_brw_stat(ctx, name, BRW_FRAG, s, len)) < 0 )
      goto done;
    s += n; len -= n;
    if( (n = _parse_brw_stat(ctx, name, BRW_FLIGHT, s, len)) < 0 )
      goto done;
    s += n; len -= n;
    if( (n = _parse_brw_stat(ctx, name, BRW_IOTIME, s, len)) < 0 )
      goto done;
    s += n; len -= n;
    if( (n = _parse_brw_stat(ctx, name, BRW_IOSIZE, s, len)) < 0 )
      goto done;
    retval = 0;
done:
    if (uuid)
        free (uuid);
    return retval;
}

int
lmt_rpc_string (pctx_t ctx, char *s, int len)
{
    ListIterator itr = NULL;
    List ostlist = NULL;
    struct utsname uts;
    int used, n, retval = -1;
    char *name;

    if (proc_lustre_ostlist (ctx, &ostlist) < 0)
        goto done;
    if (list_count (ostlist) == 0) {
        errno = 0;
        goto done;
    }
    if (uname (&uts) < 0) {
        err ("uname");
        goto done;
    }
    n = snprintf (s, len, "2;%s;",
                  uts.nodename);
    if (n >= len) {
        if (lmt_conf_get_proto_debug ())
            msg ("string overflow");
        goto done;
    }
    itr = list_iterator_create (ostlist);
    while ((name = list_next (itr))) {
        used = strlen (s);
        if (_get_rpcstring (ctx, name, s + used, len - used) < 0)
            goto done;
    }
    retval = 0;
done:
    if (itr)
        list_iterator_destroy (itr);
    if (ostlist)
        list_destroy (ostlist);
    return retval;
}


/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
