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

/* I don't actually use these, but I keep them here for reference */
char *brw_stats_keys[8] = {
  "pages per bulk r/w", /* BRW_RPC */
  "discontiguous pages", /* BRW_DISPAGES */
  "discontiguous blocks", /* BRW_DISBLOCKS */
  "disk fragmented I/Os", /* BRW_FRAG */
  "disk I/Os in flight", /* BRW_FLIGHT */
  "I/O time (1/1000s)", /* BRW_IOTIME */
  "disk I/O size", /* BRW_IOSIZE */
  NULL
};

/* I use these shorter strings, since they have no embedded blanks */
/* They correspond directly to the brw_t enum */
char *brw_enum_strings[8] = {
  "BRW_RPC",
  "BRW_DISPAGES",
  "BRW_DISBLOCKS",
  "BRW_FRAG",
  "BRW_FLIGHT",
  "BRW_IOTIME",
  "BRW_IOSIZE",
  NULL
};



static int
_parse_brw_stat(pctx_t ctx, char *name, brw_t t, char *s, int len)
{
  histogram_t *brw_stats_hist = NULL;
  char buf[MAX_BRW_STAT_LEN];
  int i, n;

  if (proc_lustre_brwstats (ctx, name, t, &brw_stats_hist) < 0) {
    if (lmt_conf_get_proto_debug ())
      err ("error reading lustre brw_stats entry %d from %s proc", (int)t, name);
    return -1;
  }
  /*
   * At this point you want to construct a text representation of
   * the histogram. 
   */
  n = snprintf( buf, MAX_BRW_STAT_LEN, "%s:{", brw_enum_strings[t] );
  for (i = 0; i < brw_stats_hist->bincount - 1; i++)
    {
      n+= snprintf( buf+n, MAX_BRW_STAT_LEN - n, "%"PRIu64":{%"PRIu64",%"PRIu64"},",
		    brw_stats_hist->bin[i].x, brw_stats_hist->bin[i].yr, 
		    brw_stats_hist->bin[i].yw);
      if (n >= MAX_BRW_STAT_LEN) {
	if (lmt_conf_get_proto_debug ())
	  msg ("string overflow");
	return -1;
      }
    }
  /* The last one has a closing brace rather than a comma */  
  i = brw_stats_hist->bincount - 1;
  n+= snprintf( buf+n, MAX_BRW_STAT_LEN - n, "%"PRIu64":{%"PRIu64",%"PRIu64"}}",
		brw_stats_hist->bin[i].x, brw_stats_hist->bin[i].yr, 
		brw_stats_hist->bin[i].yw);
  if (n >= MAX_BRW_STAT_LEN) {
    if (lmt_conf_get_proto_debug ())
      msg ("string overflow");
    return -1;
  }
  /* Now put the entire thing in the provided cerebro message buffer */
  n = snprintf (s, len, "%s;", buf);
  if (n >= len) {
    if (lmt_conf_get_proto_debug ())
      msg ("string overflow");
    return -1;
    }
  if(brw_stats_hist != NULL)
    histogram_destroy(brw_stats_hist);
  return 0;
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
    n = snprintf( s, len, "%s;", uuid );
    if (n >= len) {
        if (lmt_conf_get_proto_debug ())
            msg ("string overflow");
        goto done;
    }
    s += n; len -= n;
    /* Sequence through the various brw_stats histograms */
    brw_t t = BRW_RPC;
    while( brw_enum_strings[t] != NULL )
      {
	if( (n = _parse_brw_stat(ctx, name, t, s, len)) < 0 )
	  goto done;
	s += n; len -= n;
	t++;
      }
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

int
lmt_rpc_decode (const char *s, char **ossnamep, float *tbdp, List *ostinfop)
{
    int retval = -1;
    char *ossname =  xmalloc (strlen(s) + 1);
    char *cpy = NULL;
    List ostinfo = list_create ((ListDelF)free);

    if (sscanf (s, "%[^;];", ossname) != 1) {
        if (lmt_conf_get_proto_debug ())
            msg ("lmt_rpc: parse error: oss name");
        goto done;
    }
    if (!(s = strskip (s, 1, ';'))) {
        if (lmt_conf_get_proto_debug ())
            msg ("lmt_rpc: parse error: skipping oss name");
        goto done;
    }
    /* Get the name and seven brw_stats histograms */
    while ((cpy = strskipcpy (&s, 8, ';')))
        list_append (ostinfo, cpy);
    if (strlen (s) > 0) {
        if (lmt_conf_get_proto_debug ())
            msg ("lmt_rpc: parse error: string not exhausted");
        goto done;
    }
    *ossnamep = ossname;
    *ostinfop = ostinfo;
    retval = 0;
done:
    if (retval < 0) {
        free (ossname);
        list_destroy (ostinfo);
    }
    return retval;
}

int
lmt_rpc_decode_ostinfo (const char *s, char **ostnamep, uint64_t *tbdp)
{
    int retval = -1;
    char *ostname = xmalloc (strlen (s) + 1);;
    char *rpc_hist = xmalloc (strlen (s) + 1);;
    char *dispages_hist = xmalloc (strlen (s) + 1);;
    char *disblocks_hist = xmalloc (strlen (s) + 1);;
    char *frag_hist = xmalloc (strlen (s) + 1);;
    char *flight_hist = xmalloc (strlen (s) + 1);;
    char *iotime_hist = xmalloc (strlen (s) + 1);;
    char *iosize_hist = xmalloc (strlen (s) + 1);;
    uint64_t tbd;

    if (sscanf( s, "%[^;];{%[^;]};{%[^;]};{%[^;]};{%[^;]};{%[^;]};{%[^;]};{%[^;]};",
                ostname, rpc_hist, dispages_hist, disblocks_hist, 
		frag_hist, flight_hist, iotime_hist, iosize_hist) != 8) {
        if (lmt_conf_get_proto_debug ())
            msg ("lmt_rpc: parse error: rpc ostinfo");
        goto done;
    }
    *ostnamep = ostname;
    *tbdp = rpc_hist;
    /* Now that I have it, what do I do with it? */
    retval = 0;
done:
    if (retval < 0) {
        free (ostname);
        free (rpc_hist);
        free (dispages_hist);
        free (disblocks_hist);
        free (frag_hist);
        free (flight_hist);
        free (iotime_hist);
        free (iosize_hist);
    }
    return retval;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
