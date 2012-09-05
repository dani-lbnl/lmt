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
#include "brw.h"
#include "util.h"
#include "lmtconf.h"


#define LMT_BRW_PROTOCOL_VERSION 1

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
  return n;
}

static int
_get_brwstring (pctx_t ctx, char *name, char *s, int len)
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
lmt_brw_string (pctx_t ctx, char *s, int len)
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
    /* 
     * There is some confusion about versions. The CEREBRO protocol
     * version is 2 at this point.  There are also version 1 and
     * version 2 LMT metrics. The brw metric is version 1, which is
     * hard-coded here. Since I copied this module largely from the
     * version 2 ost metric I had to dig around a bit before I
     * realized this.
     */
    n = snprintf (s, len, "%d;%s;", LMT_BRW_PROTOCOL_VERSION, 
                  uts.nodename);
    if (n >= len) {
        if (lmt_conf_get_proto_debug ())
            msg ("string overflow");
        goto done;
    }
    itr = list_iterator_create (ostlist);
    while ((name = list_next (itr))) {
        used = strlen (s);
        if (_get_brwstring (ctx, name, s + used, len - used) < 0)
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
lmt_brw_decode_v1 (const char *s, char **ossnamep, List *ostinfop)
{
    int retval = -1;
    char *ossname =  xmalloc (strlen(s) + 1);
    char *cpy = NULL;
    List ostinfo = list_create ((ListDelF)free);

    if (sscanf (s, "%*f;%[^;];", ossname) != 1) {
        if (lmt_conf_get_proto_debug ())
            msg ("lmt_brw: parse error: oss name");
        goto done;
    }
    if (!(s = strskip (s, 2, ';'))) {
        if (lmt_conf_get_proto_debug ())
            msg ("lmt_brw: parse error: skipping oss name");
        goto done;
    }
    /* Get the name and seven brw_stats histograms */
    while ((cpy = strskipcpy (&s, 8, ';')))
        list_append (ostinfo, cpy);
    if (strlen (s) > 0) {
        if (lmt_conf_get_proto_debug ())
	  msg ("lmt_brw: parse error: string not exhausted: '%s'", s);
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

brw_stats_t *
brw_stats_create()
{
  brw_stats_t *s;

  if( !(s = malloc(sizeof(brw_stats_t))) )
    msg_exit( "Out of memory" );
  memset( s, 0, sizeof(brw_stats_t) );
  return( s );
}

void
brw_stats_destroy(brw_stats_t *s)
{
  int i;

  if( s )
    {
      for( i = 0; i < NUM_BRW_STATS; i++ )
	{
	  if( s->hist[i] != NULL )
	    {
	      free(s->hist[i]);
	      s->hist[i] = NULL;
	    }
	}
    }
}

int
lmt_brw_decode_v1_ostinfo (const char *s, char **ostnamep, 
			   brw_stats_t **statsp)
{
  int retval = -1;
  int i = 0;
  brw_stats_t *stats = brw_stats_create();
  char *ostname;

  if( (ostname = strskipcpy(&s, 1, ';')) == NULL )
    goto done;
  /* 
   * This rather lamely doesn't even decode much. It just
   * extracts the seven brw_stats histograms from the message
   * and preserves them a seven separate strings. They'll 
   * actually get interpreted once they are being put into the
   * DB.
   */

  while( (i < NUM_BRW_STATS) && *s ) 
    {
      if( (stats->hist[i] = strskipcpy(&s, 1, ';')) == NULL ) {
	if (lmt_conf_get_proto_debug ())
	  msg ("lmt_brw_decode_v1_ostinfo: parse error: failed to parse %s histogram", 
	       brw_enum_strings[i]);
	goto done;
      }
      i++;
    }
    *ostnamep = ostname;
    *statsp = stats;

    retval = 0;
done:
    if (retval < 0) {
      if( ostname != NULL ) free (ostname);
      if( stats != NULL ) brw_stats_destroy(stats);
    }
    return retval;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
