/*-------------------------------------------------------------------------
 *
 * compat.c
 *		Reimplementations of various backend functions.
 *
 * Portions Copyright (c) 2013-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pg_xlogdump/compat.c
 *
 * This file contains client-side implementations for various backend
 * functions that the rm_desc functions in *desc.c files rely on.
 *
 *-------------------------------------------------------------------------
 */

/* ugly hack, same as in e.g pg_controldata */
#define FRONTEND 1
#include "postgres.h"

#include <time.h>

#include "utils/datetime.h"
#include "lib/stringinfo.h"

#include "access/twophase.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "common/relpath.h"

/* copied from timestamp.c */
pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	pg_time_t	result;

#ifdef HAVE_INT64_TIMESTAMP
	result = (pg_time_t) (t / USECS_PER_SEC +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#else
	result = (pg_time_t) (t +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#endif
	return result;
}

/*
 * Stopgap implementation of timestamptz_to_str that doesn't depend on backend
 * infrastructure.  This will work for timestamps that are within the range
 * of the platform time_t type.  (pg_time_t is compatible except for possibly
 * being wider.)
 *
 * XXX the return value points to a static buffer, so beware of using more
 * than one result value concurrently.
 *
 * XXX: The backend timestamp infrastructure should instead be split out and
 * moved into src/common.  That's a large project though.
 */
const char *
timestamptz_to_str(TimestampTz dt)
{
	static char buf[MAXDATELEN + 1];
	char		ts[MAXDATELEN + 1];
	char		zone[MAXDATELEN + 1];
	time_t		result = (time_t) timestamptz_to_time_t(dt);
	struct tm  *ltime = localtime(&result);

	strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", ltime);
	strftime(zone, sizeof(zone), "%Z", ltime);

#ifdef HAVE_INT64_TIMESTAMP
	snprintf(buf, sizeof(buf),
			 "%s.%06d %s", ts, (int) (dt % USECS_PER_SEC), zone);
#else
	snprintf(buf, sizeof(buf),
			 "%s.%.6f %s", ts, fabs(dt - floor(dt)), zone);
#endif

	return buf;
}


/*
 * Provide a hacked up compat layer for StringInfos so xlog desc functions can
 * be linked/called.
 */


/*
 * initStringInfo
 *
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
void
initStringInfo(StringInfo str)
{
	int			size = 1024;	/* initial default buffer size */

	str->data = (char *) palloc(size);
	str->maxlen = size;
	resetStringInfo(str);
}

void
appendStringInfo(StringInfo str, const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	vprintf(fmt, args);
	va_end(args);
}

void
appendStringInfoString(StringInfo str, const char *string)
{
	appendStringInfo(str, "%s", string);
}

void
appendStringInfoChar(StringInfo str, char ch)
{
	appendStringInfo(str, "%c", ch);
}

/*
 * resetStringInfo
 *
 * Reset the StringInfo: the data buffer remains valid, but its
 * previous content, if any, is cleared.
 */
void
resetStringInfo(StringInfo str)
{
	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}


/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 12 bytes, counting a leading sign and trailing NUL).
 */
void
pg_ltoa(int32 value, char *a)
{
	char	   *start = a;
	bool		neg = false;

	/*
	 * Avoid problems with the most negative integer not being representable
	 * as a positive integer.
	 */
	if (value == (-2147483647 - 1))
	{
		memcpy(a, "-2147483648", 12);
		return;
	}
	else if (value < 0)
	{
		value = -value;
		neg = true;
	}

	/* Compute the result string backwards. */
	do
	{
		int32		remainder;
		int32		oldval = value;

		value /= 10;
		remainder = oldval - value * 10;
		*a++ = '0' + remainder;
	} while (value != 0);

	if (neg)
		*a++ = '-';

	/* Add trailing NUL byte, and back up 'a' to the last character. */
	*a-- = '\0';

	/* Reverse string. */
	while (start < a)
	{
		char		swap = *start;

		*start++ = *a;
		*a-- = swap;
	}
}
