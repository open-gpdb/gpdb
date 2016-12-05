/*-------------------------------------------------------------------------
 *
 * crypt.h
 *	  Interface to libpq/crypt.c
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/crypt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CRYPT_H
#define PG_CRYPT_H

#include "c.h"

#include "libpq/libpq-be.h"
#include "libpq/md5.h"


extern int md5_crypt_verify(const Port *port, const char *role,
				 char *client_pass, char *md5_salt, int md5_salt_len, char **logdetail);

#endif
