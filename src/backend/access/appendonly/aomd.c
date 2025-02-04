/*-------------------------------------------------------------------------
 *
 * aomd.c
 *	  This code manages append only relations that reside on magnetic disk.
 *	  It serves the same general purpose as smgr/md.c however we introduce
 *    AO specific file access functions mainly because would like to bypass 
 *	  md.c's and bgwriter's fsyncing. AO relations also use a non constant
 *	  block number to file segment mapping unlike heap relations.
 *
 *	  As of now we still let md.c create and unlink AO relations for us. This
 *	  may need to change if inconsistencies arise.
 *
 * Portions Copyright (c) 2008, Greenplum Inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/appendonly/aomd.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <access/aomd.h>

#include "access/aomd.h"
#include "access/appendonlytid.h"
#include "access/appendonlywriter.h"
#include "catalog/catalog.h"
#include "catalog/pg_appendonly_fn.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbappendonlyxlog.h"
#include "common/relpath.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#define SEGNO_SUFFIX_LENGTH 12

static bool mdunlink_ao_perFile(const int segno, void *ctx);
static bool
mdunlink_ao_yezzey(const int segno, void *ctx);
static bool copy_append_only_data_perFile(const int segno, void *ctx);
static bool truncate_ao_perFile(const int segno, void *ctx);

int
AOSegmentFilePathNameLen(Relation rel)
{
	char		*basepath;
	int 		len;
		
	/* Get base path for this relation file */
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	/*
	 * The basepath will be the RelFileNode number.  Optional part is dot "." plus 
	 * 6 digit segment file number.
	 */
	len = strlen(basepath) + 8;	// Generous.
	
	pfree(basepath);

	return len;
}

/*
 * Formats an Append Only relation file segment file name.
 *
 * The filepathname parameter assume sufficient space.
 */
void
FormatAOSegmentFileName(char *basepath,
						int segno,
						int col,
						int32 *fileSegNo,
						char *filepathname)
{
	int	pseudoSegNo;
	
	Assert(segno >= 0);
	Assert(segno <= AOTupleId_MaxSegmentFileNum);

	if (col < 0)
	{
		/*
		 * Row oriented Append-Only.
		 */
		pseudoSegNo = segno;		
	}
	else
	{
		/*
		 * Column oriented Append-only.
		 */
		pseudoSegNo = (col*AOTupleId_MultiplierSegmentFileNum) + segno;
	}
	
	*fileSegNo = pseudoSegNo;

	if (pseudoSegNo > 0)
	{
		sprintf(filepathname, "%s.%u", basepath, pseudoSegNo);
	}
	else
		strcpy(filepathname, basepath);
}

/*
 * Make an Append Only relation file segment file name.
 *
 * The filepathname parameter assume sufficient space.
 */
void
MakeAOSegmentFileName(Relation rel,
					  int segno,
					  int col,
					  int32 *fileSegNo,
					  char *filepathname)
{
	char	*basepath;
	int32   fileSegNoLocal;
	
	/* Get base path for this relation file */
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	FormatAOSegmentFileName(basepath, segno, col, &fileSegNoLocal, filepathname);
	
	*fileSegNo = fileSegNoLocal;
	
	pfree(basepath);
}

/*
 * Open an Append Only relation file segment
 *
 * The fd module's PathNameOpenFile() is used to open the file, so the
 * the File* routines can be used to read, write, close, etc, the file.
 */
File
OpenAOSegmentFile(Relation aorel, const char *nspname, char *filepathname, int64 logicalEof, int64 modcount)
{
	int			fileFlags = O_RDWR | PG_BINARY;
	File		fd;

	errno = 0;
	

	RelationOpenSmgr(aorel);

	fd = aorel->rd_smgr->smgr_ao->smgr_AORelOpenSegFile(
		RelationGetRelid(aorel), 
		nspname,
		RelationGetRelationName(aorel),
		filepathname,
		fileFlags, 0600, modcount);

	if (fd < 0)
	{
		if (logicalEof == 0 && errno == ENOENT)
			return -1;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open Append-Only segment file \"%s\": %m",
						filepathname),
				 errdetail("logicalEof for open operation: %ld", logicalEof)));
	}

	RelationCloseSmgr(aorel);
	return fd;
}


/*
 * Close an Append Only relation file segment
 */
void
CloseAOSegmentFile(Relation aorel, File fd)
{
	RelationOpenSmgr(aorel);
	Assert(aorel->rd_smgr != NULL);

	aorel->rd_smgr->smgr_ao->smgr_FileClose(fd);

	RelationCloseSmgr(aorel);
}

/*
 * Truncate all bytes from offset to end of file.
 */
void
TruncateAOSegmentFile(File fd, Relation rel, int32 segFileNum, int64 offset)
{
	char *relname = RelationGetRelationName(rel);

	Assert(fd > 0);
	Assert(offset >= 0);

	RelationOpenSmgr(rel);

	Assert(rel->rd_smgr != NULL);

	/*
	 * Call the 'fd' module with a 64-bit length since AO segment files
	 * can be multi-gigabyte to the terabytes...
	 */
	if (rel->rd_smgr->smgr_ao->smgr_FileTruncate(fd, offset) != 0)
		ereport(ERROR,
				(errmsg("\"%s\": failed to truncate data after eof: %m",
					    relname)));
	if (XLogIsNeeded() && RelationNeedsWAL(rel))
		xlog_ao_truncate(rel->rd_node, segFileNum, offset);

	if (file_truncate_hook)
	{
		RelFileNodeBackend rnode;
		rnode.node = rel->rd_node;
		rnode.backend = rel->rd_backend;
		(*file_truncate_hook)(rnode);
	}

	RelationCloseSmgr(rel);
}

struct mdunlink_ao_callback_ctx
{
	char *segPath;
	char *segpathSuffixPosition;
};

struct truncate_ao_callback_ctx
{
	char *segPath;
	char *segpathSuffixPosition;
	Relation rel;
};


void
mdunlink_ao(const char *path, ForkNumber forkNumber)
{
	/*
	 * Unlogged AO tables have INIT_FORK, in addition to MAIN_FORK.  This
	 * function is called for each fork type.  For INIT_FORK, the "_init"
	 * file is unlinked generically by mdunlinkfork.
	 */
	if (forkNumber == INIT_FORKNUM)
		return;

	Assert(forkNumber == MAIN_FORKNUM || forkNumber == YEZZEY_FORKNUM);

	int pathSize = strlen(path);
	char *segPath = (char *) palloc(pathSize + SEGNO_SUFFIX_LENGTH);
	char *segPathSuffixPosition = segPath + pathSize;
	struct mdunlink_ao_callback_ctx unlinkFiles = { 0 };

	strncpy(segPath, path, pathSize);

	unlinkFiles.segPath = segPath;
	unlinkFiles.segpathSuffixPosition = segPathSuffixPosition;

	if (forkNumber == MAIN_FORKNUM) {
    	ao_foreach_extent_file(mdunlink_ao_perFile, &unlinkFiles);
	} else /* YEZZEY_FORKNUM */ {
		ao_foreach_extent_file(mdunlink_ao_yezzey, &unlinkFiles);
	}
	pfree(segPath);
}


static bool
mdunlink_ao_yezzey(const int segno, void *ctx)
{
	const struct mdunlink_ao_callback_ctx *unlinkFiles = ctx;

	char *segPath = unlinkFiles->segPath;
	char *segPathSuffixPosition = unlinkFiles->segpathSuffixPosition;

	sprintf(segPathSuffixPosition, ".%u_yezzey", segno);

	if (unlink(segPath) != 0)
	{
		/* ENOENT is expected after the end of the extensions */
		if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
							errmsg("could not remove file \"%s\": %m", segPath)));
		else
			return false;
	}

	return true;
}


static bool
mdunlink_ao_perFile(const int segno, void *ctx)
{
	const struct mdunlink_ao_callback_ctx *unlinkFiles = ctx;

	char *segPath = unlinkFiles->segPath;
	char *segPathSuffixPosition = unlinkFiles->segpathSuffixPosition;

	sprintf(segPathSuffixPosition, ".%u", segno);
	if (unlink(segPath) != 0)
	{
		/* ENOENT is expected after the end of the extensions */
		if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
							errmsg("could not remove file \"%s\": %m", segPath)));
		else
			return false;
	}

	return true;
}

static void
copy_file(char *srcsegpath, char *dstsegpath,
		  RelFileNode dst, int segfilenum, bool use_wal)
{
	File		srcFile;
	File		dstFile;
	const struct f_smgr_ao *srcSmgr;
	const struct f_smgr_ao *dstSmgr;
	int64		left;
	off_t		offset;
	char       *buffer = palloc(BLCKSZ);
	int dstflags;

	srcSmgr = smgrao();
	dstSmgr = smgrao();

	srcFile = srcSmgr->smgr_AORelOpenSegFile(
		InvalidOid /* dont need reloid */,
		NULL,
		NULL
	/*we dont need to pass original relation name here, because yezzey not need it in r/o case*/,
		srcsegpath, O_RDONLY | PG_BINARY, 0600, -1 /* FIXME */);

	if (srcFile < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not open file %s: %m", srcsegpath))));

	dstflags = O_WRONLY | O_EXCL | PG_BINARY;
	/*
	 * .0 relfilenode is expected to exist before calling this
	 * function. Caller calls RelationCreateStorage() which creates the base
	 * file for the relation. Hence use different flag for the same.
	 */
	if (segfilenum)
		dstflags |= O_CREAT;

	dstFile = dstSmgr->smgr_AORelOpenSegFile(
		InvalidOid /*FIXME: copying yezzey files may not work here */,
		NULL,
		NULL/*FIXME: copying yezzey files may not work here*/, 
		dstsegpath, dstflags, 0600, 0);
	if (dstFile < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not create destination file %s: %m", dstsegpath))));

	left = srcSmgr->smgr_FileSeek(srcFile, 0, SEEK_END);
	if (left < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not seek to end of file %s: %m", srcsegpath))));

	if (srcSmgr->smgr_FileSeek(srcFile, 0, SEEK_SET) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not seek to beginning of file %s: %m", srcsegpath))));

	offset = 0;
	while(left > 0)
	{
		int			len;

		CHECK_FOR_INTERRUPTS();

		len = Min(left, BLCKSZ);
		if (srcSmgr->smgr_FileRead(srcFile, buffer, len) != len)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read %d bytes from file \"%s\": %m",
							len, srcsegpath)));

		if (dstSmgr->smgr_FileWrite(dstFile, buffer, len) != len)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write %d bytes to file \"%s\": %m",
							len, dstsegpath)));

		if (use_wal)
			xlog_ao_insert(dst, segfilenum, offset, buffer, len);

		offset += len;
		left -= len;
	}

	if (dstSmgr->smgr_FileSync(dstFile) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						dstsegpath)));
	srcSmgr->smgr_FileClose(srcFile);
	dstSmgr->smgr_FileClose(dstFile);
	pfree(buffer);
}

struct copy_append_only_data_callback_ctx {
	char *srcPath;
	char *dstPath;
	RelFileNode dst;
	bool useWal;
};

/*
 * Like copy_relation_data(), but for AO tables.
 *
 */
void
copy_append_only_data(RelFileNode src, RelFileNode dst,
        BackendId backendid, char relpersistence)
{
	char *srcPath;
	char *dstPath;
	bool useWal;
	struct copy_append_only_data_callback_ctx copyFiles = { 0 };
	/*
	 * We need to log the copied data in WAL iff WAL archiving/streaming is
	 * enabled AND it's a permanent relation.
	 */
	useWal = XLogIsNeeded() && relpersistence == RELPERSISTENCE_PERMANENT;

	srcPath = relpathbackend(src, backendid, MAIN_FORKNUM);
	dstPath = relpathbackend(dst, backendid, MAIN_FORKNUM);

	copy_file(srcPath, dstPath, dst, 0, useWal);

	copyFiles.srcPath = srcPath;
	copyFiles.dstPath = dstPath;
	copyFiles.dst = dst;
	copyFiles.useWal = useWal;

    ao_foreach_extent_file(copy_append_only_data_perFile, &copyFiles);

	if (file_extend_hook)
	{
		RelFileNodeBackend rnode;
		rnode.node = dst;
		rnode.backend = backendid;
		(*file_extend_hook)(rnode);
	}
}

static bool
copy_append_only_data_perFile(const int segno, void *ctx)
{
	const struct copy_append_only_data_callback_ctx *copyFiles = ctx;

	char srcSegPath[MAXPGPATH + 12];
	char dstSegPath[MAXPGPATH + 12];

	sprintf(srcSegPath, "%s.%u", copyFiles->srcPath, segno);
	if (access(srcSegPath, F_OK) != 0)
	{
		/* ENOENT is expected after the end of the extensions */
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("access failed for file \"%s\": %m", srcSegPath)));
		return false;
	}
	sprintf(dstSegPath, "%s.%u", copyFiles->dstPath, segno);
	copy_file(srcSegPath, dstSegPath, copyFiles->dst, segno, copyFiles->useWal);

	return true;
}

/*
 * ao_truncate_one_rel
 *
 * This routine deletes all data within the specified ao relation.
 */
void
ao_truncate_one_rel(Relation rel)
{
	char *basepath;
	char *segPath;
	char *segPathSuffixPosition;
	struct truncate_ao_callback_ctx truncateFiles = { 0 };
	int pathSize;

	/* Get base path for this relation file */
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	pathSize = strlen(basepath);
	segPath = (char *) palloc(pathSize + SEGNO_SUFFIX_LENGTH);
	segPathSuffixPosition = segPath + pathSize;
	strncpy(segPath, basepath, pathSize);

	truncateFiles.segPath = segPath;
	truncateFiles.segpathSuffixPosition = segPathSuffixPosition;
	truncateFiles.rel = rel;

	/* Truncate the actual file */
	ao_foreach_extent_file(truncate_ao_perFile, &truncateFiles);

	pfree(segPath);
	pfree(basepath);
}

/*
 * Truncate a specific segment file of ao relation.
 */
static bool
truncate_ao_perFile(const int segno, void *ctx)
{
	File		fd;
	Relation aorel;
	char *relname;
	char *nspname;
	const struct truncate_ao_callback_ctx *truncateFiles = ctx;

	char *segPath = truncateFiles->segPath;
	char *segPathSuffixPosition = truncateFiles->segpathSuffixPosition;
	aorel = truncateFiles->rel;

	sprintf(segPathSuffixPosition, ".%u", segno);

	relname = RelationGetRelationName(aorel);

	nspname = get_namespace_name(RelationGetNamespace(aorel));

	fd = OpenAOSegmentFile(aorel, nspname, segPath, 0, -1);

	if (fd >= 0)
	{
		TruncateAOSegmentFile(fd, aorel, segno, 0);
		CloseAOSegmentFile(aorel, fd);
	}
	else
	{
		RelationCloseSmgr(aorel);
		pfree(nspname);
		/* 
		 * we traverse possible segment files of AO/AOCS tables and call
		 * truncate_ao_perFile to truncate them. It is ok that some files do not exist
		 */
		return false;
	}

	pfree(nspname);
	return true;
}
