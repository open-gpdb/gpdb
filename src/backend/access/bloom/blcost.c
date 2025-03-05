/*-------------------------------------------------------------------------
 *
 * blcost.c
 *		Cost estimate function for bloom indexes.
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/bloom/blcost.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "optimizer/cost.h"
#include "utils/selfuncs.h"

#include "bloom.h"

/*
 * Estimate cost of bloom index scan.
 */
Datum
blcostestimate(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	IndexPath  *path = (IndexPath *) PG_GETARG_POINTER(1);
	double		loop_count = PG_GETARG_FLOAT8(2);
	Cost	   *indexStartupCost = (Cost *) PG_GETARG_POINTER(3);
	Cost	   *indexTotalCost = (Cost *) PG_GETARG_POINTER(4);
	Selectivity *indexSelectivity = (Selectivity *) PG_GETARG_POINTER(5);
	double	   *indexCorrelation = (double *) PG_GETARG_POINTER(6);
	IndexOptInfo *index = path->indexinfo;
	List	   *qinfos;
	GenericCosts costs;

	/* Do preliminary analysis of indexquals */
	qinfos = deconstruct_indexquals(path);

	MemSet(&costs, 0, sizeof(costs));

	/* We have to visit all index tuples anyway */
	costs.numIndexTuples = index->tuples;

	/* Use generic estimate */
	genericcostestimate(root, path, loop_count, qinfos, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
}
