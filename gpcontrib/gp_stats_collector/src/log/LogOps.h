#pragma once

#include <string>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
}

extern "C" {
/* CREATE TABLE gpsc.__log (...); */
void init_log();

/* TRUNCATE gpsc.__log */
void truncate_log();
}

/* INSERT INTO gpsc.__log VALUES (...) */
void insert_log(const gpsc::SetQueryReq &req, bool utility);
