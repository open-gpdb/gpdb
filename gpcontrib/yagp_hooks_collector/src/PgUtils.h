#pragma once

extern "C" {
#include "postgres.h"
#include "commands/explain.h"
}

#include <string>

class Config;

std::string get_user_name();
std::string get_db_name();
std::string get_rg_name();
bool is_top_level_query(QueryDesc *query_desc, int nesting_level);
bool nesting_is_valid(QueryDesc *query_desc, int nesting_level,
                      const Config &config);
bool need_report_nested_query(const Config &config);
bool filter_query(QueryDesc *query_desc, const Config &config);
