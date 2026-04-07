#pragma once

#include "protos/gpsc_set_service.pb.h"

class Config;

class UDSConnector
{
public:
	bool static report_query(const gpsc::SetQueryReq &req,
							 const std::string &event, const Config &config);
};
