#pragma once

#include "protos/yagpcc_set_service.pb.h"

class Config;

class UDSConnector {
public:
  static bool report_query(const yagpcc::SetQueryReq &req,
                           const std::string &event, const Config &config);
};
