#pragma once

#ifdef __cplusplus
extern "C" {
#endif

extern void hooks_init();
extern void hooks_deinit();
extern void gpsc_functions_reset();
extern Datum gpsc_functions_get(FunctionCallInfo fcinfo);

extern void init_log();
extern void truncate_log();

#ifdef __cplusplus
}
#endif