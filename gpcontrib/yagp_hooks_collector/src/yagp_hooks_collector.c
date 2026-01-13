#include "postgres.h"
#include "cdb/cdbvars.h"
#include "utils/builtins.h"
#include "miscadmin.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>

#include "hook_wrappers.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);
PG_FUNCTION_INFO_V1(yagp_stat_messages_reset);
PG_FUNCTION_INFO_V1(yagp_stat_messages);
PG_FUNCTION_INFO_V1(yagp_init_log);
PG_FUNCTION_INFO_V1(yagp_truncate_log);

PG_FUNCTION_INFO_V1(yagp_test_uds_start_server);
PG_FUNCTION_INFO_V1(yagp_test_uds_receive);
PG_FUNCTION_INFO_V1(yagp_test_uds_stop_server);

#define TEST_MAX_CONNECTIONS 4
#define TEST_RCV_BUF_SIZE 8192
#define TEST_POLL_TIMEOUT_MS 200

static int server_fd = -1;
static char *sock_path = NULL;

void _PG_init(void) {
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE) {
    hooks_init();
  }
}

void _PG_fini(void) {
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE) {
    hooks_deinit();
  }
}

Datum yagp_stat_messages_reset(PG_FUNCTION_ARGS) {
  yagp_functions_reset();
  PG_RETURN_VOID();
}

Datum yagp_stat_messages(PG_FUNCTION_ARGS) {
  return yagp_functions_get(fcinfo);
}

Datum yagp_init_log(PG_FUNCTION_ARGS) {
  init_log();
  PG_RETURN_VOID();
}

Datum yagp_truncate_log(PG_FUNCTION_ARGS) {
  truncate_log();
  PG_RETURN_VOID();
}

Datum yagp_test_uds_start_server(PG_FUNCTION_ARGS) {
  char *path = text_to_cstring(PG_GETARG_TEXT_PP(0));
  struct sockaddr_un addr = {.sun_family = AF_UNIX};

  if (strlen(path) >= sizeof(addr.sun_path))
    ereport(ERROR, (errmsg("path too long")));

  yagp_test_uds_stop_server(fcinfo);

  strlcpy(addr.sun_path, path, sizeof(addr.sun_path));
  sock_path = MemoryContextStrdup(TopMemoryContext, path);
  unlink(path);
  pfree(path);

  if ((server_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0 ||
      bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
      listen(server_fd, TEST_MAX_CONNECTIONS) < 0) {
    yagp_test_uds_stop_server(fcinfo);
    ereport(ERROR, (errmsg("socket setup failed: %m")));
  }

  PG_RETURN_VOID();
}

Datum yagp_test_uds_receive(PG_FUNCTION_ARGS) {
  int timeout_ms = PG_GETARG_INT32(0);
  int64 total = 0;
  char buf[TEST_RCV_BUF_SIZE];
  int rc;
  struct pollfd pfd = {.fd = server_fd, .events = POLLIN};

  if (server_fd < 0)
    ereport(ERROR, (errmsg("server not started")));

  for (;;) {
    CHECK_FOR_INTERRUPTS();
    rc = poll(&pfd, 1, Min(timeout_ms, TEST_POLL_TIMEOUT_MS));
    if (rc > 0)
      break;
    if (rc < 0 && errno != EINTR)
      ereport(ERROR, (errmsg("poll: %m")));
    timeout_ms -= TEST_POLL_TIMEOUT_MS;
    if (timeout_ms <= 0)
      PG_RETURN_INT64(0);
  }

  if (pfd.revents & POLLIN) {
    int client = accept(server_fd, NULL, NULL);
    ssize_t n;

    if (client < 0)
      ereport(ERROR, (errmsg("accept: %m")));

    while ((n = recv(client, buf, sizeof(buf), 0)) != 0) {
      if (n > 0)
        total += n;
      else if (errno != EINTR)
        break;
    }

    close(client);
  }

  PG_RETURN_INT64(total);
}

Datum yagp_test_uds_stop_server(PG_FUNCTION_ARGS) {
  if (server_fd >= 0) {
    close(server_fd);
    server_fd = -1;
  }
  if (sock_path) {
    unlink(sock_path);
    pfree(sock_path);
    sock_path = NULL;
  }
  PG_RETURN_VOID();
}
