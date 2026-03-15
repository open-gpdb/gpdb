/*-------------------------------------------------------------------------
 * gpvpp_shim.h
 *    Thin wrapper over VPP's VPPCOM session API for Greenplum UDP
 *    interconnect kernel bypass.
 *
 *    When USE_VPP is defined at compile time, these functions replace
 *    the POSIX socket calls in ic_udpifc.c.  When USE_VPP is not
 *    defined, ic_udpifc.c continues to use the kernel networking stack
 *    unchanged.
 *
 * Copyright (c) 2024-Present Greenplum Database
 *
 * IDENTIFICATION
 *    src/backend/cdb/motion/gpvpp_shim.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPVPP_SHIM_H
#define GPVPP_SHIM_H

#ifdef USE_VPP

#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>

/* ----------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------
 */

/*
 * gpvpp_init
 *     Attach this process to the VPP application named "greenplum".
 *     Must be called once per backend process, BEFORE any other
 *     gpvpp_* call.
 *
 * Returns 0 on success, -1 on failure (logged internally).
 */
extern int gpvpp_init(void);

/*
 * gpvpp_worker_register
 *     Register the calling thread as a new VPPCOM worker.
 *     Must be called exactly once from the rx-thread BEFORE
 *     any gpvpp_* call on that thread.
 *
 * Returns 0 on success, -1 on failure.
 */
extern int gpvpp_worker_register(void);

/*
 * gpvpp_destroy
 *     Detach from VPP and release all resources.
 *     Called during CleanupMotionUDPIFC().
 */
extern void gpvpp_destroy(void);

/* ----------------------------------------------------------------
 * Socket-like operations
 * ----------------------------------------------------------------
 */

/*
 * gpvpp_socket
 *     Create a non-blocking VPPCOM UDP session.
 *
 * Returns a session handle (>= 0) on success, -1 on error.
 *
 * IMPORTANT: The returned session handle encodes the calling thread's
 * VPP worker index.  The session MUST only be used by the thread that
 * created it (or shared via VLS clone).  Creating a session on one
 * thread and using it from another will trigger an ASSERT failure.
 */
extern int gpvpp_socket(void);

/*
 * gpvpp_bind
 *     Bind a session to an ephemeral port (port 0) on the local address.
 *     For UDP sessions VPPCOM automatically enters listen mode.
 *
 *     addr / addrlen describe the local address.  If addr is NULL,
 *     binds to INADDR_ANY with an ephemeral port.
 *
 * Returns 0 on success, -1 on error.
 */
extern int gpvpp_bind(int sh, const struct sockaddr *addr, socklen_t addrlen);

/*
 * gpvpp_getsockname
 *     Retrieve the local address (including VPP-assigned ephemeral port)
 *     of a bound session.
 *
 * Returns 0 on success, -1 on error.
 */
extern int gpvpp_getsockname(int sh, struct sockaddr *addr, socklen_t *addrlen);

/*
 * gpvpp_set_buffer_size
 *     Configure the VPPCOM session FIFO sizes.
 *     type is SO_RCVBUF or SO_SNDBUF.
 *
 *     NOTE: Current VPP FIFO size attrs are stub implementations.
 *     Actual FIFO sizes are controlled by vcl.conf rx-fifo-size /
 *     tx-fifo-size.  This function always returns desired_size.
 *
 *     Returns the buffer size on success, 0 on error.
 */
extern uint32_t gpvpp_set_buffer_size(int sh, int type, uint32_t desired_size,
									  uint32_t min_size);

/*
 * gpvpp_close
 *     Close a session.
 */
extern void gpvpp_close(int sh);

/* ----------------------------------------------------------------
 * Data-path operations
 * ----------------------------------------------------------------
 */

/*
 * gpvpp_sendto
 *     Send a datagram to the given peer address.
 *
 *     Port values in dest_addr must be in network byte order
 *     (standard sockaddr convention).  The shim passes them
 *     directly to VPPCOM without conversion.
 *
 * Returns number of bytes sent (>= 0), or -1 on error.
 * errno is set to EAGAIN when the tx FIFO is full.
 */
extern int gpvpp_sendto(int sh, const void *buf, size_t len, int flags,
						const struct sockaddr *dest_addr, socklen_t addrlen);

/*
 * gpvpp_recvfrom
 *     Receive a datagram and the sender address.
 *
 * Returns number of bytes received (>= 0), or -1 on error.
 * errno is set to EWOULDBLOCK when the rx FIFO is empty.
 */
extern int gpvpp_recvfrom(int sh, void *buf, size_t len, int flags,
						  struct sockaddr *src_addr, socklen_t *addrlen);

/*
 * gpvpp_poll
 *     Wait for readability (POLLIN) on a session, up to timeout_ms.
 *
 * Returns > 0 if data is ready, 0 on timeout, -1 on error.
 *
 * Implementation uses the MQ eventfd + kernel epoll_wait to avoid
 * busy-polling.
 */
extern int gpvpp_poll(int sh, int timeout_ms);

/* ----------------------------------------------------------------
 * Shutdown wakeup (replaces sendDummyPacket)
 * ----------------------------------------------------------------
 */

/*
 * gpvpp_create_wakeup_fd
 *     Create an eventfd for waking the rx-thread from poll().
 *     Returns the eventfd (>= 0) on success, -1 on error.
 */
extern int gpvpp_create_wakeup_fd(void);

/*
 * gpvpp_signal_wakeup
 *     Write to the wakeup eventfd to unblock the rx-thread's poll.
 */
extern void gpvpp_signal_wakeup(int wakeup_fd);

/*
 * gpvpp_close_wakeup_fd
 *     Close the wakeup eventfd.
 */
extern void gpvpp_close_wakeup_fd(int wakeup_fd);

/*
 * gpvpp_create_rx_epoll
 *     Create a composite epoll fd that watches both the VPP MQ
 *     eventfd and the wakeup eventfd.  Call this once in the rx-thread
 *     after gpvpp_worker_register(), and reuse the returned fd for
 *     all subsequent gpvpp_poll_with_wakeup() calls.
 *
 *     Returns the composite epoll fd (>= 0) on success, -1 on error.
 */
extern int gpvpp_create_rx_epoll(int wakeup_fd);

/*
 * gpvpp_poll_with_wakeup
 *     Wait for data on a session OR a wakeup signal.
 *
 *     composite_epfd is the fd returned by gpvpp_create_rx_epoll().
 *     wakeup_fd is the eventfd (needed to drain it on wakeup).
 *
 *     Returns > 0 if session data ready, 0 on timeout,
 *     -2 if woken up by the wakeup fd, -1 on error.
 */
extern int gpvpp_poll_with_wakeup(int sh, int composite_epfd,
								  int wakeup_fd, int timeout_ms);

#endif /* USE_VPP */
#endif /* GPVPP_SHIM_H */
