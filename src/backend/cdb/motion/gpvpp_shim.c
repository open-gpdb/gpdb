/*-------------------------------------------------------------------------
 * gpvpp_shim.c
 *    Thin wrapper over VPP's VPPCOM session API for Greenplum UDP
 *    interconnect kernel bypass.
 *
 * Copyright (c) 2024-Present Greenplum Database
 *
 * IDENTIFICATION
 *    src/backend/cdb/motion/gpvpp_shim.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef USE_VPP

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include <vcl/vppcom.h>

#include "gpvpp_shim.h"

/*
 * Convert a struct sockaddr (v4 or v6) to a vppcom_endpt_t.
 *
 * IMPORTANT: vppcom_endpt_t.port is in NETWORK byte order, matching
 * sin_port / sin6_port directly.  Do NOT apply ntohs/htons.
 * (Verified against LDP reference: ldp.c:1095, ldp.c:1580)
 *
 * Returns 0 on success, -1 on unsupported address family.
 */
static int
sockaddr_to_vppcom_ep(const struct sockaddr *sa, socklen_t salen,
					  vppcom_endpt_t *ep, uint8_t *addr_buf)
{
	memset(ep, 0, sizeof(*ep));

	if (sa->sa_family == AF_INET)
	{
		const struct sockaddr_in *in4 = (const struct sockaddr_in *) sa;

		ep->is_ip4 = 1;
		ep->port = in4->sin_port;	/* network byte order, no conversion */
		memcpy(addr_buf, &in4->sin_addr, sizeof(in4->sin_addr));
		ep->ip = addr_buf;
		return 0;
	}
	else if (sa->sa_family == AF_INET6)
	{
		const struct sockaddr_in6 *in6 = (const struct sockaddr_in6 *) sa;

		ep->is_ip4 = 0;
		ep->port = in6->sin6_port;	/* network byte order, no conversion */
		memcpy(addr_buf, &in6->sin6_addr, sizeof(in6->sin6_addr));
		ep->ip = addr_buf;
		return 0;
	}

	return -1;
}

/*
 * Convert a vppcom_endpt_t back to a struct sockaddr.
 * ep->port is in network byte order; we copy it directly.
 */
static void
vppcom_ep_to_sockaddr(const vppcom_endpt_t *ep,
					  struct sockaddr *addr, socklen_t *addrlen)
{
	if (ep->is_ip4)
	{
		struct sockaddr_in *in4 = (struct sockaddr_in *) addr;

		memset(in4, 0, sizeof(*in4));
		in4->sin_family = AF_INET;
		in4->sin_port = ep->port;	/* already network byte order */
		memcpy(&in4->sin_addr, ep->ip, sizeof(in4->sin_addr));
		*addrlen = sizeof(*in4);
	}
	else
	{
		struct sockaddr_in6 *in6 = (struct sockaddr_in6 *) addr;

		memset(in6, 0, sizeof(*in6));
		in6->sin6_family = AF_INET6;
		in6->sin6_port = ep->port;	/* already network byte order */
		memcpy(&in6->sin6_addr, ep->ip, sizeof(in6->sin6_addr));
		*addrlen = sizeof(*in6);
	}
}

/* ----------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------
 */

int
gpvpp_init(void)
{
	int rv;

	rv = vppcom_app_create("greenplum");
	if (rv < 0)
	{
		errno = -rv;
		return -1;
	}
	return 0;
}

int
gpvpp_worker_register(void)
{
	int rv;

	rv = vppcom_worker_register();
	if (rv < 0)
	{
		errno = -rv;
		return -1;
	}
	return 0;
}

void
gpvpp_destroy(void)
{
	vppcom_app_destroy();
}

/* ----------------------------------------------------------------
 * Socket-like operations
 * ----------------------------------------------------------------
 */

int
gpvpp_socket(void)
{
	int sh;

	/* Create a non-blocking UDP session */
	sh = vppcom_session_create(VPPCOM_PROTO_UDP, 1 /* is_nonblocking */);
	if (sh < 0)
	{
		errno = -sh;
		return -1;
	}
	return sh;
}

int
gpvpp_bind(int sh, const struct sockaddr *addr, socklen_t addrlen)
{
	vppcom_endpt_t ep;
	uint8_t addr_buf[16]; /* big enough for IPv6 */
	int rv;

	if (addr != NULL)
	{
		if (sockaddr_to_vppcom_ep(addr, addrlen, &ep, addr_buf) < 0)
		{
			errno = EAFNOSUPPORT;
			return -1;
		}
	}
	else
	{
		/* Bind to INADDR_ANY:0 (IPv4, ephemeral port) */
		memset(addr_buf, 0, sizeof(addr_buf));
		ep.is_ip4 = 1;
		ep.port = 0;
		ep.ip = addr_buf;
	}

	rv = vppcom_session_bind(sh, &ep);
	if (rv < 0)
	{
		errno = -rv;
		return -1;
	}

	/*
	 * For UDP, vppcom_session_bind() internally calls
	 * vppcom_session_listen(), so the session is ready to receive.
	 */
	return 0;
}

int
gpvpp_getsockname(int sh, struct sockaddr *addr, socklen_t *addrlen)
{
	vppcom_endpt_t ep;
	uint8_t addr_buf[16];
	uint32_t ep_len = sizeof(ep);	/* must be a variable — API takes pointer */
	int rv;

	ep.ip = addr_buf;
	rv = vppcom_session_attr(sh, VPPCOM_ATTR_GET_LCL_ADDR,
							 &ep, &ep_len);
	if (rv < 0)
	{
		errno = -rv;
		return -1;
	}

	vppcom_ep_to_sockaddr(&ep, addr, addrlen);
	return 0;
}

/*
 * gpvpp_set_buffer_size
 *
 * NOTE: VPPCOM_ATTR_SET_RX_FIFO_LEN / SET_TX_FIFO_LEN are stub
 * implementations in current VPP (they store the value but never
 * actually resize the session FIFO).  FIFO sizes are determined at
 * session creation time from vcl.conf's rx-fifo-size / tx-fifo-size.
 *
 * This function is kept for API compatibility but always returns
 * the desired_size (since the VPP stub always "succeeds").
 * Configure actual FIFO sizes in vcl.conf.
 */
uint32_t
gpvpp_set_buffer_size(int sh, int type, uint32_t desired_size,
					  uint32_t min_size)
{
	(void) sh;
	(void) type;
	(void) min_size;

	/*
	 * Return desired_size to match the caller's expectation.
	 * The real FIFO size is controlled by vcl.conf.
	 */
	return desired_size;
}

void
gpvpp_close(int sh)
{
	if (sh >= 0)
		vppcom_session_close(sh);
}

/* ----------------------------------------------------------------
 * Data-path operations
 * ----------------------------------------------------------------
 */

int
gpvpp_sendto(int sh, const void *buf, size_t len, int flags,
			 const struct sockaddr *dest_addr, socklen_t addrlen)
{
	vppcom_endpt_t ep;
	uint8_t addr_buf[16];
	int n;

	if (dest_addr != NULL)
	{
		if (sockaddr_to_vppcom_ep(dest_addr, addrlen, &ep, addr_buf) < 0)
		{
			errno = EAFNOSUPPORT;
			return -1;
		}
	}

	n = vppcom_session_sendto(sh, (void *) buf, len, flags,
							  dest_addr ? &ep : NULL);
	if (n < 0)
	{
		if (n == VPPCOM_EWOULDBLOCK || n == -EAGAIN)
			errno = EAGAIN;
		else
			errno = -n;
		return -1;
	}
	return n;
}

int
gpvpp_recvfrom(int sh, void *buf, size_t len, int flags,
			   struct sockaddr *src_addr, socklen_t *addrlen)
{
	vppcom_endpt_t ep;
	uint8_t addr_buf[16];
	int n;

	ep.ip = addr_buf;

	n = vppcom_session_recvfrom(sh, buf, len, flags, &ep);
	if (n < 0)
	{
		if (n == VPPCOM_EWOULDBLOCK || n == -EWOULDBLOCK)
			errno = EWOULDBLOCK;
		else if (n == -EAGAIN)
			errno = EAGAIN;
		else
			errno = -n;
		return -1;
	}

	if (src_addr != NULL && addrlen != NULL)
		vppcom_ep_to_sockaddr(&ep, src_addr, addrlen);

	return n;
}

int
gpvpp_poll(int sh, int timeout_ms)
{
	int mq_epfd;
	struct epoll_event ev;
	int n;
	uint32_t nread_len = 0;
	int ready;

	/* First check if data is already available (non-blocking) */
	ready = vppcom_session_attr(sh, VPPCOM_ATTR_GET_NREAD, NULL, &nread_len);
	if (ready > 0)
		return 1;

	/* No data yet — wait on the MQ eventfd */
	mq_epfd = vppcom_mq_epoll_fd();
	if (mq_epfd < 0)
	{
		/* Fallback: tight poll if eventfd not available */
		vcl_poll_t vp;

		memset(&vp, 0, sizeof(vp));
		vp.sh = sh;
		vp.events = POLLIN;

		n = vppcom_poll(&vp, 1, (double) timeout_ms / 1000.0);
		if (n < 0)
		{
			errno = -n;
			return -1;
		}
		return n;
	}

	n = epoll_wait(mq_epfd, &ev, 1, timeout_ms);
	if (n < 0)
	{
		/* errno already set by kernel epoll_wait */
		return -1;
	}
	if (n == 0)
		return 0; /* timeout */

	/*
	 * MQ has events — drain the VCL MQ and check our session.
	 * vppcom_session_attr(GET_NREAD) will internally process
	 * pending MQ events.
	 */
	ready = vppcom_session_attr(sh, VPPCOM_ATTR_GET_NREAD, NULL, &nread_len);
	return (ready > 0) ? 1 : 0;
}

/* ----------------------------------------------------------------
 * Shutdown wakeup (replaces sendDummyPacket)
 * ----------------------------------------------------------------
 */

int
gpvpp_create_wakeup_fd(void)
{
	int fd;

	fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (fd < 0)
		return -1;

	return fd;
}

void
gpvpp_signal_wakeup(int wakeup_fd)
{
	uint64_t val = 1;

	/* Best-effort write; ignore errors */
	(void) write(wakeup_fd, &val, sizeof(val));
}

void
gpvpp_close_wakeup_fd(int wakeup_fd)
{
	if (wakeup_fd >= 0)
		close(wakeup_fd);
}

/* ----------------------------------------------------------------
 * Composite epoll for rx thread (created once, reused)
 * ----------------------------------------------------------------
 */

int
gpvpp_create_rx_epoll(int wakeup_fd)
{
	int epfd;
	struct epoll_event ev;
	int mq_epfd;

	epfd = epoll_create1(EPOLL_CLOEXEC);
	if (epfd < 0)
		return -1;

	/* Add VPP MQ eventfd */
	mq_epfd = vppcom_mq_epoll_fd();
	if (mq_epfd >= 0)
	{
		memset(&ev, 0, sizeof(ev));
		ev.events = EPOLLIN;
		ev.data.fd = mq_epfd;
		if (epoll_ctl(epfd, EPOLL_CTL_ADD, mq_epfd, &ev) < 0)
		{
			close(epfd);
			return -1;
		}
	}

	/* Add wakeup fd */
	memset(&ev, 0, sizeof(ev));
	ev.events = EPOLLIN;
	ev.data.fd = wakeup_fd;
	if (epoll_ctl(epfd, EPOLL_CTL_ADD, wakeup_fd, &ev) < 0)
	{
		close(epfd);
		return -1;
	}

	return epfd;
}

int
gpvpp_poll_with_wakeup(int sh, int composite_epfd, int wakeup_fd, int timeout_ms)
{
	struct epoll_event events[2];
	int n;
	int ret;
	uint32_t nread_len = 0;
	int ready;

	/* Quick non-blocking check first */
	ready = vppcom_session_attr(sh, VPPCOM_ATTR_GET_NREAD, NULL, &nread_len);
	if (ready > 0)
		return 1;

	n = epoll_wait(composite_epfd, events, 2, timeout_ms);
	ret = 0;

	if (n < 0)
	{
		if (errno == EINTR)
			return 0; /* treat as timeout for signal safety */
		ret = -1; /* errno set by epoll_wait */
	}
	else if (n == 0)
	{
		ret = 0; /* timeout */
	}
	else
	{
		int i;

		for (i = 0; i < n; i++)
		{
			if (events[i].data.fd == wakeup_fd)
			{
				/* Drain the eventfd */
				uint64_t val;

				(void) read(wakeup_fd, &val, sizeof(val));
				ret = -2; /* wakeup signal */
			}
			else
			{
				/* MQ event — check if our session has data */
				ready = vppcom_session_attr(sh, VPPCOM_ATTR_GET_NREAD,
											NULL, &nread_len);
				if (ready > 0)
					ret = 1;
			}
		}
	}

	return ret;
}

#endif /* USE_VPP */
