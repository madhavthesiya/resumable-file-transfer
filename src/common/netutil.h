/*
 * netutil.h - the socket boilerplate shared by every server and client.
 *
 * HOW THIS WORKS
 * --------------
 * Nothing here is protocol-aware; it is purely "set up a TCP socket correctly".
 * Keeping it in one place means the eight executables cannot drift apart on
 * details like SO_REUSEADDR or TCP_NODELAY, and the socket-level performance
 * knobs evaluated in the report are all applied through this one file.
 */
#ifndef IT305_NETUTIL_H
#define IT305_NETUTIL_H

#include <stddef.h>
#include <stdint.h>

/* Create a listening socket bound to 0.0.0.0:port.
 * SO_REUSEADDR is set so a server can be restarted immediately after exiting
 * while old connections are still in TIME_WAIT - without it, the repeated
 * server restarts the benchmark harness performs would fail with EADDRINUSE.
 * Returns the fd, or -1 with errno set (and the reason already logged). */
int net_listen(uint16_t port, int backlog);

/* Connect to host:port, giving up after NET_CONNECT_TIMEOUT_MS.
 * `host` may be a dotted-quad address or a DNS name - names matter because
 * container and lab peers are addressed by name, not by literal address.
 * Returns the fd, or -1 with errno set (ETIMEDOUT if the deadline passed).
 *
 * A bounded connect matters more than it looks.  A host that REFUSES a
 * connection answers with RST immediately, but a host that DROPS the SYN -
 * a firewall, a black-holing router, a machine that vanished - says nothing
 * at all, and a blocking connect() then waits for the kernel's own retry
 * schedule, which is north of two minutes on Linux.  A client whose whole
 * purpose is to retry quickly cannot afford to be stuck there. */
#define NET_CONNECT_TIMEOUT_MS 10000
int net_connect(const char *host, uint16_t port);

/* Same, with an explicit deadline in milliseconds; <= 0 means "block".
 * The deadline covers the whole call, so a name resolving to several
 * addresses cannot multiply the caller's timeout. */
int net_connect_timeout(const char *host, uint16_t port, int timeout_ms);

/* Socket options.  Each returns 0 on success, -1 on failure (already logged).
 * All are optional in the sense that the protocol is correct without them. */
int net_set_nodelay(int fd, int on);     /* disable Nagle's algorithm        */
int net_set_nonblock(int fd, int on);    /* O_NONBLOCK, for the epoll loop   */
int net_set_sndbuf(int fd, int bytes);   /* SO_SNDBUF                        */
int net_set_rcvbuf(int fd, int bytes);   /* SO_RCVBUF                        */
int net_set_rcvtimeo(int fd, int ms);    /* SO_RCVTIMEO; 0 clears it         */

/* Write "ip:port" of the connected peer into `out`.  On failure writes "?:0",
 * so the result is always safe to print. */
void net_peer_str(int fd, char *out, size_t outsz);

#endif /* IT305_NETUTIL_H */
