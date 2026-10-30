#include "common.h"
#include "logging.h"
#include "netutil.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int net_listen(uint16_t port, int backlog)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        log_errno(LOG_ERROR, "socket");
        return -1;
    }

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) < 0)
        log_errno(LOG_WARN, "setsockopt SO_REUSEADDR");   /* not fatal */

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        log_errno(LOG_ERROR, "bind");
        close(fd);
        return -1;
    }
    if (listen(fd, backlog) < 0) {
        log_errno(LOG_ERROR, "listen");
        close(fd);
        return -1;
    }
    return fd;
}

int net_connect(const char *host, uint16_t port)
{
    return net_connect_timeout(host, port, NET_CONNECT_TIMEOUT_MS);
}

/* One bounded connect attempt against one resolved address.  Returns the fd,
 * or -1 with errno set; `deadline` is an absolute mono_now() value, or 0 to
 * block indefinitely. */
static int connect_one(const struct sockaddr *sa, socklen_t salen, double deadline)
{
    int fd = socket(sa->sa_family, SOCK_STREAM, 0);
    if (fd < 0) {
        log_errno(LOG_ERROR, "socket");
        return -1;
    }
    const int bounded = deadline > 0;        /* 0 means block indefinitely     */

    /* The connect is driven non-blocking so the wait can be bounded.  Doing it
     * this way also sidesteps a trap in the blocking form: when connect() is
     * interrupted by a signal the attempt keeps running in the kernel, so
     * calling it again returns EALREADY rather than retrying - looping on
     * EINTR around a blocking connect() is a bug, not a fix.  poll() has no
     * such quirk; it is simply restarted against the remaining deadline. */
    if (bounded && net_set_nonblock(fd, 1) < 0) {
        close(fd);
        return -1;
    }

    int rc;
    do {
        rc = connect(fd, sa, salen);
    } while (rc < 0 && errno == EINTR && !bounded);

    if (rc == 0)
        goto connected;                       /* loopback usually lands here */

    if (!bounded || (errno != EINPROGRESS && errno != EINTR)) {
        int saved = errno;                    /* a refused connect is often  */
        close(fd);                            /* expected; the caller logs   */
        errno = saved;
        return -1;
    }

    /* Wait for the handshake, re-arming poll() against the original deadline
     * so that signals cannot extend the total wait. */
    {
        for (;;) {
            double left = deadline - mono_now();
            if (left <= 0) {
                close(fd);
                errno = ETIMEDOUT;
                return -1;
            }

            struct pollfd pfd = { .fd = fd, .events = POLLOUT };
            int pr = poll(&pfd, 1, (int)(left * 1000) + 1);
            if (pr < 0) {
                if (errno == EINTR) continue;
                int saved = errno;
                close(fd);
                errno = saved;
                return -1;
            }
            if (pr == 0) continue;            /* re-check against the deadline */

            /* Readiness only says the attempt finished; SO_ERROR says how. */
            int soerr = 0;
            socklen_t len = sizeof soerr;
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &len) < 0)
                soerr = errno;
            if (soerr != 0) {
                close(fd);
                errno = soerr;
                return -1;
            }
            break;
        }
    }

connected:
    /* Hand back a blocking socket: every caller expects one, and the epoll
     * loop in the parallel server sets O_NONBLOCK itself when it wants it. */
    if (bounded && net_set_nonblock(fd, 0) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int net_connect_timeout(const char *host, uint16_t port, int timeout_ms)
{
    /* getaddrinfo rather than inet_pton, so `host` may be a name as well as a
     * dotted quad.  That is not decoration: under Docker Compose, and on any
     * network with DNS, peers are addressed by name, and a client that only
     * accepts a literal address cannot reach them at all.
     *
     * The family is pinned to AF_INET because net_listen() binds an IPv4
     * socket; offering to resolve AAAA records would only produce addresses
     * this server never listens on. */
    char portstr[16];
    snprintf(portstr, sizeof portstr, "%u", (unsigned)port);

    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    struct addrinfo *res = NULL;
    int gai = getaddrinfo(host, portstr, &hints, &res);
    if (gai != 0) {
        log_error("cannot resolve '%s': %s", host,
                  gai == EAI_SYSTEM ? strerror(errno) : gai_strerror(gai));
        errno = (gai == EAI_SYSTEM) ? errno : EINVAL;
        return -1;
    }

    /* One deadline for the whole call, shared across candidates, so a name
     * with several A records cannot multiply the caller's timeout. */
    double deadline = timeout_ms > 0 ? mono_now() + timeout_ms / 1000.0 : 0;

    int saved = ECONNREFUSED;   /* if the list is somehow empty */
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        int fd = connect_one(ai->ai_addr, ai->ai_addrlen, deadline);
        if (fd >= 0) {
            freeaddrinfo(res);
            return fd;
        }
        saved = errno;
        if (deadline > 0 && mono_now() >= deadline) {
            saved = ETIMEDOUT;
            break;
        }
    }

    freeaddrinfo(res);
    errno = saved;             /* the caller logs: a refusal is often expected */
    return -1;
}

/* All five option setters share this shape, so they share one helper. */
static int set_int_opt(int fd, int level, int opt, int val, const char *what)
{
    if (setsockopt(fd, level, opt, &val, sizeof val) < 0) {
        log_errno(LOG_WARN, what);
        return -1;
    }
    return 0;
}

int net_set_nodelay(int fd, int on)
{
    return set_int_opt(fd, IPPROTO_TCP, TCP_NODELAY, on, "setsockopt TCP_NODELAY");
}

int net_set_sndbuf(int fd, int bytes)
{
    return set_int_opt(fd, SOL_SOCKET, SO_SNDBUF, bytes, "setsockopt SO_SNDBUF");
}

int net_set_rcvbuf(int fd, int bytes)
{
    return set_int_opt(fd, SOL_SOCKET, SO_RCVBUF, bytes, "setsockopt SO_RCVBUF");
}

int net_set_nonblock(int fd, int on)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        log_errno(LOG_WARN, "fcntl F_GETFL");
        return -1;
    }
    flags = on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    if (fcntl(fd, F_SETFL, flags) < 0) {
        log_errno(LOG_WARN, "fcntl F_SETFL");
        return -1;
    }
    return 0;
}

int net_set_rcvtimeo(int fd, int ms)
{
    struct timeval tv;
    tv.tv_sec  = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv) < 0) {
        log_errno(LOG_WARN, "setsockopt SO_RCVTIMEO");
        return -1;
    }
    return 0;
}

void net_peer_str(int fd, char *out, size_t outsz)
{
    struct sockaddr_in a;
    socklen_t len = sizeof a;
    char ip[INET_ADDRSTRLEN];

    if (getpeername(fd, (struct sockaddr *)&a, &len) == 0 &&
        inet_ntop(AF_INET, &a.sin_addr, ip, sizeof ip))
        snprintf(out, outsz, "%s:%u", ip, (unsigned)ntohs(a.sin_port));
    else
        snprintf(out, outsz, "?:0");
}
