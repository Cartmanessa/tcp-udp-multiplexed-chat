#include <sys/types.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define SESSION_LIMIT 64
#define RX_RING_BYTES 600000
static int socket_make_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
#define SESS_CMD_PENDING 0
#define SESS_PUT_MODE    1
#define SESS_GET_MODE    2
struct TcpSession {
    int sock;
    int role;
    char endpoint[48];
    unsigned char rx_ring[RX_RING_BYTES];
    int rx_used;
    unsigned char *tx_heap;
    int tx_total;
    int tx_offset;
    int put_ok_sent;
    int put_pending_len;
    int put_d1, put_m1, put_y1, put_d2, put_m2, put_y2, put_hh, put_mi, put_sc;
};
static struct TcpSession g_pool[SESSION_LIMIT];
static int g_active = 0;
static int g_halt = 0;
static char g_msg_scratch[RX_RING_BYTES + 1];
static const char g_okstr[] = "ok";
static void journal_append(const char *endpoint, int d1, int m1, int y1, int d2, int m2, int y2, int hh, int mi, int sc, const char *body) {
    FILE *fp = fopen("msg.txt", "a");
    if (!fp) return;
    fprintf(fp, "%s %02d.%02d.%04d %02d.%02d.%04d %02d:%02d:%02d %s\n", endpoint, d1, m1, y1, d2, m2, y2, hh, mi, sc, body);
    fclose(fp);
}
static int complete_put_ok_and_commit(struct TcpSession *sess) {
    unsigned char *buf = sess->rx_ring;
    int n = sess->rx_used;
    while (sess->put_ok_sent < 2) {
        int m = send(sess->sock, g_okstr + sess->put_ok_sent, 2 - sess->put_ok_sent, MSG_NOSIGNAL);
        if (m < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return 0;
            return -1;
        }
        if (m == 0)
            return -1;
        sess->put_ok_sent += m;
    }
    journal_append(sess->endpoint, sess->put_d1, sess->put_m1, sess->put_y1, sess->put_d2, sess->put_m2, sess->put_y2, sess->put_hh, sess->put_mi, sess->put_sc, g_msg_scratch);
    memmove(buf, buf + sess->put_pending_len, n - sess->put_pending_len);
    sess->rx_used -= sess->put_pending_len;
    sess->put_pending_len = 0;
    sess->put_ok_sent = 0;
    if (strcmp(g_msg_scratch, "stop") == 0)
        g_halt = 1;
    return 1;
}
static int drain_one_put_frame(struct TcpSession *sess) {
    unsigned char *buf = sess->rx_ring;
    int n = sess->rx_used;
    int st;
    if (sess->put_pending_len > 0) {
        st = complete_put_ok_and_commit(sess);
        if (st <= 0)
            return st;
        n = sess->rx_used;
    }
    if (n < 16)
        return 0;
    {
        int cursor = 4;
        int d1 = (int)buf[cursor++];
        int m1 = (int)buf[cursor++];
        int y1 = ((int)buf[cursor] << 8) | (int)buf[cursor + 1];
        cursor += 2;
        int d2 = (int)buf[cursor++];
        int m2 = (int)buf[cursor++];
        int y2 = ((int)buf[cursor] << 8) | (int)buf[cursor + 1];
        cursor += 2;
        int hh = (int)buf[cursor++];
        int mi = (int)buf[cursor++];
        int sc = (int)buf[cursor++];
        int text_off = cursor;
        for (; cursor < n && buf[cursor] != '\0'; cursor++) { }
        if (cursor >= n)
            return 0;
        {
            int tlen = cursor - text_off;
            if (tlen > RX_RING_BYTES)
                return -1;
            memcpy(g_msg_scratch, buf + text_off, (size_t)tlen);
            g_msg_scratch[tlen] = '\0';
            cursor++;
        }
        sess->put_pending_len = cursor;
        sess->put_ok_sent = 0;
        sess->put_d1 = d1;
        sess->put_m1 = m1;
        sess->put_y1 = y1;
        sess->put_d2 = d2;
        sess->put_m2 = m2;
        sess->put_y2 = y2;
        sess->put_hh = hh;
        sess->put_mi = mi;
        sess->put_sc = sc;
        return complete_put_ok_and_commit(sess);
    }
}
static void build_get_payload(struct TcpSession *sess) {
    FILE *fp = fopen("msg.txt", "r");
    if (!fp) {
        sess->tx_total = 0;
        return;
    }
    unsigned char *blob = NULL;
    int blob_len = 0;
    int blob_cap = 0;
    char row[131072];
    int  row_index = 0;
    for (; fgets(row, sizeof(row), fp); ) {
        char peer_token[64];
        int  d1,m1,y1, d2,m2,y2, hh,mi,sc;
        if (sscanf(row, "%63s %d.%d.%d %d.%d.%d %d:%d:%d", peer_token, &d1,&m1,&y1, &d2,&m2,&y2, &hh,&mi,&sc) != 10)
            continue;
        const char *tail = row;
        int space_hops = 0;
        for (; *tail && space_hops < 4; tail++) {
            if (*tail == ' ') space_hops++;
        }
        if (!*tail) continue;
        int tlen = (int)strlen(tail);
        for (; tlen > 0 && (tail[tlen-1] == '\n' || tail[tlen-1] == '\r'); tlen--) { }
        int need = 16 + tlen;
        if (blob_len + need > blob_cap) {
            blob_cap = (blob_len + need) * 2 + 1024;
            blob = (unsigned char *)realloc(blob, blob_cap);
            if (!blob) { blob_len = 0; break; }
        }
        unsigned char *slot = blob + blob_len;
        int w = 0;
        {
            unsigned int be_idx = htonl((unsigned int)row_index);
            memcpy(slot + w, &be_idx, 4); w += 4;
        }
        slot[w++] = (unsigned char)d1;
        slot[w++] = (unsigned char)m1;
        {
            unsigned short be_y = htons((unsigned short)y1);
            memcpy(slot + w, &be_y, 2); w += 2;
        }
        slot[w++] = (unsigned char)d2;
        slot[w++] = (unsigned char)m2;
        {
            unsigned short be_y = htons((unsigned short)y2);
            memcpy(slot + w, &be_y, 2); w += 2;
        }
        slot[w++] = (unsigned char)hh;
        slot[w++] = (unsigned char)mi;
        slot[w++] = (unsigned char)sc;
        memcpy(slot + w, tail, tlen); w += tlen;
        slot[w++] = 0;
        blob_len += w;
        row_index++;
    }
    fclose(fp);
    sess->tx_heap = blob;
    sess->tx_total = blob_len;
    sess->tx_offset = 0;
}
static void detach_session(int idx) {
    close(g_pool[idx].sock);
    if (g_pool[idx].tx_heap) {
        free(g_pool[idx].tx_heap);
    }
    fprintf(stdout, "Client disconnected: %s\n", g_pool[idx].endpoint);
    memmove(&g_pool[idx], &g_pool[idx+1], sizeof(struct TcpSession) * (g_active - idx - 1));
    g_active--;
}
static void register_session(int fd, struct sockaddr_in *addr) {
    if (g_active >= SESSION_LIMIT) {
        fprintf(stdout, "Too many clients, rejecting connection\n");
        close(fd);
        return;
    }
    socket_make_nonblocking(fd);
    struct TcpSession *sess = &g_pool[g_active++];
    memset(sess, 0, sizeof(*sess));
    sess->sock = fd;
    sess->role = SESS_CMD_PENDING;
    unsigned int host = ntohl(addr->sin_addr.s_addr);
    snprintf(sess->endpoint, sizeof(sess->endpoint), "%u.%u.%u.%u:%u",
             (host>>24)&0xff, (host>>16)&0xff, (host>>8)&0xff, host&0xff,
             ntohs(addr->sin_port));
    fprintf(stdout, "Client connected: %s\n", sess->endpoint);
}
static void on_session_input(int idx) {
    struct TcpSession *sess = &g_pool[idx];
    {
        int n = recv(sess->sock,
                     sess->rx_ring + sess->rx_used,
                     sizeof(sess->rx_ring) - sess->rx_used,
                     0);
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            n = 0;
        } else if (n <= 0) {
            detach_session(idx);
            return;
        }
        sess->rx_used += n;
    }
    if (sess->role == SESS_CMD_PENDING && sess->rx_used >= 3) {
        if (memcmp(sess->rx_ring, "put", 3) == 0) {
            sess->role = SESS_PUT_MODE;
            memmove(sess->rx_ring, sess->rx_ring + 3, sess->rx_used - 3);
            sess->rx_used -= 3;
        } else if (memcmp(sess->rx_ring, "get", 3) == 0) {
            sess->role = SESS_GET_MODE;
            sess->rx_used = 0;
            build_get_payload(sess);
            return;
        } else {
            fprintf(stdout, "Unknown command from %s\n", sess->endpoint);
            detach_session(idx);
            return;
        }
    }
    if (sess->role == SESS_PUT_MODE) {
        int st;
        for (;;) {
            st = drain_one_put_frame(sess);
            if (st == 0) break;
            if (st < 0) {
                detach_session(idx);
                return;
            }
        }
    }
}
static void on_session_output(int idx) {
    struct TcpSession *sess = &g_pool[idx];
    if (sess->tx_offset >= sess->tx_total) {
        detach_session(idx);
        return;
    }
    int n = send(sess->sock,
                 sess->tx_heap + sess->tx_offset,
                 sess->tx_total - sess->tx_offset,
                 MSG_NOSIGNAL);
    if (n <= 0) {
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        detach_session(idx);
        return;
    }
    sess->tx_offset += n;
    if (sess->tx_offset >= sess->tx_total)
        detach_session(idx);
}
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stdout, "Usage: tcpserver PORT\n");
        return 1;
    }
    int listen_port = atoi(argv[1]);
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        perror("socket");
        return 1;
    }
    int reuse = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    socket_make_nonblocking(listener);
    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons((unsigned short)listen_port);
    bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(listener, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0) {
        fprintf(stdout, "ERROR: cannot bind port %d: %s\n", listen_port, strerror(errno));
        close(listener);
        return 1;
    }
    if (listen(listener, 16) < 0) {
        fprintf(stdout, "ERROR: listen: %s\n", strerror(errno));
        close(listener);
        return 1;
    }
    fprintf(stdout, "Listening TCP port: %d\n", listen_port);
    struct pollfd fds[1 + SESSION_LIMIT];
    for (; !g_halt; ) {
        int nfds = 0;
        fds[nfds].fd = listener;
        fds[nfds].events = POLLIN;
        fds[nfds].revents = 0;
        nfds++;
        int j;
        for (j = 0; j < g_active; j++) {
            fds[nfds].fd      = g_pool[j].sock;
            fds[nfds].revents = 0;
            if (g_pool[j].role == SESS_GET_MODE) {
                fds[nfds].events = POLLOUT;
            } else if (g_pool[j].role == SESS_PUT_MODE
                       && g_pool[j].put_pending_len > 0
                       && g_pool[j].put_ok_sent < 2) {
                fds[nfds].events = POLLIN | POLLOUT;
            } else {
                fds[nfds].events = POLLIN;
            }
            nfds++;
        }
        int pr = poll(fds, nfds, 500);
        if (pr < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }
        if (fds[0].revents & POLLIN) {
            struct sockaddr_in cli;
            socklen_t clen = sizeof(cli);
            int cs = accept(listener, (struct sockaddr *)&cli, &clen);
            if (cs >= 0) register_session(cs, &cli);
        }
        for (j = g_active - 1; j >= 0; j--) {
            int pi = j + 1;
            if (fds[pi].revents & (POLLERR | POLLHUP)) {
                detach_session(j);
                continue;
            }
            if (g_pool[j].role == SESS_GET_MODE) {
                if (fds[pi].revents & POLLOUT)
                    on_session_output(j);
            } else {
                if (fds[pi].revents & (POLLIN | POLLOUT))
                    on_session_input(j);
            }
        }
    }
    {
        int j;
        for (j = g_active - 1; j >= 0; j--) detach_session(j);
    }
    close(listener);
    fprintf(stdout, "Server terminated.\n");
    return 0;
}
