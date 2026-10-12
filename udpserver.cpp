#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#pragma comment(lib, "ws2_32.lib")
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#define PORT_SLOT_MAX   64
#define PEER_TABLE_MAX  256
#define SEQ_HISTORY_MAX 1024
#define IDLE_SEC_LIMIT  30
struct PeerEntry {
    int live;
    char tag[48];
    time_t seen_at;
    int seqbuf[SEQ_HISTORY_MAX];
    int seq_count;
};
static PeerEntry g_peers[PEER_TABLE_MAX];
static PeerEntry *peer_lookup_or_create(const char *tag) {
    int idx;
    int vacant = -1;
    for (idx = 0; idx < PEER_TABLE_MAX; idx++) {
        if (!g_peers[idx].live) {
            if (vacant < 0) vacant = idx;
            continue;
        }
        if (strcmp(g_peers[idx].tag, tag) == 0)
            return &g_peers[idx];
    }
    if (vacant < 0) return NULL;
    memset(&g_peers[vacant], 0, sizeof(g_peers[vacant]));
    g_peers[vacant].live = 1;
    g_peers[vacant].seen_at = time(NULL);
    strncpy(g_peers[vacant].tag, tag, sizeof(g_peers[vacant].tag) - 1);
    g_peers[vacant].tag[sizeof(g_peers[vacant].tag) - 1] = '\0';
    fprintf(stdout, "New client: %s\n", tag);
    return &g_peers[vacant];
}
static int peer_has_seq(PeerEntry *p, int seq_id) {
    int t;
    for (t = 0; t < p->seq_count; t++) {
        if (p->seqbuf[t] == seq_id) return 1;
    }
    return 0;
}
static void peer_push_seq(PeerEntry *p, int seq_id) {
    if (p->seq_count < SEQ_HISTORY_MAX)
        p->seqbuf[p->seq_count++] = seq_id;
}
static void peer_expire_stale(void) {
    time_t now = time(NULL);
    int idx;
    for (idx = 0; idx < PEER_TABLE_MAX; idx++) {
        if (g_peers[idx].live && (now - g_peers[idx].seen_at) > (time_t)IDLE_SEC_LIMIT) {
            fprintf(stdout, "Client expired: %s\n", g_peers[idx].tag);
            g_peers[idx].live = 0;
        }
    }
}
static void journal_line(const char *tag,
                         int d1, int m1, int y1,
                         int d2, int m2, int y2,
                         int hh, int mm, int ss,
                         const char *body) {
    FILE *fp = NULL;
    fp = fopen("msg.txt", "a");
    if (!fp) return;
    fprintf(fp, "%s %02d.%02d.%04d %02d.%02d.%04d %02d:%02d:%02d %s\n",
            tag, d1, m1, y1, d2, m2, y2, hh, mm, ss, body);
    fclose(fp);
}
static int unpack_wire_frame(const unsigned char *dg, int dg_len,
                             int *seq_id,
                             int *d1, int *m1, int *y1,
                             int *d2, int *m2, int *y2,
                             int *hh, int *mm, int *ss,
                             char *body, int body_cap)
{
    int pos;
    int txt0;
    int tlen;
    if (dg_len < 16) return 0;
    pos = 0;
    {
        unsigned int raw;
        memcpy(&raw, dg + pos, 4);
        pos += 4;
        *seq_id = (int)ntohl(raw);
    }
    *d1 = (int)dg[pos++];
    *m1 = (int)dg[pos++];
    {
        unsigned short yn;
        memcpy(&yn, dg + pos, 2);
        *y1 = (int)ntohs(yn);
        pos += 2;
    }
    *d2 = (int)dg[pos++];
    *m2 = (int)dg[pos++];
    {
        unsigned short yn;
        memcpy(&yn, dg + pos, 2);
        *y2 = (int)ntohs(yn);
        pos += 2;
    }
    *hh = (int)dg[pos++];
    *mm = (int)dg[pos++];
    *ss = (int)dg[pos++];
    txt0 = pos;
    for (; pos < dg_len && dg[pos] != '\0'; pos++) { }
    if (pos >= dg_len) return 0;
    tlen = pos - txt0;
    if (tlen >= body_cap) return 0;
    memcpy(body, dg + txt0, (size_t)tlen);
    body[tlen] = '\0';
    return 1;
}
static int g_shutdown = 0;
static void dispatch_datagram(SOCKET s,
                              const unsigned char *dg, int dg_len,
                              struct sockaddr_in *from, int fromlen)
{
    unsigned int ip;
    char tag[48];
    int seq_id;
    int d1, m1, y1, d2, m2, y2, hh, mm, ss;
    char payload[65536];
    unsigned char reply[20 * 4];
    int rlen;
    int base;
    int t;
    PeerEntry *p;
    ip = ntohl(from->sin_addr.s_addr);
    sprintf_s(tag, sizeof(tag), "%u.%u.%u.%u:%u",
              (unsigned int)((ip >> 24) & 0xffu),
              (unsigned int)((ip >> 16) & 0xffu),
              (unsigned int)((ip >> 8) & 0xffu),
              (unsigned int)(ip & 0xffu),
              (unsigned int)ntohs(from->sin_port));
    if (!unpack_wire_frame(dg, dg_len,
                           &seq_id,
                           &d1, &m1, &y1, &d2, &m2, &y2, &hh, &mm, &ss,
                           payload, (int)sizeof(payload))) {
        fprintf(stdout, "Invalid datagram from %s, ignored\n", tag);
        return;
    }
    p = peer_lookup_or_create(tag);
    if (!p) {
        fprintf(stdout, "No room for new client %s\n", tag);
        return;
    }
    p->seen_at = time(NULL);
    if (!peer_has_seq(p, seq_id)) {
        peer_push_seq(p, seq_id);
        journal_line(tag, d1, m1, y1, d2, m2, y2, hh, mm, ss, payload);
        fprintf(stdout, "Msg #%d from %s: %s\n", seq_id, tag, payload);
        if (strcmp(payload, "stop") == 0)
            g_shutdown = 1;
    }
    rlen = 0;
    base = p->seq_count - 20;
    if (base < 0) base = 0;
    for (t = base; t < p->seq_count; t++) {
        unsigned int be;
        be = htonl((unsigned int)p->seqbuf[t]);
        memcpy(reply + rlen, &be, 4);
        rlen += 4;
    }
    (void)sendto(s, (const char *)reply, rlen, 0,
                 (struct sockaddr *)from, fromlen);
}
static int wsa_fail(const char *where)
{
    fprintf(stdout, "ERROR %s: WSAError=%d\n", where, (int)WSAGetLastError());
    return -1;
}
int main(int argc, char *argv[])
{
    int lo;
    int hi;
    int span;
    int k;
    WSADATA wsa;
    SOCKET socks[PORT_SLOT_MAX];
    time_t last_sweep;
    fd_set rfds;
    SOCKET top;
    struct timeval tv;
    int sel;
    if (argc < 3) {
        fprintf(stdout, "Usage: udpserver PORT_FROM PORT_TO\n");
        return 1;
    }
    lo = atoi(argv[1]);
    hi = atoi(argv[2]);
    if (lo > hi || lo <= 0 || hi > 65535) {
        fprintf(stdout, "ERROR: invalid port range\n");
        return 1;
    }
    span = hi - lo + 1;
    if (span > PORT_SLOT_MAX) {
        fprintf(stdout, "ERROR: too many ports (max %d)\n", PORT_SLOT_MAX);
        return 1;
    }
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        fprintf(stdout, "ERROR: WSAStartup failed\n");
        return 1;
    }
    for (k = 0; k < span; k++) {
        struct sockaddr_in bind_addr;
        socks[k] = socket(AF_INET, SOCK_DGRAM, 0);
        if (socks[k] == INVALID_SOCKET) {
            int j;
            wsa_fail("socket");
            for (j = 0; j < k; j++)
                closesocket(socks[j]);
            WSACleanup();
            return 1;
        }
        {
            unsigned long async = 1;
            if (ioctlsocket(socks[k], FIONBIO, &async) != 0) {
                wsa_fail("ioctlsocket");
                while (k >= 0) { closesocket(socks[k]); k--; }
                WSACleanup();
                return 1;
            }
        }
        memset(&bind_addr, 0, sizeof(bind_addr));
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_port = htons((unsigned short)(lo + k));
        bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(socks[k], (struct sockaddr *)&bind_addr, sizeof(bind_addr)) != 0) {
            fprintf(stdout, "ERROR: cannot bind port %d\n", lo + k);
            while (k >= 0) { closesocket(socks[k]); k--; }
            WSACleanup();
            return 1;
        }
        fprintf(stdout, "Listening UDP port: %d\n", lo + k);
    }
    last_sweep = time(NULL);
    while (!g_shutdown) {
        FD_ZERO(&rfds);
        top = 0;
        for (k = 0; k < span; k++) {
            FD_SET(socks[k], &rfds);
            if (socks[k] > top) top = socks[k];
        }
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        sel = select((int)(top + 1), &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            wsa_fail("select");
            break;
        }
        if (sel > 0) {
            for (k = 0; k < span; k++) {
                if (!FD_ISSET(socks[k], &rfds))
                    continue;
                {
                    unsigned char blob[65536];
                    struct sockaddr_in peer;
                    int plen = (int)sizeof(peer);
                    int n;
                    n = recvfrom(socks[k], (char *)blob, (int)sizeof(blob), 0,
                                 (struct sockaddr *)&peer, &plen);
                    if (n > 0)
                        dispatch_datagram(socks[k], blob, n, &peer, plen);
                }
            }
        }
        {
            time_t now = time(NULL);
            if ((now - last_sweep) >= 1) {
                peer_expire_stale();
                last_sweep = now;
            }
        }
    }
    for (k = 0; k < span; k++)
        closesocket(socks[k]);
    WSACleanup();
    fprintf(stdout, "Server terminated.\n");
    return 0;
}
