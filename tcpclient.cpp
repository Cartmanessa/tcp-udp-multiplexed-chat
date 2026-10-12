#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define INPUT_LINE_CAP (1024 * 1024)
#define PAYLOAD_CAP    (1024 * 1024)
static char g_line_buf[INPUT_LINE_CAP];
static char g_payload_buf[PAYLOAD_CAP];
static int wsa_fail(const char *where) {
    fprintf(stdout, "ERROR %s: WSAError=%d\n", where, WSAGetLastError());
    return -1;
}
static int tcp_write_full(SOCKET h, const void *src, int nbytes) {
    int done;
    for (done = 0; done < nbytes; ) {
        int n = send(h, (const char *)src + done, nbytes - done, 0);
        if (n <= 0) return wsa_fail("send");
        done += n;
    }
    return 0;
}
static int tcp_read_full(SOCKET h, void *dst, int nbytes) {
    int done;
    for (done = 0; done < nbytes; ) {
        int n = recv(h, (char *)dst + done, nbytes - done, 0);
        if (n == 0) { fprintf(stdout, "ERROR: server closed connection\n"); return -1; }
        if (n < 0)  return wsa_fail("recv");
        done += n;
    }
    return 0;
}
static int parse_schedule_row(const char *row,
                              int *d1, int *m1, int *y1,
                              int *d2, int *m2, int *y2,
                              int *hh, int *mm, int *ss,
                              char *body, int body_cap) {
    const char *q = row;
    if (sscanf(q, "%d.%d.%d", d1, m1, y1) != 3) return 0;
    if (*d1 < 1 || *d1 > 31) return 0;
    if (*m1 < 1 || *m1 > 12) return 0;
    if (*y1 < 0 || *y1 > 9999) return 0;
    for (; *q && *q != ' '; q++) { }
    if (*q++ != ' ') return 0;
    if (sscanf(q, "%d.%d.%d", d2, m2, y2) != 3) return 0;
    if (*d2 < 1 || *d2 > 31) return 0;
    if (*m2 < 1 || *m2 > 12) return 0;
    if (*y2 < 0 || *y2 > 9999) return 0;
    for (; *q && *q != ' '; q++) { }
    if (*q++ != ' ') return 0;
    if (sscanf(q, "%d:%d:%d", hh, mm, ss) != 3) return 0;
    if (*hh < 0 || *hh > 23) return 0;
    if (*mm < 0 || *mm > 59) return 0;
    if (*ss < 0 || *ss > 59) return 0;
    for (; *q && *q != ' '; q++) { }
    if (*q++ != ' ') return 0;
    {
        int blen = (int)strlen(q);
        for (; blen > 0 && (q[blen-1] == '\r' || q[blen-1] == '\n'); blen--) { }
        if (blen <= 0 || blen >= body_cap) return 0;
        memcpy(body, q, blen);
        body[blen] = '\0';
    }
    return 1;
}
int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stdout, "Usage: tcpclient IP:PORT file.txt\n");
        return 1;
    }
    char hostname[128] = {0};
    int  svc_port = 0;
    {
        char scratch[256];
        strncpy(scratch, argv[1], sizeof(scratch) - 1);
        scratch[sizeof(scratch) - 1] = '\0';
        char *sep = strrchr(scratch, ':');
        if (!sep) { fprintf(stdout, "ERROR: bad address, expected IP:PORT\n"); return 1; }
        *sep = '\0';
        strncpy(hostname, scratch, sizeof(hostname) - 1);
        hostname[sizeof(hostname) - 1] = '\0';
        svc_port = atoi(sep + 1);
        if (svc_port <= 0 || svc_port > 65535) { fprintf(stdout, "ERROR: invalid port\n"); return 1; }
    }
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        fprintf(stdout, "ERROR: WSAStartup failed\n");
        return 1;
    }
    SOCKET hsock = INVALID_SOCKET;
    {
        struct sockaddr_in peer;
        memset(&peer, 0, sizeof(peer));
        peer.sin_family = AF_INET;
        peer.sin_port = htons((unsigned short)svc_port);
        peer.sin_addr.s_addr = inet_addr(hostname);
        if (peer.sin_addr.s_addr == INADDR_NONE) {
            struct addrinfo hints;
            struct addrinfo *list = NULL;
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            if (getaddrinfo(hostname, NULL, &hints, &list) == 0 && list) {
                peer.sin_addr = ((struct sockaddr_in *)list->ai_addr)->sin_addr;
                freeaddrinfo(list);
            } else {
                fprintf(stdout, "ERROR: cannot resolve host: %s\n", hostname);
                WSACleanup();
                return 1;
            }
        }
        {
            int try_idx;
            for (try_idx = 0; try_idx < 10; try_idx++) {
                hsock = socket(AF_INET, SOCK_STREAM, 0);
                if (hsock == INVALID_SOCKET) { wsa_fail("socket"); break; }
                if (connect(hsock, (struct sockaddr *)&peer, sizeof(peer)) == 0) break;
                closesocket(hsock);
                hsock = INVALID_SOCKET;
                fprintf(stdout, "Connection attempt %d failed, retrying in 100ms...\n", try_idx + 1);
                Sleep(100);
            }
        }
        if (hsock == INVALID_SOCKET) {
            fprintf(stdout, "ERROR: could not connect after 10 attempts\n");
            WSACleanup();
            return 1;
        }
        fprintf(stdout, "Connected to %s:%d\n", hostname, svc_port);
    }
    FILE *infile = fopen(argv[2], "r");
    if (!infile) {
        fprintf(stdout, "ERROR: cannot open file: %s\n", argv[2]);
        closesocket(hsock);
        WSACleanup();
        return 1;
    }
    {
        int exit_ok = 0;
        if (tcp_write_full(hsock, "put", 3) != 0) { exit_ok = -1; goto finish; }
        {
            int seq = 0;
            for (; fgets(g_line_buf, sizeof(g_line_buf), infile); ) {
                {
                    int rlen = (int)strlen(g_line_buf);
                    if (rlen > 0 && g_line_buf[rlen - 1] != '\n' && !feof(infile)) {
                        int ch;
                        for (ch = fgetc(infile); ch != EOF && ch != '\n'; ch = fgetc(infile)) { }
                        fprintf(stdout, "Skipping too long line (>%d bytes)\n", INPUT_LINE_CAP - 1);
                        continue;
                    }
                }
                {
                    const char *sp = g_line_buf;
                    for (; *sp == ' ' || *sp == '\t' || *sp == '\r' || *sp == '\n'; sp++) { }
                    if (*sp == '\0') continue;
                }
                {
                    int d1, m1, y1, d2, m2, y2, hh, mi, sc;
                    if (!parse_schedule_row(g_line_buf, &d1, &m1, &y1,
                                                   &d2, &m2, &y2,
                                                   &hh, &mi, &sc,
                                                   g_payload_buf, sizeof(g_payload_buf))) {
                        fprintf(stdout, "Skipping invalid line (first bytes): %.200s...\n", g_line_buf);
                        continue;
                    }
                    {
                        int tlen = (int)strlen(g_payload_buf);
                        int frame_bytes = 16 + tlen;
                        unsigned char *frame = (unsigned char *)malloc(frame_bytes);
                        int off = 0;
                        if (!frame) {
                            fprintf(stdout, "ERROR: out of memory while building packet\n");
                            exit_ok = -1;
                            goto finish;
                        }
                        {
                            unsigned long be_seq = htonl((unsigned long)seq);
                            memcpy(frame + off, &be_seq, 4); off += 4;
                        }
                        frame[off++] = (unsigned char)d1;
                        frame[off++] = (unsigned char)m1;
                        {
                            unsigned short be_y = htons((unsigned short)y1);
                            memcpy(frame + off, &be_y, 2); off += 2;
                        }
                        frame[off++] = (unsigned char)d2;
                        frame[off++] = (unsigned char)m2;
                        {
                            unsigned short be_y = htons((unsigned short)y2);
                            memcpy(frame + off, &be_y, 2); off += 2;
                        }
                        frame[off++] = (unsigned char)hh;
                        frame[off++] = (unsigned char)mi;
                        frame[off++] = (unsigned char)sc;
                        memcpy(frame + off, g_payload_buf, tlen + 1);
                        off += tlen + 1;
                        if (tcp_write_full(hsock, frame, off) != 0) {
                            free(frame);
                            exit_ok = -1;
                            goto finish;
                        }
                        free(frame);
                    }
                    seq++;
                }
            }
            {
                int k;
                for (k = 0; k < seq; k++) {
                    char ack[2];
                    if (tcp_read_full(hsock, ack, 2) != 0) { exit_ok = -1; goto finish; }
                    if (ack[0] != 'o' || ack[1] != 'k') {
                        fprintf(stdout, "ERROR: unexpected response from server\n");
                        exit_ok = -1;
                        goto finish;
                    }
                }
            }
            fprintf(stdout, "Done: %d messages sent and confirmed.\n", seq);
        }
finish:
        fclose(infile);
        closesocket(hsock);
        WSACleanup();
        return (exit_ok == 0) ? 0 : 1;
    }
}
