#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
struct UdpRecord {
    int  d1, m1, y1;
    int  d2, m2, y2;
    int  hh, mm, ss;
    char text[8192];
    int  acked;
};
#define QUEUE_CAP 100000
static UdpRecord g_queue[QUEUE_CAP];
static int g_queued = 0;
static int tokenize_log_line(const char *row,
                             int *d1, int *m1, int *y1,
                             int *d2, int *m2, int *y2,
                             int *hh, int *mm, int *ss,
                             char *body, int body_cap) {
    const char *q = row;
    if (sscanf(q, "%d.%d.%d", d1, m1, y1) != 3) return 0;
    if (*d1<1||*d1>31||*m1<1||*m1>12||*y1<0||*y1>9999) return 0;
    for (; *q && *q != ' '; q++) { }
    if (*q++ != ' ') return 0;
    if (sscanf(q, "%d.%d.%d", d2, m2, y2) != 3) return 0;
    if (*d2<1||*d2>31||*m2<1||*m2>12||*y2<0||*y2>9999) return 0;
    for (; *q && *q != ' '; q++) { }
    if (*q++ != ' ') return 0;
    if (sscanf(q, "%d:%d:%d", hh, mm, ss) != 3) return 0;
    if (*hh<0||*hh>23||*mm<0||*mm>59||*ss<0||*ss>59) return 0;
    for (; *q && *q != ' '; q++) { }
    if (*q++ != ' ') return 0;
    int blen = (int)strlen(q);
    for (; blen > 0 && (q[blen-1] == '\r' || q[blen-1] == '\n'); blen--) { }
    if (blen <= 0 || blen >= body_cap) return 0;
    memcpy(body, q, blen);
    body[blen] = '\0';
    return 1;
}
static int pack_udp_frame(int index, unsigned char *dst, int dst_cap) {
    UdpRecord *rec = &g_queue[index];
    int off = 0;
    if (dst_cap < (int)(16 + strlen(rec->text) + 1)) return -1;
    {
        unsigned int be = htonl((unsigned int)index);
        memcpy(dst + off, &be, 4); off += 4;
    }
    dst[off++] = (unsigned char)rec->d1;
    dst[off++] = (unsigned char)rec->m1;
    {
        unsigned short be_y = htons((unsigned short)rec->y1);
        memcpy(dst + off, &be_y, 2); off += 2;
    }
    dst[off++] = (unsigned char)rec->d2;
    dst[off++] = (unsigned char)rec->m2;
    {
        unsigned short be_y = htons((unsigned short)rec->y2);
        memcpy(dst + off, &be_y, 2); off += 2;
    }
    dst[off++] = (unsigned char)rec->hh;
    dst[off++] = (unsigned char)rec->mm;
    dst[off++] = (unsigned char)rec->ss;
    int tlen = (int)strlen(rec->text);
    memcpy(dst + off, rec->text, tlen + 1);
    off += tlen + 1;
    return off;
}
static int fetch_ack_batch(int fd, int wait_ms) {
    struct timeval tv;
    tv.tv_sec  = wait_ms / 1000;
    tv.tv_usec = (wait_ms % 1000) * 1000;
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    int sel = select(fd + 1, &rfds, NULL, NULL, &tv);
    if (sel <= 0) return 0;
    unsigned char slab[65536];
    int got = recv(fd, slab, sizeof(slab), 0);
    if (got <= 0) return 0;
    int marked = 0;
    int ofs;
    for (ofs = 0; ofs + 4 <= got; ofs += 4) {
        unsigned int id;
        memcpy(&id, slab + ofs, 4);
        id = ntohl(id);
        if ((int)id < g_queued && !g_queue[id].acked) {
            g_queue[id].acked = 1;
            marked++;
        }
    }
    return marked;
}
int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stdout, "Usage: udpclient IP:PORT file.txt\n");
        return 1;
    }
    char host[128] = {0};
    int  port = 0;
    {
        char tmp[256];
        strncpy(tmp, argv[1], sizeof(tmp) - 1);
        char *colon = strrchr(tmp, ':');
        if (!colon) { fprintf(stdout, "ERROR: bad address format\n"); return 1; }
        *colon = '\0';
        strncpy(host, tmp, sizeof(host) - 1);
        port = atoi(colon + 1);
    }
    struct sockaddr_in remote;
    memset(&remote, 0, sizeof(remote));
    remote.sin_family = AF_INET;
    remote.sin_port = htons((unsigned short)port);
    remote.sin_addr.s_addr = inet_addr(host);
    if (remote.sin_addr.s_addr == (unsigned int)-1) {
        struct hostent *he = gethostbyname(host);
        if (!he) { fprintf(stdout, "ERROR: cannot resolve %s\n", host); return 1; }
        memcpy(&remote.sin_addr, he->h_addr, he->h_length);
    }
    int udp = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp < 0) { perror("socket"); return 1; }
    if (connect(udp, (struct sockaddr *)&remote, sizeof(remote)) < 0) {
        perror("connect");
        close(udp);
        return 1;
    }
    FILE *src = fopen(argv[2], "r");
    if (!src) { fprintf(stdout, "ERROR: cannot open %s\n", argv[2]); close(udp); return 1; }
    {
        char row[16384];
        for (; fgets(row, sizeof(row), src) && g_queued < QUEUE_CAP; ) {
            const char *sp = row;
            for (; *sp == ' '||*sp=='\t'||*sp=='\r'||*sp=='\n'; sp++) { }
            if (!*sp) continue;
            UdpRecord *slot = &g_queue[g_queued];
            if (tokenize_log_line(row,
                                  &slot->d1,&slot->m1,&slot->y1,
                                  &slot->d2,&slot->m2,&slot->y2,
                                  &slot->hh,&slot->mm,&slot->ss,
                                  slot->text, sizeof(slot->text))) {
                slot->acked = 0;
                g_queued++;
            }
        }
    }
    fclose(src);
    fprintf(stdout, "Loaded %d messages from file\n", g_queued);
    int need = (g_queued >= 20) ? 20 : g_queued;
    fprintf(stdout, "Need %d confirmations from server\n", need);
    unsigned char wire[65536];
    for (;;) {
        int confirmed = 0;
        int i;
        for (i = 0; i < g_queued; i++)
            if (g_queue[i].acked) confirmed++;
        if (confirmed >= need) {
            fprintf(stdout, "Done! %d messages confirmed by server.\n", confirmed);
            break;
        }
        int burst = 0;
        for (i = 0; i < g_queued; i++) {
            if (g_queue[i].acked) continue;
            int plen = pack_udp_frame(i, wire, sizeof(wire));
            if (plen < 0) continue;
            if (send(udp, wire, plen, 0) < 0) perror("send");
            else burst++;
        }
        if (burst == 0) {
            break;
        }
        int tmp = fetch_ack_batch(udp, 100);
        (void)tmp;
    }
    close(udp);
    return 0;
}
