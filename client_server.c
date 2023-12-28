/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 * Copyright (c) 2006 Cisco Systems.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "client_utils.h"

#define NUM_CLIENTS (1)
//#define NUM_CLIENTS (2)

enum ibv_mtu pp_mtu_to_enum(int mtu) {
    switch (mtu) {
        case 256:
            return IBV_MTU_256;
        case 512:
            return IBV_MTU_512;
        case 1024:
            return IBV_MTU_1024;
        case 2048:
            return IBV_MTU_2048;
        case 4096:
            return IBV_MTU_4096;
        default:
            return -1;
    }
}

uint16_t pp_get_local_lid(struct ibv_context *context, int port) {
    struct ibv_port_attr attr;

    if (ibv_query_port(context, port, &attr))
        return 0;

    return attr.lid;
}

int pp_get_port_info(struct ibv_context *context, int port,
                     struct ibv_port_attr *attr) {
    return ibv_query_port(context, port, attr);
}

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid) {
    char tmp[9];
    uint32_t v32;
    int i;

    for (tmp[8] = 0, i = 0; i < 4; ++i) {
        memcpy(tmp, wgid + i * 8, 8);
        sscanf(tmp, "%x", &v32);
        *(uint32_t * )(&gid->raw[i * 4]) = ntohl(v32);
    }
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[]) {
    int i;

    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t * )(gid->raw + i * 4)));
}

static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
                          enum ibv_mtu mtu, int sl,
                          struct pingpong_dest *dest, int sgid_idx) {
    struct ibv_qp_attr attr = {
            .qp_state        = IBV_QPS_RTR,
            .path_mtu        = mtu,
            .dest_qp_num        = dest->qpn,
            .rq_psn            = dest->psn,
            .max_dest_rd_atomic    = 1,
            .min_rnr_timer        = 12,
            .ah_attr        = {
                    .is_global    = 0,
                    .dlid        = dest->lid,
                    .sl        = sl,
                    .src_path_bits    = 0,
                    .port_num    = port
            }
    };

    if (dest->gid.global.interface_id) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid;
        attr.ah_attr.grh.sgid_index = sgid_idx;
    }
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_AV |
                      IBV_QP_PATH_MTU |
                      IBV_QP_DEST_QPN |
                      IBV_QP_RQ_PSN |
                      IBV_QP_MAX_DEST_RD_ATOMIC |
                      IBV_QP_MIN_RNR_TIMER)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = my_psn;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_TIMEOUT |
                      IBV_QP_RETRY_CNT |
                      IBV_QP_RNR_RETRY |
                      IBV_QP_SQ_PSN |
                      IBV_QP_MAX_QP_RD_ATOMIC)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port,
                                                 const struct pingpong_dest *my_dest) {
    struct addrinfo *res, *t;
    struct addrinfo hints = {
            .ai_family   = AF_INET,
            .ai_socktype = SOCK_STREAM
    };

    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(servername, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(sockfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        goto out;
    }

    if (read(sockfd, msg, sizeof msg) != sizeof msg) {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }

    write(sockfd, "done", sizeof "done");

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    out:
    close(sockfd);
    return rem_dest;
}

static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx,
                                                 int ib_port, enum ibv_mtu mtu,
                                                 int port, int sl,
                                                 const struct pingpong_dest *my_dest,
                                                 int sgid_idx) {
    struct addrinfo *res, *t;
    struct addrinfo hints = {
            .ai_flags    = AI_PASSIVE,
            .ai_family   = AF_INET,
            .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1, connfd;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(NULL, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            n = 1;

            setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

            if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(sockfd, 1);
    connfd = accept(sockfd, NULL, 0);
    close(sockfd);
    if (connfd < 0) {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    }

    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg) {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
        goto out;
    }

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx)) {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }


    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    read(connfd, msg, sizeof msg);

    out:
    close(connfd);
    return rem_dest;
}

#include <sys/param.h>

static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,
                                            int rx_depth, int tx_depth, int port,
                                            int use_event, int is_server) {
    struct pingpong_context *ctx;

    ctx = calloc(1, sizeof *ctx);
    if (!ctx)
        return NULL;

    ctx->size = size;
    ctx->rx_depth = rx_depth;
    ctx->routs = rx_depth;

    ctx->buf = malloc(roundup(size, page_size));
    if (!ctx->buf) {
        fprintf(stderr, "Couldn't allocate work buf.\n");
        return NULL;
    }

    memset(ctx->buf, 0x7b + is_server, size);

    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context) {
        fprintf(stderr, "Couldn't get context for %s\n",
                ibv_get_device_name(ib_dev));
        return NULL;
    }

    if (use_event) {
        ctx->channel = ibv_create_comp_channel(ctx->context);
        if (!ctx->channel) {
            fprintf(stderr, "Couldn't create completion channel\n");
            return NULL;
        }
    } else
        ctx->channel = NULL;

    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        return NULL;
    }

    ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, IBV_ACCESS_LOCAL_WRITE);
    if (!ctx->mr) {
        fprintf(stderr, "Couldn't register MR\n");
        return NULL;
    }

    ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL,
                            ctx->channel, 0);
    if (!ctx->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        return NULL;
    }

    {
        struct ibv_qp_init_attr attr = {
                .send_cq = ctx->cq,
                .recv_cq = ctx->cq,
                .cap     = {
                        .max_send_wr  = tx_depth,
                        .max_recv_wr  = rx_depth,
                        .max_send_sge = 1,
                        .max_recv_sge = 1
                },
                .qp_type = IBV_QPT_RC
        };

        ctx->qp = ibv_create_qp(ctx->pd, &attr);
        if (!ctx->qp) {
            fprintf(stderr, "Couldn't create QP\n");
            return NULL;
        }
    }

    {
        struct ibv_qp_attr attr = {
                .qp_state        = IBV_QPS_INIT,
                .pkey_index      = 0,
                .port_num        = port,
                .qp_access_flags = IBV_ACCESS_REMOTE_READ |
                                   IBV_ACCESS_REMOTE_WRITE
        };

        if (ibv_modify_qp(ctx->qp, &attr,
                          IBV_QP_STATE |
                          IBV_QP_PKEY_INDEX |
                          IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            return NULL;
        }
    }

    return ctx;
}

int pp_close_ctx(struct pingpong_context *ctx) {
    if (ibv_destroy_qp(ctx->qp)) {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(ctx->cq)) {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }

    if (ibv_dereg_mr(ctx->mr)) {
        fprintf(stderr, "Couldn't deregister MR\n");
        return 1;
    }

    if (ibv_dealloc_pd(ctx->pd)) {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (ctx->channel) {
        if (ibv_destroy_comp_channel(ctx->channel)) {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(ctx->context)) {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    free(ctx->buf);
    free(ctx);

    return 0;
}

static int pp_post_recv(struct pingpong_context *ctx, int n) {
    struct ibv_sge list = {
            .addr    = (uintptr_t) ctx->buf,
            .length = ctx->size,
            .lkey    = ctx->mr->lkey
    };
    struct ibv_recv_wr wr = {
            .wr_id        = PINGPONG_RECV_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .next       = NULL
    };
    struct ibv_recv_wr *bad_wr;
    int i;

    for (i = 0; i < n; ++i)
        if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
            break;

    return i;
}

static int pp_post_send(struct pingpong_context *ctx) {
    struct ibv_sge list = {
            .addr    = (uint64_t) ctx->buf,
            .length = ctx->size,
            .lkey    = ctx->mr->lkey
    };

    int ibv_send_inline = 0;
    if (ctx->size < SWITCH_INLINE)
        ibv_send_inline = IBV_SEND_INLINE;

    struct ibv_send_wr *bad_wr, wr = {
            .wr_id        = PINGPONG_SEND_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .opcode     = IBV_WR_SEND,
            .send_flags = IBV_SEND_SIGNALED | ibv_send_inline,
            .next       = NULL
    };

    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

//_______________________________________________________________________________________________________________
static int pp_rdma_read(struct ibv_qp *qp, struct ibv_mr *mr, uint64_t remote_addr, uint32_t rkey) {
    struct ibv_sge list = {
            .addr    = (uint64_t) mr->addr,
            .length = mr->length,
            .lkey    = mr->lkey
    };

    struct ibv_send_wr *bad_wr, wr = {
            .wr_id        = PINGPONG_SEND_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .opcode     = IBV_WR_RDMA_READ,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL
    };
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    return ibv_post_send(qp, &wr, &bad_wr);
}

static int pp_rdma_write(struct ibv_qp *qp, struct ibv_mr *mr, uint64_t remote_addr, uint32_t rkey) {
    struct ibv_sge list = {
            .addr    = (uint64_t) mr->addr,
            .length = mr->length,
            .lkey    = mr->lkey
    };

    struct ibv_send_wr *bad_wr, wr = {
            .wr_id        = PINGPONG_SEND_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .opcode     = IBV_WR_RDMA_WRITE,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL
    };

    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    return ibv_post_send(qp, &wr, &bad_wr);
}
//_______________________________________________________________________________________________________________

int pp_wait_completions(struct pingpong_context *ctx, int iters) {
    int rcnt = 0, scnt = 0;
    while (rcnt + scnt < iters) {
        struct ibv_wc wc[WC_BATCH];
        int ne, i;

        do {
            ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc);
            if (ne < 0) {
                fprintf(stderr, "poll CQ failed %d\n", ne);
                return 1;
            }

        } while (ne < 1);

        for (i = 0; i < ne; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                        ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int) wc[i].wr_id);
                return 1;
            }

            switch ((int) wc[i].wr_id) {
                case PINGPONG_SEND_WRID:
                    ++scnt;
                    break;

                case PINGPONG_RECV_WRID:
                    if (--ctx->routs <= 10) {
                        ctx->routs += pp_post_recv(ctx, ctx->rx_depth - ctx->routs);
                        if (ctx->routs < ctx->rx_depth) {
                            fprintf(stderr,
                                    "Couldn't post receive (%d)\n",
                                    ctx->routs);
                            return 1;
                        }
                    }
                    ++rcnt;
                    break;

                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n",
                            (int) wc[i].wr_id);
                    return 1;
            }
        }

    }
    return 0;
}

int pp_wait_completions_multi(struct pingpong_context *ctx_list[], int iters) {

    int rcnt = 0, scnt = 0;
    int k = 0;
    while (rcnt + scnt < iters) {
        struct ibv_wc wc[WC_BATCH];

        int ne, i;
        do {
            for (int j = 0; j < NUM_CLIENTS; j++) {
                ne = ibv_poll_cq(ctx_list[j]->cq, WC_BATCH, wc);
                if (ne < 0) {
                    fprintf(stderr, "poll CQ failed %d\n", ne);
                    return -1;
                }
                if (ne >= 1) {
                    k = j;
                    break;
                }
            }
        } while (ne < 1);
        printf("in WQ number k: %d\n", k);
        struct pingpong_context *ctx = ctx_list[k];
        for (i = 0; i < ne; ++i) {

            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                        ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int) wc[i].wr_id);
                return -1;
            }

            switch ((int) wc[i].wr_id) {
                case PINGPONG_SEND_WRID:
                    ++scnt;
                    break;

                case PINGPONG_RECV_WRID:
                    if (--ctx->routs <= 10) {
                        ctx->routs += pp_post_recv(ctx,
                                                   ctx->rx_depth - ctx->routs);
                        if (ctx->routs < ctx->rx_depth) {
                            fprintf(stderr,
                                    "Couldn't post receive (%d)\n",
                                    ctx->routs);
                            return -1;
                        }
                    }
                    ++rcnt;
                    break;

                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n",
                            (int) wc[i].wr_id);
                    return -1;
            }
        }

    }
    return k;
}

static void usage(const char *argv0) {
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
    printf("  -l, --sl=<sl>          service level value\n");
    printf("  -e, --events           sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
}

double calculate_throughput(struct timeval *start_time, struct timeval *end_time, size_t data_size) {
    double total_time = (double) (end_time->tv_sec * 1000000 - start_time->tv_sec * 1000000)
                        + (end_time->tv_usec - start_time->tv_usec);
    total_time = total_time / CLOCKS_PER_SEC;
    double throughput = ((((double) data_size * (double) REPEAT) / 1000000) / total_time);
    return throughput;
}

/**int client_send_data_cycle(struct pingpong_context *ctx, int iters, int tx_depth) {
    // send
//  pp_wait_completions (ctx, 1); //send
    printf("********* in client_send_data_cycle *********\n");
    printf("buf : %s \n", (char *) ctx->buf);
    if (pp_post_send(ctx)) {
        fprintf(stderr, "Client couldn't post send\n");
        return 1;
    }
    pp_wait_completions(ctx, iters); //send //TODO: 2 for get??

//  for (int j = 0; j < iters; ++j)
//    {
//      if ((j != 0) && (j % tx_depth == 0)) {
//          pp_wait_completions(ctx, tx_depth);
//        }
//      if (pp_post_send(ctx)) {
//          fprintf(stderr, "Client couldn't post send\n");
//          return 1;
//        }
//    }
////  pp_wait_completions(ctx, 1);
//
//  // ack
//  pp_wait_completions(ctx, 1);

    printf("********* out client_send_data_cycle *********\n");
    return 0;
}**/

int client_set_rendezvous_cycle(struct pingpong_context *ctx, const char *key, const char *value) {

    ctx->size = 4 * 1024;

    pp_wait_completions(ctx, 1); // receive remote address and r key for the new memory region in server side
    // buf after server replay = remote_addr|rkey
    uint64_t remote_addr;               // Remote start address (the message size is according to the S/G entries)
    uint32_t rkey;                      // rkey of Memory Region that is associated with remote memory
    sscanf(ctx->buf, "%p|%u", &remote_addr, &rkey);
    printf("-------  remote_addr : %p \t rkey : %u ---------\n", remote_addr, rkey);

    struct ibv_mr *mr = ibv_reg_mr(ctx->pd, value, strlen(value),
                                   IBV_ACCESS_LOCAL_WRITE
                                   | IBV_ACCESS_REMOTE_READ
                                   | IBV_ACCESS_REMOTE_WRITE);

    if (pp_rdma_write(ctx->qp, mr, remote_addr, rkey)) {
        fprintf(stderr, "Server couldn't post send - line 723\n");
        return 1;
    }

    pp_wait_completions(ctx, 1); // write

    printf("-------  after pp_wait_completions ---------\n");

    ibv_dereg_mr(mr);
    // Send ACK
    sprintf(ctx->buf, "%s|%s", RNDZ_FIN, key); // TODO: use this key to release STATE in DB
    ctx->size = (int) strlen(ctx->buf) + 1;
    printf("buf: %s, size: %d\n", (char *) ctx->buf, ctx->size);
    if (pp_post_send(ctx)) {
        fprintf(stderr, "Server couldn't post send\n");
        return 1;
    }
    pp_wait_completions(ctx, 1); //send
}

int server_recv_data_cycle(struct pingpong_context **ctx_list, struct map *db, List *key_ctx_list) {
    printf("********* in server_recv_data_cycle *********\n");
    print_ctx_list(key_ctx_list);
    int ctx_index = pp_wait_completions_multi(ctx_list, 1);
    printf("ctx_index %d\n", ctx_index);
    struct pingpong_context *ctx = ctx_list[ctx_index];
    printf("Buffer received from client: %s\n", (char *) ctx->buf);


    char *data, *new_data, *operation_type, *protocol_type, *key_buf;
    data = (char *) malloc(sizeof(char) * (ctx->size + 1));
    strcpy(data, ctx->buf);
    printf(" buf : %s\n", data);
    operation_type = strtok(data, "|");
    if (strcmp(operation_type, SET_OP_FLAG) == 0)
        protocol_type = strtok(NULL, "|"); /** get the protocol type **/
    key_buf = strtok(NULL, "|"); /** get the key **/

    struct Cell *cell = map_get(db, key_buf);
    // op|protocol|key|{value || strlen(value)} OR op|key
    // before choosing operation_type
    if (strcmp(operation_type, RNDZ_FIN) == 0) /** FIN_ACK **/ {
        printf("********* in FIN_ACK *********\n");

        // FIN_ACK| key
        struct Cell *cell = map_get(db, key_buf);
        printf("before cell_state %d\n", cell->state);
        if (cell->state == SET) {
            cell->state = READY;
        } else if (cell->state == GET) {
            printf("cell->usage_count  :  %d\n", cell->usage_count);
            cell->usage_count--;
            if (cell->usage_count == 0)
                cell->state = READY;
        }

        printf("after cell_state %d\n", cell->state);
        print_ctx_list(key_ctx_list);

        Node *next_job = pop(key_ctx_list, key_buf);
        if (next_job != NULL) {
            printf("next job %s\n", (char *) next_job->ctx->buf);
            memcpy(ctx, next_job->ctx, sizeof (*ctx));
            sprintf(ctx->buf, "%s", next_job->ctx->buf);

            new_data = (char *) malloc(sizeof(char) * (ctx->size + 1));
            strcpy(new_data, ctx->buf);
            printf("new_data : %s\n", new_data);
            operation_type = strtok(new_data, "|");
            if (strcmp(operation_type, SET_OP_FLAG) == 0) {
                protocol_type = strtok(NULL, "|");
            } /** get the protocol type **/
            key_buf = strtok(NULL, "|"); /** get the key **/
        } else {
            printf("else in FIN_ACK\n");
            return EXIT_SUCCESS;
        }
    }

    if (strcmp(operation_type, SET_OP_FLAG) == 0) /** SET **/ {
        if (cell != NULL && cell->state != READY) {
            add(key_ctx_list, ctx, key_buf);
            return EXIT_SUCCESS;
        }

        // ------Eager------ operation|protocol|key|value
        // can go inside only if state is READY OR GET
        printf("protocol_type : %s\n", protocol_type);
        if (strcmp(protocol_type, EAGER_FLAG) == 0) /** Eager **/ {
            printf("********* in set Eager *********\n");
            char *value_buf = strtok(NULL, "|");
            char *value_buf_malloc = (char *) malloc(
                    strlen(value_buf) * sizeof(char) + 1);
            strcpy(value_buf_malloc, value_buf);
            printf("value - %s \t value_buf_malloc - %s \n", value_buf, value_buf_malloc);
            map_set(db, key_buf, value_buf_malloc, strlen(value_buf));

            if (pp_post_send(ctx)) /** send to client ack **/  {
                fprintf(stderr, "Server couldn't post send\n");
                return 1;
            }
            pp_wait_completions(ctx, 1); /** finish sending the ack **/
        }

            // ------Rendezvous------ operation|protocol|key|strlen(value)
        else if (strcmp(protocol_type, RENDEZVOUS_FLAG) == 0) /** Rendezvous **/ {
            printf("********* in set Rendezvous *********\n");

            char *value_len = strtok(NULL, "|");

            int value_len_int = (int) strtol(value_len, NULL, 10);
            printf("%s | %s | %s | %d\n", operation_type, protocol_type, key_buf, value_len_int);

            char *value_buf_malloc = (char *) malloc(value_len_int * sizeof(char) + 1);
            map_set(db, key_buf, value_buf_malloc, value_len_int);
            struct Cell *c = map_get(db, key_buf);
            c->state = SET;

            c->mr = ibv_reg_mr(ctx->pd, (c->value), value_len_int, IBV_ACCESS_LOCAL_WRITE
                                                                   | IBV_ACCESS_REMOTE_READ
                                                                   | IBV_ACCESS_REMOTE_WRITE);

            sprintf(ctx->buf, "%p|%u", c->value, c->mr->rkey);
            printf("value_buf_malloc = %p  \t  c->value = %p \n", value_buf_malloc, c->value);
            printf("Buffer address and rkey for client: c->value %p    rkey : %u\n", c->value, c->mr->rkey);
            ctx->size = strlen(ctx->buf) + 1;
            if (pp_post_send(ctx)) {
                fprintf(stderr, "Server couldn't post send\n");
                return 1;
            }

            pp_wait_completions(ctx, 1);
        }
    }

        // ------Eager and Rendezvous------ operation|key
    else if (strcmp(operation_type, GET_OP_FLAG) == 0) /** GET **/{
        printf("********* in get *********\n");
        if (cell == NULL)
          {
            printf("no cell for some reason\n");
            return EXIT_FAILURE;
          }
        else if (cell->state == SET) {
            // add to future work queue
            add(key_ctx_list, ctx, key_buf);
            return EXIT_SUCCESS;
        }

        if (cell->value_len <= 4 * KB) /** GET - Eager **/ {
            char *value = (char *) (cell->value);
            ctx->size = cell->value_len + 1 + 2;
            sprintf(ctx->buf, "%s|%s", EAGER_FLAG, value);
            if (pp_post_send(ctx)) {
                fprintf(stderr, "Server couldn't post send\n");
                return EXIT_FAILURE;
            }
            pp_wait_completions(ctx, 1);

        }
        else /** Get - Rendezvous **/ {
            cell->state = GET;
            cell->usage_count++;
            // operation| protocol|address|rkey|value_len    cell->value_len -> (long or int)
            sprintf(ctx->buf, "%s|%p|%u|%d", RENDEZVOUS_FLAG, cell->value, cell->mr->rkey,
                    cell->value_len); // @addr-rkey-length
            printf("Data to client Rendezvous: %s\n", (char *) ctx->buf);
            ctx->size = (int) strlen(ctx->buf) + 1;
            if (pp_post_send(ctx)) {
                fprintf(stderr, "Server couldn't post send\n");
                return EXIT_FAILURE;
            }
            pp_wait_completions(ctx, 1);
            printf("********* out get rndz *********\n");
        }

    }

    else if(strcmp(operation_type, RNDZ_FIN) != 0) {
        printf("ERROR- no such operation was found\n");
        return EXIT_FAILURE;
    }

    printf("********* out server  *********\n");
    return EXIT_SUCCESS;
}

struct KV_Handle {
    struct pingpong_context *ctx;
} KV_Handle;

// ____________________________________________________________________________


int bw_init(char *servername, struct pingpong_context **ctx) {
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct pingpong_dest my_dest;
    struct pingpong_dest *rem_dest;
    char *ib_devname = NULL;
    int port = 12346;
    int ib_port = 1;
    enum ibv_mtu mtu = IBV_MTU_2048;
    int rx_depth = 100;
    int tx_depth = WC_BATCH;
    int iters = 1000;
    int use_event = 0;
    int size = 4 * 1024;
    int sl = 0;
    int gidx = -1;
    char gid[33];

    srand48(getpid() * time(NULL));

    page_size = sysconf(_SC_PAGESIZE);

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!ib_devname) {
        ib_dev = *dev_list;
        if (!ib_dev) {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    } else {
        int i;
        for (i = 0; dev_list[i]; ++i) {
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname)) {
                break;
            }
        }
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "IB device %s not found\n", ib_devname);
            return 1;
        }
    }

    (*ctx) = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);
    if (!(*ctx)) {
        return 1;
    }

    (*ctx)->routs = pp_post_recv((*ctx), (*ctx)->rx_depth);
    if ((*ctx)->routs < (*ctx)->rx_depth) {
        fprintf(stderr, "Couldn't post receive (%d)\n", (*ctx)->routs);
        return 1;
    }

    if (use_event) {
        if (ibv_req_notify_cq((*ctx)->cq, 0)) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }
    }

    if (pp_get_port_info((*ctx)->context, ib_port, &(*ctx)->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = (*ctx)->portinfo.lid;
    if ((*ctx)->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0) {
        if (ibv_query_gid((*ctx)->context, ib_port, gidx, &my_dest.gid)) {
            fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
            return 1;
        }
    } else {
        memset(&my_dest.gid, 0, sizeof my_dest.gid);
    }

    my_dest.qpn = (*ctx)->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);
    if (!servername)
        rem_dest = pp_server_exch_dest((*ctx), ib_port, mtu, port, sl, &my_dest, gidx);
    else {
        rem_dest = pp_client_exch_dest(servername, port, &my_dest);
        if (!rem_dest)
            return 1;
        inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
        if (pp_connect_ctx((*ctx), ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
            return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;

}

/** client functions **/
/*Connect to server*/
int kv_open(char *servername, void **kv_handle) {
    *kv_handle = malloc(sizeof(struct KV_Handle *));
    struct pingpong_context *ctx;
    if (bw_init(servername, &ctx) == EXIT_FAILURE) {
        printf("Could not initialize bw");
        exit(EXIT_FAILURE);
    }
    struct KV_Handle **kvh = (struct KV_Handle **) kv_handle;
    (*kvh)->ctx = ctx;
    return EXIT_SUCCESS;
}

int kv_set(void *kv_handle, const char *key, const char *value) {
    printf("********* in set *********\n");
    // operation|protocol|key|value
    struct KV_Handle *kv_h = (struct KV_Handle *) kv_handle;

    // build ctx.buf
    int key_len = strlen(key);
    int value_len = strlen(value);
    int msg_len = 5 + key_len + value_len + 1;
    printf("key_len : %d \t value_len : %d \t msg_len : %d \n", key_len, value_len, msg_len);

    if (msg_len <= 4 * KB) /** Eager - operation|protocol|key|value **/{

        printf("Eager \n");
        // build the msg to send the server
        sprintf(kv_h->ctx->buf, "%s|%s|%s|%s", SET_OP_FLAG, EAGER_FLAG, key, value);
        kv_h->ctx->size = (int) msg_len;
    }

    else /** Rendezvous - operation|protocol|key|str(value_len) **/ {
        printf("Rendezvous\n");
        printf("%s|%s|%s|%d\n", SET_OP_FLAG, RENDEZVOUS_FLAG, key, value_len);

        sprintf(kv_h->ctx->buf, "%s|%s|%s|%d", SET_OP_FLAG, RENDEZVOUS_FLAG, key, value_len);
        kv_h->ctx->size = (int) strlen(kv_h->ctx->buf) + 1;
    }

    // sending a msg to server
    if (pp_post_send(kv_h->ctx)) {
        fprintf(stderr, "Client couldn't post send\n");
        return 1;
    }
    printf("send buf-msg to server\n");

    // wait for send to finish
    pp_wait_completions(kv_h->ctx, 1);

    if (msg_len > 4 * KB) {
        // protocol|operation|key|str(value_len)
        if (client_set_rendezvous_cycle(kv_h->ctx, key, value)) {
            return EXIT_FAILURE;
        }
    } else {
        pp_wait_completions(kv_h->ctx, 1);
    }

    printf("********* out set *********\n");
    return EXIT_SUCCESS;
}

int kv_get(void *kv_handle, const char *key, char **value) {
    printf("********* in get *********\n");
    // operation|key
    struct KV_Handle *kv_h = (struct KV_Handle *) kv_handle;
    size_t key_len = strlen(key);
    size_t msg_len = 2 + key_len + 1;

    // build the msg to sent the server
    sprintf(kv_h->ctx->buf, "%s|%s", GET_OP_FLAG, key);
//    printf("test");
    printf("ctx->buf : %s\n", kv_h->ctx->buf);
    kv_h->ctx->size = (int) msg_len;
    if (pp_post_send(kv_h->ctx)) {
        fprintf(stderr, "Client couldn't post send\n");
        return 1;
    }
    pp_wait_completions(kv_h->ctx, 2);
    printf("client got back: %s\n", kv_h->ctx->buf);

    // kv_h->ctx->buf = protocol|value or protocol|address|rkey|value_len
    char *buff = malloc(sizeof(char) * strlen(kv_h->ctx->buf) + 1);
    sprintf(buff, "%s", kv_h->ctx->buf);
    char *protocol_type = strtok(buff, "|");

    if (strcmp(protocol_type, EAGER_FLAG) == 0) /** Eager **/{
        // kv_h->ctx->buf = protocol|value
        char *value_buf = strtok(NULL, "|");

//        sscanf (kv_h->ctx->buf + 2, "%s", value_buf);
        printf("value_buf = %s\n", value_buf);
        sprintf(*value, "%s", value_buf);
        printf("client got back: %s\n", kv_h->ctx->buf);
    }
    else if (strcmp(protocol_type, RENDEZVOUS_FLAG) == 0) /** Rendezvous **/ {
        // kv_h->ctx->buf = protocol|address|rkey|value_len
        printf("Rendezvous\n");
        uint64_t remote_addr; //Remote start address (the message size is according to the S/G entries)
        uint32_t rkey;        //rkey of Memory Region that is associated with remote memory
        int value_len;

        sscanf(kv_h->ctx->buf + 2, "%p|%u|%d", &remote_addr, &rkey, &value_len);
        printf("%s|%p|%u|%d\n", RENDEZVOUS_FLAG, remote_addr, rkey, value_len);
        *value = (char *) malloc(value_len);

        if (kv_h->ctx->pd == NULL)
          printf("pd is null\n");
        struct ibv_mr *mr = ibv_reg_mr(kv_h->ctx->pd, *value, value_len,
                                       IBV_ACCESS_LOCAL_WRITE
                                       | IBV_ACCESS_REMOTE_READ
                                       | IBV_ACCESS_REMOTE_WRITE);
        if (mr == NULL)
          printf("mr is null\n");

        if (pp_rdma_read(kv_h->ctx->qp, mr, remote_addr, rkey)) {
            fprintf(stderr, "Client couldn't post send in Rendezvous \n");
            return 1;
        }

        pp_wait_completions(kv_h->ctx, 1); // wait for the rdma read to finish
        ibv_dereg_mr(mr);
        // FIN_GET_ACK
        sprintf(kv_h->ctx->buf, "%s|%s", RNDZ_FIN, key);
        if (pp_post_send(kv_h->ctx)) {
            fprintf(stderr, "Client couldn't post send\n");
            return 1;
        }
        pp_wait_completions(kv_h->ctx, 1); //send FIN_GET_ACK

    }

    printf("********* out get *********\n");
    return EXIT_SUCCESS;
}

void kv_release(char *value) {
    free(value);
}

int kv_close(void *kv_handle) {
    pp_close_ctx((((struct KV_Handle *) kv_handle)->ctx));
    printf("Client Done.\n");
    return 1;
}

// ____________________________________________________________________________



int main(int argc, char *argv[]) {

    char* servername = NULL;
    struct KV_Handle kv_hand;
    struct KV_Handle* kv_handle = &kv_hand;

    struct KV_Handle kv_hand2;
    struct KV_Handle* kv_handle2 = &kv_hand2;

    int is_sender = 0;

    printf("argc: %d\n", argc);

    if (argc == 2)
        servername = strdup(argv[1]);

  if (argc == 3)
    {
      servername = strdup(argv[1]);
      if (strcmp(argv[2], "0") == 0)
        is_sender = 1;
    }

    if (servername) /** Client **/ {
        void *kv_handle;
        kv_open(servername, &kv_handle);

        sleep(1);

        if (is_sender){
            char value2[9]; /** set Eager **/
            memset(value2, '#', 9);
            value2[8] = '\0';
            kv_set(kv_handle, "Hi\0", value2);

            sleep(1);

            printf("Enter 4 Set \n"); /** set Rendezvous **/
            char value0[5000];
            memset(value0, '!', 5000);
            value0[4999] = '\0';
            kv_set(kv_handle, "Hello", value0);

            sleep(1);

            char *get2 = malloc(9);  /** get Eager **/
            kv_get(kv_handle, "Hi\0", &get2);
            printf("Hi - %s \n", get2);
        }

        if (!is_sender) {
            char value2[9]; /** set Eager **/
            memset(value2, ';', 9);
            value2[8] = '\0';
            kv_set(kv_handle, "Test2\0", value2);

            sleep(1);

            char value1[5000]; /** set Rendezvous **/
            memset(value1, '1', 5000);
            value1[4999] = '\0';
            kv_set(kv_handle, "Hello", value1);

            sleep(1);

            char *get2 = malloc(9);  /** get Eager **/
            kv_get(kv_handle, "Test2\0", &get2);
            printf("Test2 - %s \n", get2);

            sleep(1);

            char *get4 = malloc(5000); /** get Rendezvous **/
            kv_get(kv_handle, "Hello\0", &get4);
            printf("Hello - %s \n", get4);
        }
    }

    else /** Server **/{
        // create db
        kv_open(servername, (void **) &kv_handle);
//        kv_open(servername, (void **) &kv_handle2);
//        struct pingpong_context *ctx_list[NUM_CLIENTS] = {(kv_handle->ctx), (kv_handle2->ctx)};
        struct pingpong_context *ctx_list[NUM_CLIENTS] = {(kv_handle->ctx)};

        map db = map_create();
        List *key_ctx_list = makelist();

        while (1) {
            printf("\n********* started while *********\n");
            server_recv_data_cycle(ctx_list, db, key_ctx_list);
            printf("********* finish a while iteration *********\n");
        }
    }

    return 0;
}