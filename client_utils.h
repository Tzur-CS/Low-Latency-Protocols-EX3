#ifndef EX_3_CLIENT_UTILS_H
#define EX_3_CLIENT_UTILS_H


#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <infiniband/verbs.h>
#include "map.h"
#include "linkedlist.h"

#define WC_BATCH (10)
#define KB (1024)
#define WARM 300
#define MB 1048576
#define REPEAT 1500
#define SWITCH_INLINE 32
//#define NUM_CLIENTS1 (1)
//#define NUM_CLIENTS2 (2)
//#define NUM_CLIENTS (2)
#define SET_OP_FLAG "0"
#define GET_OP_FLAG "1"
#define EAGER_FLAG "0"
#define RENDEZVOUS_FLAG "1"
#define RNDZ_FIN "2"


enum {
    PINGPONG_RECV_WRID = 1,
    PINGPONG_SEND_WRID = 2,
};

static int page_size;


struct pingpong_context {
    struct ibv_context *context;
    struct ibv_comp_channel *channel;
    struct ibv_pd *pd;
    struct ibv_mr *mr;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    void *buf;
    int size;
    int rx_depth;
    int routs;
    struct ibv_port_attr portinfo;
};

struct pingpong_dest {
    int lid;
    int qpn;
    int psn;
    union ibv_gid gid;
};



#endif //EX_3_CLIENT_UTILS_H
