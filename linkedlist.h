#ifndef LINKEDLIST_HEADER
#define LINKEDLIST_HEADER

#include "client_utils.h"

struct node {
    struct pingpong_context *ctx;
    char* key;
    struct node *next;
};

typedef struct node Node;

struct list {
    Node *head;
};

typedef struct list List;

List * makelist();
void add(List *list, struct pingpong_context *ctx, char* key);
Node* pop(List *list, char* key);
void display(List * list);
void destroy(List * list);
void print_ctx_list(List* list);

#endif
