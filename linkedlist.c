#include <stdio.h>
#include <stdlib.h>
#include "linkedlist.h"

Node *createnode(struct pingpong_context *ctx, char* key);

Node *createnode(struct pingpong_context *ctx, char* key) {
    Node *newNode = malloc(sizeof(Node));
    if (!newNode) {
        return NULL;
    }
    newNode->ctx = ctx;
    newNode->key = key;
    newNode->next = NULL;
    return newNode;
}

List *makelist() {
    List *list = malloc(sizeof(List));
    if (!list) {
        return NULL;
    }
    list->head = NULL;
    return list;
}

void display(List *list) {
    Node *current = list->head;
    if (list->head == NULL)
        return;
    printf("linked-list: ");
    for (; current != NULL; current = current->next) {
        printf("%s->", current->key);
    }
    printf("\n");
}

void add(List *list, struct pingpong_context *ctx, char* key) {
    Node *current = NULL;
    if (list->head == NULL) {
        list->head = createnode(ctx, key);
    } else {
        current = list->head;
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = createnode(ctx, key);
    }
}

Node* pop(List *list, char* key) {
    printf("\nkey = %s\n",key );
    Node *current = list->head;
    if(current == NULL)
        return NULL;
    printf("current: %s\n", (char*)current->ctx->buf);
    printf("current_key: %s\n", (char*)current->key);

    Node *previous = current;
    while (current != NULL) {
        printf("current_key: %s\n", (char*)current->key);
        printf("\nkey = %s\n",key );
        if (strcmp(current->key, key)==0) {
            previous->next = current->next;
            if (current == list->head)
                list->head = current->next;

            return current;
        }
        previous = current;
        current = current->next;
    }
    return NULL;
}


void print_ctx_list(List* list){
    printf("list:  \n");
    Node *current = list->head;
    Node *previous = current;
    while (current != NULL) {
        printf("ctx_list_buf: %s\n", (char *)current->ctx->buf);
        previous = current;
        current = current->next;
    }

}

void destroy(List *list) {
    Node *current = list->head;
    Node *next = current;
    while (current != NULL) {
        next = current->next;
        free(current);
        current = next;
    }
    free(list);
}
