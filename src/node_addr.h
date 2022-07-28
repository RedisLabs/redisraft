#ifndef REDISRAFT_NODE_ADDR_H
#define REDISRAFT_NODE_ADDR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* Node address specifier. */
typedef struct node_addr {
    uint16_t port;
    char host[256]; /* Hostname or IP address */
} NodeAddr;

/* A singly linked list of NodeAddr elements */
typedef struct NodeAddrListElement {
    NodeAddr addr;
    struct NodeAddrListElement *next;
} NodeAddrListElement;

bool NodeAddrParse(const char *node_addr, size_t node_addr_len, NodeAddr *result);
bool NodeAddrEqual(const NodeAddr *a1, const NodeAddr *a2);
void NodeAddrListAddElement(NodeAddrListElement **head, const NodeAddr *addr);
void NodeAddrListConcat(NodeAddrListElement **head, const NodeAddrListElement *other);
void NodeAddrListFree(NodeAddrListElement *head);

#endif
