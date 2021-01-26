/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <string.h>
#include "redisraft.h"

/* Attempt to parse a node addrss in the form of <addr>:<port>
 * and populate the result NodeAddr. Returns true if successful.
 */
bool NodeAddrParse(const char *node_addr, size_t node_addr_len, NodeAddr *result)
{
    char buf[32] = { 0 };
    char *endptr;
    unsigned long l;

    /* Split */
    const char *colon = node_addr + node_addr_len;
    while (colon > node_addr && *colon != ':') {
        colon--;
    }
    if (*colon != ':') {
        return false;
    }

    /* Get port */
    int portlen = node_addr_len - (colon + 1 - node_addr);
    if (portlen >= sizeof(buf) || portlen < 1) {
        return false;
    }

    strncpy(buf, colon + 1, portlen);
    l = strtoul(buf, &endptr, 10);
    if (*endptr != '\0' || l < 1 || l > 65535) {
        return false;
    }
    result->port = l;

    /* Get addr */
    int addrlen = colon - node_addr;
    if (addrlen >= sizeof(result->host)) {
        addrlen = sizeof(result->host)-1;
    }
    memcpy(result->host, node_addr, addrlen);
    result->host[addrlen] = '\0';

    return true;
}

/* Compare two NodeAddr sructs */
bool NodeAddrEqual(const NodeAddr *a1, const NodeAddr *a2)
{
    return (a1->port == a2->port && !strcmp(a1->host, a2->host));
}

/* Add a NodeAddrListElement to a chain of elements.  If an existing element with the same
 * address already exists, nothing is done.  The addr pointer provided is copied into newly
 * allocated memory, caller should free addr if necessary.
 */
void NodeAddrListAddElement(NodeAddrListElement **head, const NodeAddr *addr)
{
    while (*head != NULL) {
        if (NodeAddrEqual(&(*head)->addr, addr)) {
            return;
        }

        head = &(*head)->next;
    }

    *head = RedisModule_Calloc(1, sizeof(NodeAddrListElement));
    (*head)->addr = *addr;
}

/* Concat a NodeAddrList to another NodeAddrList */
void NodeAddrListConcat(NodeAddrListElement **head, const NodeAddrListElement *other)
{
    const NodeAddrListElement *e = other;

    while (e != NULL) {
        NodeAddrListAddElement(head, &e->addr);
        e = e->next;
    }
}

/* Free a linked list of NodeAddrListElement */
void NodeAddrListFree(NodeAddrListElement *head)
{
    NodeAddrListElement *t;

    while (head != NULL) {
        t = head->next;
        RedisModule_Free(head);
        head = t;
    }
}

