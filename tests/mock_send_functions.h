#ifndef MOCK_SEND_FUNCTIONS_H
#define MOCK_SEND_FUNCTIONS_H

typedef enum
{
    RAFT_REQUESTVOTE_REQ,
    RAFT_REQUESTVOTE_RESP,
    RAFT_APPENDENTRIES_REQ,
    RAFT_APPENDENTRIES_RESP,
    RAFT_ENTRY_REQ,
    RAFT_ENTRY_RESP,
} raft_message_type_e;

void senders_new();

void* sender_new(void* address);

void* sender_poll_msg_data(void* s);

void sender_poll_msgs(void* s);

int sender_msgs_available(void* s);

void sender_set_raft(void* s, void* r);

int sender_requestvote(raft_server_t* raft,
        void* udata, raft_node_t* node, raft_requestvote_req_t* msg);

int sender_requestvote_response(raft_server_t* raft,
        void* udata, raft_node_t* node, raft_requestvote_resp_t* msg);

int sender_appendentries(raft_server_t* raft,
        void* udata, raft_node_t* node, raft_appendentries_req_t* msg);

int sender_appendentries_response(raft_server_t* raft,
        void* udata, raft_node_t* node, raft_appendentries_resp_t* msg);

int sender_entries(raft_server_t* raft,
        void* udata, raft_node_t* node, raft_entry_req_t* msg);

int sender_entries_response(raft_server_t* raft,
        void* udata, raft_node_t* node, raft_entry_resp_t* msg);

#endif /* MOCK_SEND_FUNCTIONS_H */
