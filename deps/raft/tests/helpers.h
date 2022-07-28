#ifndef _HELPERS_H
#define _HELPERS_H

static raft_entry_t *__MAKE_ENTRY(int id, raft_term_t term, const char *data)
{
    raft_entry_t *ety = raft_entry_new(data ? strlen(data) : 0);
    ety->id = id;
    ety->term = term;
    if (data) {
        memcpy(ety->data, data, strlen(data));
    }
    return ety;
}

static raft_entry_t **__MAKE_ENTRY_ARRAY(int id, raft_term_t term, const char *data)
{
    raft_entry_t **array = calloc(1, sizeof(raft_entry_t *));
    array[0] = __MAKE_ENTRY(id, term, data);

    return array;
}

static raft_entry_t **__MAKE_ENTRY_ARRAY_SEQ_ID(int count, int start_id, raft_term_t term, const char *data)
{
    raft_entry_t **array = calloc(count, sizeof(raft_entry_t *));
    int i;

    for (i = 0; i < count; i++) {
        array[i] = __MAKE_ENTRY(start_id++, term, data);
    }

    return array;
}

static void __RAFT_APPEND_ENTRY(void *r, int id, raft_term_t term, const char *data)
{
    raft_entry_t *e = __MAKE_ENTRY(id, term, data);
    raft_append_entry(r, e);
    raft_node_set_match_idx(raft_get_my_node(r), raft_get_current_idx(r));
}

static void __RAFT_APPEND_ENTRIES_SEQ_ID(void *r, int count, int id, raft_term_t term, const char *data)
{
    int i;
    for (i = 0; i < count; i++) {
        raft_entry_t *e = __MAKE_ENTRY(id++, term, data);
        raft_append_entry(r, e);
    }
}

static void __RAFT_APPEND_ENTRIES_SEQ_ID_TERM(void *r, int count, int id, raft_term_t term, const char *data)
{
    int i;
    for (i = 0; i < count; i++) {
        raft_entry_t *e = __MAKE_ENTRY(id++, term++, data);
        raft_append_entry(r, e);
    }
}

#endif
