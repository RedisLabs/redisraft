
#include "meta.h"

#include "redisraft.h"

#include <stdio.h>
#include <string.h>

int RaftMetaRead(RaftMeta *meta, const char *filename)
{
    char metafile[1024];
    snprintf(metafile, sizeof(metafile), "%s.meta", filename);

    meta->term = 0;
    meta->vote = RAFT_NODE_ID_NONE;

    FILE *fp = fopen(metafile, "r");
    if (!fp) {
        if (errno == ENOENT) {
            return RR_ERROR;
        }

        abort();
    }

    fscanf(fp, "%lu %d", &meta->term, &meta->vote);
    fclose(fp);

    return RR_OK;
}

int RaftMetaWrite(RaftMeta *meta,
                  const char *filename,
                  raft_term_t term,
                  raft_node_id_t vote)
{
    char orig[1024];
    snprintf(orig, sizeof(orig), "%s.meta", filename);

    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s.meta.tmp", filename);

    FILE *fp = fopen(tmp, "w+");
    if (!fp) {
        LOG_WARNING("RaftMetaWrite() fopen(): %s ", strerror(errno));
        return RR_ERROR;
    }

    fprintf(fp, "%lu %d", term, vote);
    fclose(fp);

    if (rename(tmp, orig) != 0) {
        LOG_WARNING("RaftMetaWrite() rename() : %s ", strerror(errno));
        abort();
    }

    meta->term = term;
    meta->vote = vote;

    return RR_OK;
}