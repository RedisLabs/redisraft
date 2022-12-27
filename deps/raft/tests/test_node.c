#include <stdbool.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "CuTest.h"

#include "raft.h"
#include "raft_log.h"
#include "raft_private.h"

void TestRaft_node_set_nextIdx(CuTest * tc)
{
    raft_node_t *p = raft_node_new((void *) 1, 1, 1);
    raft_node_set_next_idx(p, 3);
    CuAssertTrue(tc, 3 == raft_node_get_next_idx(p));
}


int main(void)
{
    CuString *output = CuStringNew();
    CuSuite* suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, TestRaft_node_set_nextIdx);

    CuSuiteRun(suite);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    return suite->failCount == 0 ? 0 : 1;
}
