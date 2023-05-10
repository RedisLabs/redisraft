/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

void test_util(void);
void test_serialization(void);
void test_file(void);
void test_log(void);

int main(int argc, char *argv[])
{
    test_util();
    test_serialization();
    test_file();
    test_log();

    return 0;
}
