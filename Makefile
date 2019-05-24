all: test1 test2



test2: storage_mgr.c test_expr.c dberror.c record_mgr.c buffer_mgr_stat.c buffer_mgr.c expr.c rm_serializer.c btree_mgr.c
	gcc -g -Wall record_mgr.c buffer_mgr_stat.c buffer_mgr.c expr.c rm_serializer.c storage_mgr.c test_expr.c dberror.c btree_mgr.c -o expr
test1: storage_mgr.c test_assign4_1.c dberror.c record_mgr.c buffer_mgr_stat.c buffer_mgr.c expr.c rm_serializer.c
	gcc -g record_mgr.c buffer_mgr_stat.c buffer_mgr.c expr.c rm_serializer.c storage_mgr.c test_assign4_1.c dberror.c btree_mgr.c -o assign4


runTest1: assign4
	./assign4

runTest2: expr
	./expr


