include ../makefile.inc

all: librm.a rmtest_create_tables rmtest_delete_tables rmtest rmtest_p1 rmtest_p2 rmtest_p3 rmtest_p4 rmtest_p5 rmtest_p6 rmtest_p7

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

rmtest.o: rm.h rm_test_util.h
rmtest_create_tables.o: rm.h rm_test_util.h
rmtest_delete_tables.o: rm.h rm_test_util.h
rmtest_p1.o: rm.h rm_test_util.h
rmtest_p2.o: rm.h rm_test_util.h
rmtest_p3.o: rm.h rm_test_util.h
rmtest_p4.o: rm.h rm_test_util.h
rmtest_p5.o: rm.h rm_test_util.h
rmtest_p6.o: rm.h rm_test_util.h
rmtest_p7.o: rm.h rm_test_util.h

# binary dependencies
rmtest_create_tables: rmtest_create_tables.o librm.a $(CODEROOT)/rbf/librbf.a
rmtest_delete_tables: rmtest_delete_tables.o librm.a $(CODEROOT)/rbf/librbf.a
rmtest: rmtest.o librm.a $(CODEROOT)/rbf/librbf.a 
rmtest_p1: rmtest_p1.o librm.a $(CODEROOT)/rbf/librbf.a 
rmtest_p2: rmtest_p2.o librm.a $(CODEROOT)/rbf/librbf.a 
rmtest_p3: rmtest_p3.o librm.a $(CODEROOT)/rbf/librbf.a 
rmtest_p4: rmtest_p4.o librm.a $(CODEROOT)/rbf/librbf.a 
rmtest_p5: rmtest_p5.o librm.a $(CODEROOT)/rbf/librbf.a 
rmtest_p6: rmtest_p6.o librm.a $(CODEROOT)/rbf/librbf.a 
rmtest_p7: rmtest_p7.o librm.a $(CODEROOT)/rbf/librbf.a 



# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a


.PHONY: clean
clean:
	-rm rmtest_create_tables rmtest_delete_tables rmtest rmtest_p1 rmtest_p2 rmtest_p3 rmtest_p4 rmtest_p5 rmtest_p6 rmtest_p7 *.a *.o *~ *.t *_file
	$(MAKE) -C $(CODEROOT)/rbf clean
