###Assignment 4 - B+ Tree Manager


##### To Build: `make all`


##### To Run Test 1(test_assign4_1.c) : `make runTest1`


##### To Run Test 2(test_expr.c) : `make runTest2`

This project is to implement a b+ tree manager on top of the storage manager, buffer manager and record manager designmed. The basic functionalities of the code are described below:

extern RC initIndexManager (void mgmtData):
This function initializes the b+ tree manager. Another way to activate the b+ tree manager.

extern RC shutdownIndexManager ():
This function is called to shut down the b+ tree manager.

extern RC createBtree (char idxId, DataType keyType, int n):
This function allocates memory to all the b+tree structures. It also creates a page file with IdxId.

extern RC openBtree (BTreeHandle tree, char idxId):
This function opens a tree with given IdxId.

extern RC closeBtree (BTreeHandle tree):
This function closes the tree that was open. This function also frees all the allocated memory to that tree.

extern RC deleteBtree (char idxId):
This function deletes all the record data of the given tree.

extern RC getNumNodes (BTreeHandle tree, int result):
This function gets the number of nodes in a given tree.

extern RC getNumEntries (BTreeHandle tree, int result):
This function returns the total number of entries in a given tree.

extern RC getKeyType (BTreeHandle tree, DataType result):
This function returns the key type of of the tree, such as integer, string etc. 

extern RC findKey (BTreeHandle tree, Value key, RID result):
This function returns the key that needs to be searched for in the tree.

extern RC insertKey (BTreeHandle tree, Value key, RID rid):
This function inserts the key that the respective RIDs into the given tree.

extern RC deleteKey (BTreeHandle tree, Value key):
This function delets the given key from the existing b+ tree.

extern RC openTreeScan (BTreeHandle tree, BT_ScanHandle handle):
This function scans all the entries of the given tree.

extern RC nextEntry (BT_ScanHandle handle, RID result):
This function scans the next entry in the given tree.

extern RC closeTreeScan (BT_ScanHandle handle):
This function closes the scanning elements of the program.

extern char printTree (BTreeHandle tree):
This function is used for debugging puropses, which displays the arrangement of the tree. 
