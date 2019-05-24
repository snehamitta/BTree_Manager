#include "dberror.h"
#include "tables.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "errno.h"

//structure needed to handle functions
typedef struct NodeHandle{
    bool delete[2];
    int numValues;
    int values[2];
    struct NodeHandle *nodes[3];
    bool leaf;
    RID rids[2];
} NodeHandle;

//structure needed to handle functions
typedef struct btreeList{
    BTreeHandle * bth;
    int numNodes;
    int numEntries;
} btreeList;

//global varialbles initialization
int currentPos = 0;
int t = 0;
int i = 0;
btreeList btl[55];
int initIndexMan = 0;

//initialize btree manager 
RC initIndexManager (void *mgmtData){
	initIndexMan = 1; //setting the global variable to 1
	mgmtData = NULL; //initializing the management data
	printf("Initialize index manager");
    return RC_OK;
}

//shut down btree manager 
RC shutdownIndexManager (){
	initIndexMan = 0; //setting global variable to 0
	printf("Shuting down index manager\n");
    return RC_OK;
}

//creating a btree 
RC createBtree (char *idxId, DataType keyType, int n){
    BTreeHandle* tree = (BTreeHandle*)malloc(sizeof(BTreeHandle)); //allocating memory for treeHandle
    NodeHandle* rootNode = (NodeHandle*)malloc(sizeof(NodeHandle)); //allocating memory to nodehandle
    if(tree == NULL) { //checking if memory is allocated
    	printf("Malloc failed\n");
    }else { //if not put in the values as per input parameters
    	tree->mgmtData = rootNode;
    	tree->keyType = keyType;
    	tree->idxId = idxId;
    }
    if(rootNode == NULL) { //checking if memory is allocated
    	printf("Malloc Failed\n");
    }else { //if not initialize the structure defined for book keeping
    	rootNode->numValues = 0;
    	rootNode->leaf = true;
    	btl[t].bth = tree;
   	 	btl[t].numNodes = 0;
    	btl[t].numEntries = 0;
    	t = t+1;
    }
    
    return RC_OK;
}

//opening a btree 
RC openBtree (BTreeHandle **tree, char *idxId){//check
    int i=0;
    while (i<t){ //checking for the entire globally defined array
        if(strcmp(idxId,btl[i].bth->idxId)==0) //comparing the index ids of of the tree
        {
            *tree = btl[i].bth;
            return RC_OK;
        }
        i++; //incrementing the loop
    }
    return -1;
}

//closing a btree 
RC closeBtree (BTreeHandle *tree){//check
	if(tree == NULL) { //checking if tree initialized and opened is null
		fprintf(stderr, "Issue: %s\n", strerror(errno));
	}
    return RC_OK; //if not should be okay
}

//deleting a btree 
RC deleteBtree (char *idxId){//check
    int i=0;
    if(idxId == NULL) { //
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    }else {
    	while (i<t){// checking for the entire defined global array
        	if(strcmp(idxId,btl[i].bth->idxId)==0) { //comparing the index ids of the tree
            	free(btl[i].bth); //if a match, then free
            	btl[i].numNodes = 0; //setting nodes and entries to 0
            	btl[i].numEntries = 0;
            	t--;
            	return RC_OK;
        	}
        i++; //traverse through all the nodes
    	}
    }
    return RC_OK;
}

//get number of nodes in tree
RC getNumNodes (BTreeHandle *tree, int *result){
    int i=0;
    if(tree == NULL) { //checking if tree has been initialized
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    }else {
    	while (i<t){//checking for the entire defined global array
        	if(strcmp(tree->idxId,btl[i].bth->idxId)==0) { //comparing the index ids of the tree
            	*result = btl[i].numNodes; //store the number of nodes in varialbe result
            	return RC_OK;
        	}
        	i++; //traverse thru the array
    	}
    }
    return -1;
}

//defined function to increase the number of nodes in the tree
RC increaseNodes (BTreeHandle *tree){//check
    int i=0;
    if(tree == NULL) { //checking if the tree has been initialized
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    }else {
    	while (i<t){ //checking for the entire defined global array
        	if(strcmp(tree->idxId,btl[i].bth->idxId)==0) { //comparing the index ids of the tree
            	btl[i].numNodes = btl[i].numNodes+1; //increment the number of nodes by 1
            	return RC_OK;
        	}
        	i++;//traverse the loop
    	}	
    }
    
    return -1;
}

//increase the number of entries in the tree
RC increaseEntries (BTreeHandle *tree){
    int i=0;
    if(tree == NULL) {
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    } else {
    	while (i<t){ //checking for the entire defined global array
        	if(strcmp(tree->idxId,btl[i].bth->idxId)==0) { //comparing the index ids of the tree
            	btl[i].numEntries = btl[i].numEntries+1; //increment the number of entries by 1
            	return RC_OK;
        	}
        	i++; //traverse the loop
   		}
    }
    
    return -1;
}

//get number of entries in tree
RC getNumEntries (BTreeHandle *tree, int *result) {//check
    int i=0;
    if(tree == NULL) { //checking if the tree has been initialized
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    } else {
    	while (i<t){ //checking for the entire defined global array
        	if(strcmp(tree->idxId,btl[i].bth->idxId)==0) { //comparing the index ids of the tree
            	*result = btl[i].numEntries; //retrieving the number of entries
            	return RC_OK;
        	}
        	i++; //traverse the loop
    	}
    }
    return -1;
}

//get keyType for btree
RC getKeyType (BTreeHandle *tree, DataType *result) {
	char * trid = tree->idxId;
    int i =0;
    while(i<t){ //checking for the entire defined global array
    	if(strcmp(trid,btl[i].bth->idxId)==0) //comparing the index ids of the tree
        {
            *result = btl[i].bth->keyType; //retrieving the key type
            return RC_OK;
        }
        i = i+1; //traverse the loop
    }
    return RC_IM_NO_MORE_ENTRIES; //result in no more entries once done
}

//find needed key
RC findKey (BTreeHandle *tree, Value *key, RID *result){
    int val = key->v.intV; //give value of key
    NodeHandle *myNode = (NodeHandle *)tree->mgmtData; //retrieve management data and store in allocated nodehandle
    int i;
    int check;
    if(myNode == NULL) { //checking if allocation is valid
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    } else {
    	while(1){
        	if(myNode->leaf==true){//checking if leaf node
            	for(i=0;i<myNode->numValues;i++){ //traversing through the number of values
                	if(myNode->values[i]==val){ //if value matches
                    		if(myNode->delete[i] == false) { //if delete condition is false
                    			result->page = myNode->rids[i].page; //store slot and page ids in result
                    			result->slot = myNode->rids[i].slot;
                    			return RC_OK;
                    	}
                	}
            	}
            	return RC_IM_KEY_NOT_FOUND; //otherwise key not found
        	}
        	if(myNode != NULL) {//checking if node is not null
        		int mVals0 = myNode->values[0]; //variables to compare 
        		int mVals1 = myNode->values[1];
        		if(mVals0 > 0 && mVals1 > 0) { //checking for the valid conditions 
        			if(val < mVals0){ 
            			check = 0;
        			}
        			else if(val < mVals1){
            			check = 1;
        			}
        			else{
            			check = 2;
        			}
        		}	
        	}
        	switch(check) {
        		case 0: {
        			myNode = myNode->nodes[0]; //if the key is the left one
        			break;
        		}
        		case 1 : {
        			myNode = myNode->nodes[1]; //if the key is the middle one
        			break;
        		}
        		case 2: {
        			myNode = myNode->nodes[2]; //if the key is the right one
        			break;
        		}
        		default : break;

        	}
    	}
    }
    return RC_OK;   
}

//declaring it cause not present in header file
NodeHandle *insertNewNode(BTreeHandle *tree, NodeHandle *myNode, int val, RID rid);

//inserts key and record in tree
RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    NodeHandle* root = (NodeHandle*)tree->mgmtData; //define pointer of type nodehandle 
    NodeHandle **superRoot = &root; 

    if(root == NULL) { //checking if root has been initial
    	printf("root not initialized\n");
    }
    else  {
    	if((*superRoot)->numValues < 1){ //initializing all the values in the struct
        	(*superRoot)->rids[0].page=rid.page; //allocating the input values to the required attributes
        	(*superRoot)->rids[0].slot=rid.slot;
        	(*superRoot)->values[0]=key->v.intV;
        	(*superRoot)->values[1]=0;
        	(*superRoot)->nodes[2]=NULL;
        	(*superRoot)->numValues++;
        	increaseNodes(tree); //increasing the nodes
        	increaseEntries(tree); //increasing the entries
        	return RC_OK;
    	}
    }
    
    NodeHandle *ret = insertNewNode(tree,root,key->v.intV,rid); //calling the insert new node function
    if(ret == NULL) {
    	printf("");
    }
    else{
        tree->mgmtData = ret;
        increaseNodes(tree); //otherwise, increase the nodes
    }
    return RC_OK;
}

//setting the properties of leaf nodes
void setPropertiesLeaf( NodeHandle *myNode, RID rid, int val) {
	myNode->values[1] = myNode->values[0];
    myNode->rids[1].page = myNode->rids[0].page;
    myNode->rids[1].slot = myNode->rids[0].slot;
    myNode->values[0] = val;
    myNode->rids[0].page = rid.page;
    myNode->rids[0].slot = rid.slot;
               			
}

//setting the properties of non leaf nodes
void setPropertiesNonLeaf( NodeHandle *myNode, NodeHandle *root, int val) {
	myNode->values[1] = myNode->values[0];
    myNode->values[0] = root->values[0];
    myNode->nodes[2] = myNode->nodes[1];
    myNode->nodes[0] = root->nodes[0];
    myNode->nodes[1] = root->nodes[1];
}

//self defined insert new node function
NodeHandle *insertNewNode(BTreeHandle *tree, NodeHandle *myNode, int val, RID rid){
    NodeHandle *ret,*left,*right;
    if(myNode->numValues > 0) {
    	switch(myNode->leaf) { //reach leaf
    		case true: {
        		if(myNode->numValues==1){
            		myNode->numValues++;
           			if(myNode->values[0] > val){
           				setPropertiesLeaf(myNode, rid,val);
               			increaseEntries(tree);
               			return NULL;
           			} else {
           				myNode->values[1] = val;
           				myNode->rids[1].page = rid.page;
            			myNode->rids[1].slot = rid.slot;
            			increaseEntries(tree);
           				return NULL;
           			}
           			
        		} else {
        			//leaf split
        			increaseNodes(tree);
        			ret = (NodeHandle *)malloc(sizeof(NodeHandle));
        			if(ret == NULL) {
        				printf("Malloc failed\n");
        			}
        			else {
        				ret->leaf = false;
        				ret->numValues = 1;	
        				right = (NodeHandle *)malloc(sizeof(NodeHandle));
        				if(right == NULL) {
        					printf("Malloc failed\n");
        				}else {
        					right->leaf = true;
     		  				right->numValues = 1;
        					right->nodes[2] = myNode->nodes[2];
        					myNode->nodes[2] = right;
        					myNode->leaf = true;
        					right->values[1]=0;
						if (val < myNode ->values[0]){
						  goto label1;
						}
						else if (val < myNode->values[1]){
						  goto label2;
						}
						else{
						  goto label3;
						}
					label1:{
						right->values[0] = myNode->values[1];
						right->rids[0] = myNode->rids[1];
						setPropertiesLeaf(myNode,rid,val);
						goto label4;
						}
					label2:{
						right->values[0] = myNode->values[1];
						right->rids[0].page = myNode->rids[1].page;
						right->rids[0].slot = myNode->rids[1].slot;
						myNode->values[1] = val;
						myNode->rids[1].page = rid.page;
						myNode->rids[1].slot = rid.slot;
						goto label4;
						}
					label3:{
						right->values[0] = val;
						right->rids[0].page = rid.page;
						right->rids[0].slot = rid.slot;
						goto label4;
						}
					label4:{
						ret->nodes[0] = myNode;
        					ret->nodes[1] = right;
        					ret->values[0] = right->values[0];
        					increaseEntries(tree);
        					return ret;
						}
        				}
        			}
        		}
		}

    	case false: { //non leaf
    		if(myNode->numValues==1){
       			NodeHandle *root = NULL;
       			if(myNode->values[0] != val) {
       				if(myNode->values[0]>val){
           				root = insertNewNode(tree,myNode->nodes[0],val,rid);
       				} else {
            			root = insertNewNode(tree,myNode->nodes[1],val,rid);
        			}
       			}
        		if(root == NULL){
        			printf("");
        		}
        		else { 
            		myNode->numValues++;
            		if(root->values[0]<myNode->values[0]){
                		setPropertiesNonLeaf(myNode,root,val);
                		return NULL;
            		}
            		if(myNode->values[1] != root->values[0]) {
            			myNode->values[1] = root->values[0];
            			myNode->nodes[1] = root->nodes[0];
           				myNode->nodes[2] = root->nodes[1];
            		}
            		
           			return NULL;
        		}
       			return NULL;
   			}
   			NodeHandle *root = NULL; 
   			if(myNode->values[0] != val) {
   				int mVals0 = myNode->values[0];
   				int mVals1 = myNode->values[1];
   				if(mVals0>0 && mVals1 > 0) {
   					if(mVals0>val){
       					root = insertNewNode(tree,myNode->nodes[0],val,rid);
   					} else if(mVals1>val) {
        				root = insertNewNode(tree,myNode->nodes[1],val,rid);
    				} else {
        				root = insertNewNode(tree,myNode->nodes[2],val,rid);
    				}
   				}
   				
   			}
    		if(root == NULL){ 
    			printf("");
    		} //non leaf split
    		else {
        		increaseNodes(tree);
        		ret = (NodeHandle *)malloc(sizeof(NodeHandle));
        		if(ret == NULL) {
        			printf("Malloc Failed\n");
        		}
        		else {
        			ret->leaf = false;
        			ret->numValues = 1;
        			right = (NodeHandle *)malloc(sizeof(NodeHandle));
        			if(right == NULL) {
        				printf("Malloc failed\n");
        			}
        			else {
        				right->leaf = false;
        				right->numValues = 1;
        				left = (NodeHandle *)malloc(sizeof(NodeHandle));
        				if(left == NULL) {
        					printf("Malloc Failed\n");
        				} else {
        					left->leaf = false;
        					left->numValues = 1;
        					if(root->values[0]<myNode->values[0]){
           						goto label5;
        					} else if(root->values[0]<myNode->values[1]){
           						goto label6;
        					} else {
           						goto label7;
       						}
        					label5:
        						ret->values[0] = myNode->values[0];
           						ret->nodes[0] = left;
           						ret->nodes[1] = right;
           						right->values[0] = myNode->values[1];
           						right->nodes[0] = myNode->nodes[1];
           						right->nodes[1] = myNode->nodes[2];
           						left->values[0] = root->values[0];
           						left->nodes[0] = root->nodes[0];
           						left->nodes[1] = root->nodes[1];
           						goto label8;
           					label6:
           						ret->values[0] = root->values[0];
           						ret->nodes[0] = left;
           						ret->nodes[1] = right;
           						right->values[0] = myNode->values[1];
           						right->nodes[0] = root->nodes[1];
           						right->nodes[1] = myNode->nodes[2];
           						left->values[0] = myNode->values[0];
           						left->nodes[0] = myNode->nodes[0];
           						left->nodes[1] = root->nodes[0];
           						goto label8;
           					label7:
           						ret->values[0] = myNode->values[1];
           						ret->nodes[0] = left;
           						ret->nodes[1] = right;
           						right->values[0] = root->values[0];
           						right->nodes[0] = root->nodes[0];
           						right->nodes[1] = root->nodes[1];
           						left->values[0] = myNode->values[0];
           						left->nodes[0] = myNode->nodes[0];
           						left->nodes[1] = myNode->nodes[1];
           						goto label8;
       						label8:
     							free(root);
      							return ret;
        					}
        					
        				}	
        			}
    			}
    		}
    	}
   }
   return NULL;
}

//delete given key
RC deleteKey (BTreeHandle *tree, Value *key){
    int val = key->v.intV;
    NodeHandle *myNode = (NodeHandle *)tree->mgmtData;
    int i = 0;;
    int check;
    if(myNode == NULL) { //checking if node given is null
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    } else {
    	while(1){
        	if(myNode->leaf==true){//checking for leaf node
            	while(i < myNode->numValues) {
            		if(myNode->values[i]==val && myNode->delete[i]==false){
                    	myNode->delete[i]=true; //setting delete attribute as true
                    	return RC_OK;
                	}
                	i = i + 1; //iterate thru the loop
            	}
            	return RC_IM_KEY_NOT_FOUND;
        	}
        	if(myNode != NULL) {
        		int mVals0 = myNode->values[0];
        		int mVals1 = myNode->values[1];
        		if(mVals0 > 0 && mVals1 > 0) { //setting attributes for switch case
        			if(val < mVals0){
            			check = 0;
        			}
        			else if(val < mVals1){
            			check = 1;
        			}
        			else{
            			check = 2;
        			}
        		}
        	
        	}
        	
        	switch(check) { //checking if node is left, middle or right
        		case 0: {
        			myNode = myNode->nodes[0];
        			break;
        		}
        		case 1 : {
        			myNode = myNode->nodes[1];
        			break;
        		}
        		case 2: {
        			myNode = myNode->nodes[2];
        			break;
        		}
        		default : break;

        	}
    	}
    }
    return RC_OK;
    
}

//opens the scan handle to scan the tree
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
    currentPos = 1;
    NodeHandle * myNode = (NodeHandle*)tree->mgmtData; //initializing node handle and storing management data
    if(myNode == NULL) { //if null, throw error
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    } else {
    	while(myNode->leaf!=true){
        	myNode = myNode->nodes[0];
    	}
    }
    
    BT_ScanHandle *sh = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle)); //initialize the scan handle by allocationg memory
    if(sh == NULL) { //if null, throw error
    	fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
    } else { //else, store values in allocated attributes
    	sh->tree = tree;
    	sh->mgmtData = myNode;
    	*handle = sh;
    }
    return RC_OK;
}

//gives the next entry in the scan process
RC nextEntry (BT_ScanHandle *handle, RID *result){
    if(currentPos==-1){ //if the current position is -1, then return no more entries
        return RC_IM_NO_MORE_ENTRIES;
    }
    NodeHandle * myNode = (NodeHandle *)handle->mgmtData;
    if(myNode == NULL) { //checking if node handle is null
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    } else {
    	if(currentPos > 0) { //but if current position is greater than 0
    		if(currentPos==1){
        	result->page = myNode->rids[0].page;//store values in result attribute
        	result->slot = myNode->rids[0].slot;
        	currentPos = 2; //incrememt the current position
        	return RC_OK;
    		}
    		if(currentPos==2){ //checking if current position is 2
        		if (myNode->values[1]!=0){
        			if(myNode != NULL) {
        				int resPage = myNode->rids[1].page;
        				int resSlot = myNode->rids[1].slot;
        				if(resSlot >= 0 && resPage >= 0) {
        					result->page = resPage; //store values in result attribute
            				result->slot = resSlot;
            				myNode = myNode->nodes[2];
            				handle->mgmtData = myNode;
        				}
            			if(myNode==NULL){ //check if we have gone out of the increments
                			currentPos = -1;
                			return RC_OK;
            			}	
            			currentPos = 1; 
            			return RC_OK;
        			}	
        		}
    		}
    	} else {
    		return RC_IM_NO_MORE_ENTRIES; //if all entries are scanned, return no more entries
    	}
    	
    	myNode = myNode->nodes[2];
    	handle->mgmtData = myNode;
    	if(myNode==NULL){
        	return RC_IM_NO_MORE_ENTRIES; //if node is null, then reurn no more entries
    	} else {
    		result->page = myNode->rids[0].page; //if not, store values in the result attribute
    		result->slot = myNode->rids[0].slot;
    		currentPos = 2;
    	}
    }
   	return RC_OK;
}

//close the opened scan
RC closeTreeScan (BT_ScanHandle *handle){
	if(handle == NULL) {
		return RC_NOT_OK;
	}else {
		return RC_OK;
	}
    
}

// calling the functions, but not in header file
void printNode(NodeHandle *myNode, int ct);
char *printTree(BTreeHandle *tree);

//printing the tree for debugging purposes
char *printTree (BTreeHandle *tree){
    NodeHandle * myNode = (NodeHandle *)tree->mgmtData;
    if(myNode == NULL) { //checking if node is null
    	fprintf(stderr, "Issue: %s\n", strerror(errno));
    	return "Issue";
    } else {// else call print node function
    	printNode(myNode,0);
    	return "";
    }
}

//self defined function to call printing of node
void printNode(NodeHandle *myNode, int ct){
	if(myNode == NULL) { //check if node is null
		fprintf(stderr, "Issue: %s\n", strerror(errno));
	} else { //if not, print the node details
		printf(" (%d)[%d,%d,%d,%d]\n", ct, myNode->values[0],myNode->values[1],myNode->leaf?0:1,myNode->nodes[2]==NULL?0:1);
    	if(ct >= 0) {
    		if(myNode->leaf == false){
        		printNode(myNode->nodes[0], ct+1); //checking if not leaf, increment and go down the tree to print
        		printNode(myNode->nodes[1], ct+1);
        		if(myNode->numValues==2){
            		printNode(myNode->nodes[2],ct+1);
        		}
    		}
    	}
    	
	}
    
}
