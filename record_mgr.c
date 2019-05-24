#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "record_mgr.h"
#include "rm_serializer.h"
#include <errno.h>

int initRecMan = 0;
int test = 0;
extern int errno ;

//Structure to store the table details
typedef struct Table {
	int numTuples;
	int numSlots;
	int slotSize;
	Schema *schema;
	BM_BufferPool *bm;
	int totalTuples;
	int schemaSize;
} Table;

//structure to store the scanning details
typedef struct Scanner {
	Expr *expression;
	int currPage;
	int currSlot;
	int numTuples;
	int numSlots;
	int scannedTuples;
	int totalNumPages;
} Scanner;


//global pointers to store scan management details
char *nameGlobal;
Schema *schemaGlobal;
int numTuplesGb = 0;
int numSlotsGb = 0;

//This function was pasted from rm_serializer
RC
attrOffset (Schema *schema, int attrNum, int *result)
{
	int offset = 0;
	int attrPos = 0;

	for(attrPos = 0; attrPos < attrNum; attrPos++)
		switch (schema->dataTypes[attrPos])
		{
			case DT_STRING:
				offset += schema->typeLength[attrPos];
				break;
			case DT_INT:
				offset += sizeof(int);
				break;
			case DT_FLOAT:
				offset += sizeof(float);
				break;
			case DT_BOOL:
				offset += sizeof(bool);
				break;
		}

	*result = offset;
	return RC_OK;	
}

//Function to initialize the record manager
extern RC initRecordManager (void *mgmtData){
	RC rc = RC_OK;
	if(!initRecMan){
		initRecMan = 1; //setting the record manager to start
		rc = RC_OK;
	}
	else {
		printf("Record Manager not initialized\n");
		rc = RC_NOT_OK;
	}
	return rc;
}

//shutting down record manager
extern RC shutdownRecordManager (){
	initRecMan = 0; //closing the record manager
	printf("Shutting Down Record Manager\n");
	return RC_OK;
}

//function which gets the length of slot from the input schema
int slotLength(Schema *schema) {
	int size = 0;
	int totalSize = 0;
		for(int i = 0; i < schema->numAttr; i++){ 
			switch(schema -> dataTypes[i]){//switch case for the data type of the attribute
				case DT_INT:
					size = sizeof(int);
					break;

				case DT_STRING:
					size = (schema->typeLength[i])*sizeof(char);
					break;

				case DT_FLOAT:
					size = sizeof(float);
					break;

				case DT_BOOL:
					size = sizeof(bool);
					break;

			}
			totalSize += (size + strlen(schema->attrNames[i])); //returns the total size based on the case
		}
	return totalSize;
}

//This function creates a table using the input schema and name
extern RC createTable (char *name, Schema *schema){
	initStorageManager(); //initializing the storage manager
	RC rc;
	schemaGlobal = (Schema*) malloc(sizeof(Schema)); //allocating schema size to the global variable
	schemaGlobal = schema; //putting in the input schema into the initialized global variable
	Schema *schemaTemp; //using a temporary pointer of type schema
	schemaTemp = serializeSchema(schema); //using the serializeSchema function from rm_serializer
	schemaGlobal = (Schema*) malloc(sizeof(Schema));

	//setting all attributes of schema global to the input values obtained from schema
	schemaGlobal->numAttr = schema->numAttr;
	schemaGlobal->dataTypes = schema->dataTypes;
	schemaGlobal->typeLength = schema->typeLength;
	schemaGlobal->keyAttrs = schema->keyAttrs;
	schemaGlobal->attrNames = schema->attrNames;
	schemaGlobal->keySize = schema->keySize;

    BM_PageHandle *pageHandle=(BM_PageHandle*)malloc(sizeof(BM_PageHandle)); //allocating and checking memory for BM_pagehandle
    if(pageHandle == NULL) {
        fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
    }
    Table *myTable=(Table*)malloc(sizeof(Table)); //allocating and checking memory for table
    if(myTable == NULL) {
        fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
    }

	myTable->bm = (BM_BufferPool*)malloc(sizeof(BM_BufferPool)); //the bm in table gets memory size of bufferpool
	char * data;

	nameGlobal = name; //setting name of pagefile

	rc = createPageFile(name); //creating a pagefile with given name
	if(rc != RC_OK) {
		return rc;
	}
//    printf("After createPageFile\n");
	rc = initBufferPool(myTable->bm, name, 3, RS_LRU, NULL); //initializing bufferpool
	if(rc != RC_OK) {
		return rc;
	}
//    printf("After initBufferPool\n");
	myTable->numTuples =0; //setting attributes of table struct
	myTable->schema=schema;
	myTable->slotSize = slotLength(schema);
	myTable->numSlots = PAGE_SIZE/myTable->slotSize;
	numTuplesGb = 0;
	numSlotsGb = PAGE_SIZE/myTable->slotSize;
	myTable->schemaSize = sizeof(schema);

	VarString *string;
	MAKE_VARSTRING(string); //tokenize the number of slots, number of tuples and size of slots
	APPEND(string, "numofSlots<%d>, numofTuple<%d>, slotSize<%d>",myTable->numSlots,myTable->numTuples,myTable->slotSize);
	GET_STRING(data,string);

	rc = writeBlock (0, ((poolDetail*)myTable->bm->mgmtData)->fileHandle, data); //write data to file handle
	if(rc != RC_OK) {
		return rc;
	}
//	printf("After writeBlock\n");
	rc = pinPage(myTable->bm, pageHandle, 0); //pin the page after block written

	return rc;
}

//This function extracts values from the tokenized format to just plain numbers
Table* extractValues(char *data){
	Table *myTable= (Table*)malloc(sizeof(Table));
	char tempString[strlen(data)];
	strcpy(tempString,data);
//	printf("tempStr:%s\n",data);

	char *temp1;
	long tempLong;

	for(int i = 0; i<1;i++) {
		temp1 = strtok(tempString, "<");
		temp1 = strtok(NULL,">");

	}
	tempLong = (long) atoi(temp1);
	myTable->numSlots=tempLong; //retrieval of number of slots
//	printf("numSlots: %d\n",myTable->numSlots);

	for(int i = 0; i<1;i++) {
		temp1 = strtok(NULL, "<");
		temp1 = strtok(NULL,">");

	}
	tempLong = (long) atoi(temp1);
	myTable->numTuples=tempLong;//retrieval of number of tuples
//	printf("numTup: %d\n",myTable->numTuples);

	for(int i = 0; i<1;i++) {
		temp1 = strtok(NULL, "<");
		temp1 = strtok(NULL,">");

	}
	tempLong = (long) atoi(temp1);
	myTable->slotSize=tempLong; //retrieval of slot size
//	printf("slotSize: %d\n",myTable->slotSize);

	return myTable;
}

//This function opens a table with the given name and data in it being from rel
RC openTable (RM_TableData *rel, char *name){
	RC rc = RC_OK;
	if(name == nameGlobal) {
		BM_BufferPool *bm=(BM_BufferPool*)malloc(sizeof(BM_BufferPool)); //allocating memory for bufferpool and checking condition
		if(bm == NULL) {
            fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
		}
		BM_PageHandle *pageHandle=(BM_PageHandle*)malloc(sizeof(BM_PageHandle)); //allocating memory for pagehandle and checking condition
		if(pageHandle == NULL) {
            fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
		}
		Table *myTable=(Table*)malloc(sizeof(Table)); //allocating memory for table struct and checking condition
		if(myTable == NULL) {
            fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
		}

		rc = initBufferPool(bm, name, 3, RS_LRU, NULL); //initializing bufferpool
		if(rc == RC_OK) {
            pinPage(bm, pageHandle, 0);
            myTable=extractValues(pageHandle->data); //get the values numSlots, NumTuples and slotSize
            myTable->bm=bm;
		}
		else {
            printf("Could not init buffer pool\n");
		}

		rel->schema = schemaGlobal; //store schema in the global variable
		rel->mgmtData=myTable; //input rel management data goes into table struct
		return rc;
	}
	else {
		rc = RC_NOT_OK;
		return rc;
	}
}

//closes the table with data coming in from rel
RC closeTable (RM_TableData *rel){
	RC rc = RC_OK;
	Table *myTable = (Table*)malloc(sizeof(Table)); //allocating memory to table struct


	BM_PageHandle *pageHandle = (BM_PageHandle*)malloc(sizeof(BM_PageHandle)); //allocating memory to pagehandle
	myTable = rel->mgmtData; //table struct has the management data of rel

	char* pageData;
	VarString *string;

	MAKE_VARSTRING(string);
	APPEND(string, "numofSlots<%d>, numofTuple<%d>, slotSize<%d>",myTable->numSlots,myTable->numTuples,myTable->slotSize);
	GET_STRING(pageData,string); //tokenizing the three values

	rc = writeBlock (0, ((poolDetail*)myTable->bm->mgmtData)->fileHandle, pageData); //writing page data to filehandle
	free(pageHandle); //freeing the page handle

	return rc;
}

// This function deletes the pagefile
RC deleteTable (char *name){
	RC rc = RC_OK;
	if(name == nameGlobal) {
		rc = destroyPageFile(name); //destroying the pagefile with the given name
	}
	return rc;
}

//This function returns the number of tuples
int getNumTuples (RM_TableData *rel){
	int numberTuples = ((Table*)rel->mgmtData)->numTuples; //to get the number of tuples stored in rel
	return(numberTuples);
}

//This function inserts record using data from rel
extern RC insertRecord (RM_TableData *rel, Record *record){
	RC rc = RC_OK;
	Table *myTable = rel->mgmtData;
	BM_PageHandle *pageHandle=(BM_PageHandle*)malloc(sizeof(BM_PageHandle)); //allocating memory for pagehandle and checking this condition
    if(pageHandle == NULL) {
        fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
    }
	rc = pinPage(myTable->bm, pageHandle, ((poolDetail *)myTable->bm->mgmtData)->fileHandle->totalNumPages-1); //pinning page

	if (rc == RC_OK) {
		int x = 0; int y = 0;
		x = myTable->numTuples;
		y = myTable->numSlots;
		record->id.slot =  x % y;
		if (pageHandle->pageNum ==1 || record->id.slot == 0 ){ //checking if the conditions satisfy to append empty block
			rc = appendEmptyBlock(((poolDetail *)myTable->bm->mgmtData)->fileHandle);
			rc = pinPage(myTable->bm, pageHandle, ((poolDetail *)myTable->bm->mgmtData)->fileHandle->totalNumPages-1);
		}
		else {
			rc = RC_NOT_OK;
		}

	}
	int totalNumPages = ((poolDetail *)myTable->bm->mgmtData)->fileHandle->totalNumPages - 1; //getting value of total number of pages
	if(!(totalNumPages < 0 )) { //if total number of pages is greater than 0
        record->id.page = totalNumPages; //allocating the value of pages to page id
        memcpy(pageHandle->data + record->id.slot * myTable->slotSize, record->data, 12); //copying data into it
        myTable->numTuples++;
        rc = markDirty(myTable->bm, pageHandle); //marking the page as read, hence dirty
        rc = unpinPage(myTable->bm, pageHandle); //unpinning the read page
	}
	else {
	    printf("Total Num Pages value corrupted, scap memory value\n");
	}

	rel->mgmtData=myTable; //putting back updated table data into rel's management data
	free(pageHandle); //freeing the page handle

	return rc;
}

//This function deletes the record
extern RC deleteRecord (RM_TableData *rel, RID id){
	return RC_OK;

}

//This function updates the given record
extern RC updateRecord (RM_TableData *rel, Record *record){
	RC rc = RC_OK;
	Table *myTable = rel->mgmtData; //storing rel's data into table struct
	BM_PageHandle *pageHandle=(BM_PageHandle*)malloc(sizeof(BM_PageHandle)); //allocating memory to pagehandle and checking the condition
	if(pageHandle == NULL) {
        fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
	}

	rc = pinPage(myTable->bm, pageHandle, record->id.page); //pinning the page
	if (rc == RC_OK){
		union Data {
			char *data;
			int slotSize;
		};
		union Data data; //working with unions
		data.slotSize = sizeof(char)*myTable->slotSize; //allocating memory for slotsize to data variable
		memcpy(pageHandle->data + (record->id.slot * myTable->slotSize), record->data, data.slotSize); //copying the data
		rc = markDirty(myTable->bm, pageHandle); //marking the page as read, hence dirty
		if(rc != RC_OK) {
			printf("Could not mark page dirty\n");
		}
		rc = unpinPage(myTable->bm, pageHandle); //unpinning the read page
		if(rc != RC_OK) {
			printf("Could not un pin page\n");
		}
	}
	else {
		printf("Cannot Update record");
	}

	free(pageHandle); //freeing the pageHandle
	return rc;
}
//This function retrieves record based on record ID
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
	RC rc = RC_OK;
	Table *myTable = rel->mgmtData;
	BM_PageHandle *pageHandle=(BM_PageHandle*)malloc(sizeof(BM_PageHandle));
	if(pageHandle == NULL) {
		fprintf(stderr, "Malloc Failed: %s\n", strerror(errno));
	}
	int pageId = id.page;
	rc = pinPage(myTable->bm, pageHandle, pageId); //pinning the page with given page ID

	if (rc == RC_OK) {
		int recPageId = record->id.page; //getting the page and slot ID
		int recSlotId = record->id.slot;
		if(recPageId < 0 || recSlotId < 0) { //checking if the conditions are valid
			printf("Issue with record\n");
		} else {
			recPageId = id.page;
			recSlotId = id.slot;
		}
		union Data {
			char *data;
			int slotSize;
		};
		union Data data; //working with unions
		data.data = record->data;
		data.slotSize = myTable->slotSize; //getting the slotsize
		memcpy(record->data, pageHandle->data+(id.slot*myTable->slotSize), data.slotSize); //copying the slotsize
	}
	else {
		printf("Could not fetch the record\n");
		rc = RC_NOT_OK;
	}
	free(pageHandle); //freeing the pagehandle

	return rc;
}

//This function starts the scan when called
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	RC rc= RC_OK;
	Scanner * scanManager = malloc(sizeof(Scanner));

	int numSlots = ((Table *) rel->mgmtData)-> numSlots; //getting the number of slots from rel
	int numTuples = ((Table *) rel->mgmtData)-> numTuples; //getting the number of tuples from rel
	if(numTuples < 0 || numSlots < 0) {
	    printf("Scrap memory values\n");
	}
	else {
        scanManager->numSlots= ((Table *) rel->mgmtData)-> numSlots;
        scanManager->numTuples= ((Table *) rel->mgmtData)-> numTuples;
	}
	scanManager->currSlot=0; //updating all the other attributes of the scanner struct
	scanManager->currPage=2;
	scanManager->expression=cond;
	scanManager->scannedTuples =0;

	scan->rel = rel;
	scan->mgmtData=scanManager;
	return rc;
}

//This function frees the schema
extern RC freeSchema (Schema *schema) { 
	free(schema);
	return RC_OK;
}

//The next function is called to scan the next input
extern RC next (RM_ScanHandle *scan, Record *record){
	RC rc = RC_OK;
	Scanner *ourscanner = scan -> mgmtData;
	Value *results = malloc(sizeof(char)*sizeof(Value));

	// To get record id
	RID *recordID = malloc(sizeof(RID)); //allocating size of record ID
	recordID -> page = ourscanner -> currPage; 
	recordID -> slot = ourscanner -> currSlot;

	rc = getRecord(scan->rel, *recordID, record); //get record to be scanned
	if (rc == 0) {
		evalExpr(record, scan->rel->schema, ourscanner->expression, &results); //calling the eval expression from rm_serializer

		ourscanner->currSlot != ourscanner->numSlots ? ourscanner->currSlot++ : ourscanner->currSlot + 0;

		if (!(ourscanner->currSlot == ourscanner->numSlots)) { //check to see if they satisfy the conditions
			rc = RC_INCORRECT_PAGENUM;
		} else {
			ourscanner->currSlot = 0;
			ourscanner->currPage = ourscanner->currPage + 1;
		}
		ourscanner->scannedTuples = ourscanner->scannedTuples + 1;
		scan->mgmtData = ourscanner;
		if (results->v.boolV != 1 ) {
			if (ourscanner->scannedTuples >= ourscanner->numTuples) {
				rc = RC_RM_NO_MORE_TUPLES;
			} else {
				rc = next(scan,record); //recursively call next function to finsh scanning all records
			}
		} else {
			rc = RC_OK;
		}
	}
	else {
		rc = RC_NOT_OK;
	}
	return rc;
}

//This function closes the scanner
extern RC closeScan (RM_ScanHandle *scan){
	schemaGlobal = NULL;
	return RC_OK;
}

//This function gets the size of the record
extern int getRecordSize (Schema *schema){
	int size = 0, temp = 0; int i = 0;
	for(i = 0;i<schema->numAttr;++i) {
		switch (schema->dataTypes[i]) { //checking what datatype matches thru switch cases
			case DT_INT: {
				temp = sizeof(int);
				break;
			}
			case DT_STRING: {
				temp = (schema->typeLength[i]) * sizeof(char);
				break;
			}
			case DT_FLOAT: {
				temp = sizeof(float);
				break;
			}
			case DT_BOOL: {
				temp = sizeof(bool);
				break;
			}
			default:
				temp = sizeof(int);
				break;
		}
		size = size + temp;
	}
	return size;
}

//This function creates schema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){


	if(numAttr < 0) {
		printf("Negative number of attributes, scrap memory value\n");
		return NULL;
	}
	else { //setting all the input values to the corresponding variables of the schema
		Schema *schema=(Schema*)malloc(sizeof(Schema));
		schema->attrNames=attrNames;
		schema->dataTypes=dataTypes;
		schema->keyAttrs=keys;
		schema->keySize=keySize;
		schema->typeLength=typeLength;
		schema->numAttr=numAttr;
		return schema;
	}


}

//This function creates record 
extern RC createRecord (Record **record, Schema *schema){
	*record=(Record*)malloc(sizeof(Record)); //allocating size of record
	(*record)->data=(char*)malloc((getRecordSize(schema))); //allocating size of schema
	return RC_OK;
}

//This function frees the record being called
extern RC freeRecord (Record *record){
	free(record->data);
	free(record);
	return RC_OK;
}

//This function gets the attributes of the given record
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
	* value = (Value *) malloc(sizeof(value));
	int offset = 0;
	int length = 0;
	char *attr;
	char *str;
//	printf("attrNum Get; %d\n", schema->numAttr);
//	printf("attrNum_Value Get:%d\n", attrNum);
	if(attrNum < 0){
//		printf("GetAttr is bad\n");
		return RC_NO_TUPLES;
	}
	if(attrNum > schema->numAttr){
//		printf("GetAttr is bad_2\n");
		return RC_NO_TUPLES;
	}

	attrOffset(schema, attrNum, &offset); //calling the attribute offset function
	attr = record -> data + offset;
	(* value)->dt = schema -> dataTypes[attrNum];

	int a = 0;

	if(schema->dataTypes[attrNum] == DT_INT){ //obtaining the datatype of the attribute
		a = 0;
	}
	else if(schema->dataTypes[attrNum] == DT_STRING){
		a = 1;
	}
	else if (schema->dataTypes[attrNum] == DT_FLOAT){
		a = 2;
	}
	else if (schema->dataTypes[attrNum] == DT_BOOL){
		a = 3;
	}

	switch(a){ //copying the datatype size of the attribute into value
		case 0:{
			memcpy(&((*value)->v.intV),attr, sizeof(int));
			break;
		}
		case 1:{
			length=schema->typeLength[attrNum];
			str=(char*)malloc(length+1);
			bzero(str, length+1);
			strncpy(str, attr, length);
			str[length]='\0';
			(*value)->v.stringV=str;
			break;
		}
		case 2:{
			memcpy(&((*value)->v.floatV),attr, sizeof(float));
			break;
		}
		case 3:{
			memcpy(&((*value)->v.boolV),attr, sizeof(bool));
			break;
		}
		default:
			break;
	}


	return RC_OK;
}

//This function sets the attribute of the record
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	char *str; char *attr; int len = 0; int offset = 0;
//	printf("Inside Set Attr\n");
//	printf("attrNum: %d\n", attrNum);
//	printf("schemaAttr: %d\n", schema->numAttr);
//	printf("attrNum; %d\n", schema->numAttr);
//	printf("attrNum_Value:%d\n", attrNum);
	if(attrNum < 0 ) {
//		printf("SetAttr is bad\n");
		return RC_NO_TUPLES;
	}
	if(attrNum > schema->numAttr) {
//		printf("SetAttr is bad_2\n");
		return RC_NO_TUPLES;
	}

//	printf("after if\n");
	attrOffset(schema, attrNum, &offset); //calling the attribute offset function
//	printf("after if\n");
//	printf("Data: %s\n", record->data);
	attr = (record->data + offset);
//	printf("attr: %s\n",attr);

	int a = 0;
	if(schema->dataTypes[attrNum] == DT_INT){
		a = 0;
	}
	else if(schema->dataTypes[attrNum] == DT_STRING){
		a = 1;
	}
	else if (schema->dataTypes[attrNum] == DT_FLOAT){
		a = 2;
	}
	else if (schema->dataTypes[attrNum] == DT_BOOL){
		a = 3;
	}

	switch(a) { //setting the attribute value based on what datatype was obtained
		case 0:
			memcpy(attr, &(value->v.intV), sizeof(int));
			break;

		case 1:
			len = schema -> typeLength[attrNum];
			str = (char *) malloc(len);
			str = value -> v.stringV;
			memcpy(attr, str, len);
			break;

		case 2:
			memcpy(attr, &(value->v.floatV), sizeof(float));
			break;

		case 3:
			memcpy(attr, &(value->v.boolV), sizeof(bool));
			break;
		default:
			break;
	}
	return RC_OK;
}
