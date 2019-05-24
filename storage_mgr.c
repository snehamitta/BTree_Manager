#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include "storage_mgr.h"
#include "dberror.h"
/*Sneha Mitta*/

/* GLOBAL VARIABLES */

int initialize = 0;
//int sysInit = 0;

/* manipulating page files */

void initStorageManager (void){
    initialize = 1; /*initializing the storage manager*/
    printf("The Storage Manager has been Initialized\n");
}

RC createPageFile (char *fileName){
    FILE * create_file = fopen(fileName, "w"); /*creating a new file with given filename*/
    if (create_file == NULL){ /*checking id file created is null*/
        return RC_FILE_NOT_FOUND;
    }
    else{
        char *page = (char *)malloc(PAGE_SIZE * sizeof(char)); /*allocating a page of size PAGE_SIZE*/

        for (int i=0; i< PAGE_SIZE; i++){
            page[i] = '\0';
        }

        //page[0] = '\0'; /*filling page with \0 bytes*/
        int check = fwrite(page, sizeof(char), PAGE_SIZE, create_file);/*writing the above into our page*/
        if (check != PAGE_SIZE){ /*checking page size*/
            return RC_WRITE_FAILED;
        }


        fclose(create_file); /*closing the created file*/
        free(page); /*freeing memory*/
        page = NULL;
        return RC_OK;
    }

}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    FILE * open_file = fopen(fileName, "r+"); /*opening given filename*/
    if (open_file == NULL){ /*checking if filename exists*/
        return RC_FILE_NOT_FOUND;
    }
    else{
        fseek(open_file, 0, SEEK_END); /*seeking the end of the file*/
        long int file_size = ftell(open_file); /*getting the size of the file*/
        fHandle -> fileName = fileName; /*giving name of file*/
        fHandle -> totalNumPages = (file_size/PAGE_SIZE); /*obtaining total numer of pages by dividing the size of file by PAGE_SIZE*/
        fHandle -> curPagePos = 0; /*setting current page position to 0*/
        fHandle -> mgmtInfo = open_file; /*storing the openfile in mgmtInfo*/
        return RC_OK;
    }
}

RC closePageFile (SM_FileHandle *fHandle){
    int close_file; /*initializing variable*/
    close_file = fclose(fHandle -> mgmtInfo); /*closing file which was open*/
    fHandle = NULL;
    if (close_file == 0){ /*checking if file is closed*/
        return RC_OK;
    }
    else{
        return RC_FILE_NOT_FOUND;
    }

}

RC destroyPageFile (char *fileName){
    int delete_file; /*initializing variable*/

    FILE * file = fopen(fileName, "r");
    if (!file) {
        return RC_FILE_NOT_FOUND;
    } else {
        fclose(file);
        delete_file = remove(fileName); /*removing the file in question*/
        if (delete_file == 0) {/*checking if file is deleted*/
            return RC_OK;
        } else {
            return RC_FILE_NOT_DESTROY;
        }
    }

}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (initialize){
        if(fHandle->mgmtInfo == NULL){ // check whether there is a open file
            printf("FileHandle not initialized\n");
            return RC_FILE_HANDLE_NOT_INIT;
        } else if (pageNum > fHandle->totalNumPages || pageNum < 0) { // verify page number
            printf("Incorrect page number\n");
            return RC_READ_NON_EXISTING_PAGE;
        } else {
            fseek((FILE*)fHandle->mgmtInfo, pageNum*PAGE_SIZE, SEEK_SET);
            size_t size = fread(memPage, 1, PAGE_SIZE, (FILE *)fHandle->mgmtInfo);
            fHandle->curPagePos=pageNum;

            if (size < PAGE_SIZE){ // check whether buffer has been flushed out
                printf("Error reading block\n");
                return RC_READ_FAILED;
            }

        }
        return RC_OK;
    }
    else {
        printf("StorageManager is not initialized\n");
        return RC_SM_NOT_INIT;
    }

}

int getBlockPos (SM_FileHandle *fHandle){
    if (initialize){
        return fHandle->curPagePos;
    } else {
        printf("StorageManager is not initialized\n");
        return RC_SM_NOT_INIT;
    }
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (initialize){
        return readBlock(0, fHandle, memPage); // set the page number to 0
    }
    else {
        printf("StorageManager is not initialized\n");
        return RC_SM_NOT_INIT;
    }
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (initialize){
        return readBlock(fHandle->curPagePos - 1, fHandle, memPage); // set the page number to previous position
    } else {
        printf("StorageManager is not initialized\n");
        return RC_SM_NOT_INIT;
    }
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (initialize) {
        return readBlock(fHandle->curPagePos, fHandle, memPage); // set the page number to current position
    } else {
        printf("StorageManager is not initialized\n");
        return RC_SM_NOT_INIT;
    }
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (initialize){
        return readBlock(fHandle->curPagePos + 1, fHandle, memPage); // set postion to new page

    } else {
        printf("StorageManager is not initialized\n");
        return RC_SM_NOT_INIT;
    }
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (initialize){
        return readBlock(fHandle->totalNumPages-1, fHandle, memPage); // set the postion to the last block

    } else {
        printf("StorageManager is not initialized\n");
        return RC_SM_NOT_INIT;
    }
}

/*Write a page to disk using either the current position
or an absolute position.*/
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (initialize){
//        printf("inside writebloc\n");
//        printf("totalNumPages: %d\n", fHandle->totalNumPages);
        if(fHandle == NULL){ //checks for file
            return RC_FILE_NOT_FOUND;
        } else {
            if (pageNum < 0 || fHandle->totalNumPages <= pageNum){ //checks if page number is less than 0
                return RC_WRITE_FAILED;
            } else {
//                printf("Inside writeBlock\n");
                fseek(fHandle->mgmtInfo,PAGE_SIZE*pageNum, SEEK_SET);
                size_t lengthOfWrite = fwrite( memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo );
                if(lengthOfWrite < PAGE_SIZE){ //checking the length of the page
                    return RC_WRITE_FAILED;
                }else {
                    fHandle->curPagePos = pageNum;
                }
            }
        }
        return RC_OK;
    } else {
        printf("StorageManager is not initialized\n");
        return RC_SM_NOT_INIT;
    }
}


RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (initialize){
        return writeBlock(fHandle -> curPagePos, fHandle, memPage); //writing the buffer into the current page
    }else {
        return RC_SM_NOT_INIT;
    }
}

/*Increase the number of pages in the file by one.
The new last page should be filled with zero bytes.*/

RC appendEmptyBlock (SM_FileHandle *fHandle){
    if (initialize){
        if(fHandle->mgmtInfo == NULL) //checking to see if the file is null
        {

            return RC_FILE_NOT_FOUND; // returns file not found
        } else {
            char *newPage = (char *)malloc(PAGE_SIZE);
            for (int i=0; i< PAGE_SIZE; i++){
                newPage[i] = '\0';
            }
            fHandle->totalNumPages++;
            RC rc = writeBlock(fHandle->totalNumPages-1, fHandle, newPage);
            free(newPage);
            return rc;
        }
    } else {
        return RC_SM_NOT_INIT;
    }
}


RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    int complement = numberOfPages - fHandle->totalNumPages;
    if(complement == 0) {
//        printf("Number of pages up to date\n");
        return RC_OK;
    }
    else {
        for(int i = 0; i<complement;i++) {
            appendEmptyBlock(fHandle);
            fHandle->totalNumPages++;
        }
        if(fHandle->totalNumPages > INT_MAX) {
            printf("Issue with num pages\n");
        }
        return  RC_OK;
    }
}







