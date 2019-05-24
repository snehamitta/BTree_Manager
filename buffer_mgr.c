#include <stdio.h>
#include <stdlib.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr_stat.h"


typedef struct Frame {
    BM_PageHandle *pageHandler; // including the page number and pointer to the area in memory storing the content of the page
    int isDirty; // whether the page has been changed
    int fixCount; // Pinning a page increases its fix count by 1, unpinning the page reduces its fix count.
    int lruCount;
    int fifoCount;
    struct Frame *nf; // pointer to next pageFrame

} Frame;

/* Required data per buffer pool. Will be attached to BM_BufferPool->poolDetail */
typedef struct poolDetail{
    SM_FileHandle *fileHandle;
    Frame *topFrame;
    int pageNumber;
    int readNum;
    int writeNum;
    //void *strategy;
} poolDetail;


int gFix=0;


RC initBufferPool(BM_BufferPool * const bm, const char * const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {

    bm->pageFile = (char*) pageFileName;
    bm->numPages = numPages;
    bm->mgmtData = malloc(sizeof(poolDetail));
    bm->strategy = strategy;
    ((poolDetail *) bm->mgmtData)->fileHandle = malloc(sizeof(SM_FileHandle));
    ((poolDetail *) bm->mgmtData)->topFrame = NULL;
    ((poolDetail *) bm->mgmtData)->pageNumber = 0;
    ((poolDetail *) bm->mgmtData)->readNum = 0;
    ((poolDetail *) bm->mgmtData)->writeNum = 0;
    //((poolDetail *) bm->mgmtData)->strategy = stratData;

    return openPageFile(bm->pageFile, ((poolDetail *) bm->mgmtData)->fileHandle); // locate the first page of the file
}

RC shutdownBufferPool(BM_BufferPool * const bm) {

    RC flag = RC_OK;
    Frame *cf = ((poolDetail *) bm->mgmtData)->topFrame;
    while(cf!=NULL)
    {
        cf->fixCount =0;
        free(cf->pageHandler);
        cf = cf->nf;
    }

    if (forceFlushPool(bm) == RC_OK){
        flag = closePageFile(((poolDetail *) bm->mgmtData)->fileHandle);
        free(cf);
    }
//    ((poolDetail*) bm->mgmtData)->writeNum = 0;
//    ((poolDetail*) bm->mgmtData)->readNum = 0;
    return flag;


}


RC forceFlushPool(BM_BufferPool * const bm) {
    RC rc = RC_OK;

    Frame *cf = ((poolDetail *) bm->mgmtData)->topFrame;
    while (cf != NULL) {
        if (cf->isDirty) {
            rc = writeBlock(cf->pageHandler->pageNum,((poolDetail *) bm->mgmtData)->fileHandle, cf->pageHandler->data);
            cf->isDirty = FALSE;
        }
        cf = cf->nf;
    }

    return rc;
}

//==========================================================

RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page) {

    Frame *cf = ((poolDetail *) bm->mgmtData)->topFrame;
    while (cf != NULL) {
        if (cf->pageHandler->pageNum == page->pageNum) {
            cf->isDirty = TRUE;
            return RC_OK;
        } else {
            cf = cf->nf;
        }
    }
    return -1;
}


Frame* createFrame(BM_BufferPool *bm, Frame * next, int pageNum, int fifoCount, int lruCount) {
    Frame *cf = (Frame*) malloc(sizeof(struct Frame));
    cf->pageHandler = malloc(sizeof(BM_PageHandle));
    cf->isDirty = FALSE;
    cf->fixCount = 0;
    cf->fifoCount= fifoCount;
    cf->pageHandler->pageNum = pageNum;
    cf->lruCount = lruCount;
    cf->nf = next;
    ((poolDetail *) bm->mgmtData)->pageNumber++;
    return cf;
}

Frame *Lru(BM_BufferPool *bm, int pageNum){
    Frame *cf = ((poolDetail *) bm->mgmtData)->topFrame;
    Frame *ff = ((poolDetail *) bm->mgmtData)->topFrame;
    cf = ((poolDetail *) bm->mgmtData)->topFrame;
    bool inPoll = FALSE;

    while (cf != NULL && !inPoll) {
        if (cf->lruCount == bm->numPages - 1) {
            inPoll = TRUE;
        } else {

            cf=cf->nf;
        }
    }

    while (ff != NULL){
        if (ff != cf)
            ff->lruCount ++;
        ff=ff->nf;
    }
    cf->lruCount=0;

    return cf;
}

Frame * fifo(BM_BufferPool *bm, int pageNum){
    Frame *cf = ((poolDetail *) bm->mgmtData)->topFrame;
    Frame *ff = ((poolDetail *) bm->mgmtData)->topFrame;
    cf = ff;
    while(cf != NULL){
        if(cf->fifoCount < ff -> fifoCount && cf->fixCount==0)
            ff = cf;
        cf=cf->nf;
    }

    if(ff == ((poolDetail *) bm->mgmtData)->topFrame){
        cf = createFrame(bm, ((poolDetail *) bm->mgmtData)->topFrame->nf, pageNum,gFix++, 0);
        ((poolDetail *) bm->mgmtData)->topFrame = cf;
        ff=((poolDetail *) bm->mgmtData)->topFrame;
    }
    else{
        cf = ((poolDetail *) bm->mgmtData)->topFrame;
        while(cf->nf != ff)
            cf=cf->nf;
        cf->nf=createFrame(bm, ff->nf, pageNum,gFix++, 0);
        ff = cf->nf;
    }
    return ff;
}

RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) {
    RC flag = RC_OK;
    bool inPoll = FALSE;

    Frame *cf = ((poolDetail *) bm->mgmtData)->topFrame;

    while (cf != NULL && !inPoll) {
        if (cf->pageHandler->pageNum == pageNum)
            inPoll = TRUE;
        else
            cf = cf->nf;
    }

    if (!inPoll) {
        if (((poolDetail*) bm->mgmtData)->pageNumber == 0) { //if the pool is empty
            cf = createFrame(bm, NULL, 0, gFix++, 0);
            ((poolDetail*) bm->mgmtData)->topFrame = cf;
        } else {
            cf = ((poolDetail *) bm->mgmtData)->topFrame;
            while (cf->nf != NULL)
                cf = cf->nf;
            if ((((poolDetail *) bm->mgmtData)->pageNumber >= bm->numPages)){ // if need more place for a new page
                if(bm->strategy == RS_LRU){
                    cf = Lru(bm, pageNum);
                } else if (bm->strategy == RS_FIFO){
                    cf = fifo(bm, pageNum);
                } // read a new page from disk with given stratgy
                // cf = strategy((BM_BufferPool *)bm, pageNum);
            }
            else {
                cf->nf = createFrame(bm, NULL, pageNum,gFix++, 0);
                cf=cf->nf;

                Frame * cfCopy = ((poolDetail*) bm->mgmtData)->topFrame;
                while (cfCopy != NULL && cfCopy != cf){
                    cfCopy->lruCount ++;
                    cfCopy=cfCopy->nf;
                }
            }
        }
        ((poolDetail *) bm->mgmtData)->readNum++;
    } else {
        if (bm->strategy == RS_LRU){
            Frame * cfCopy = ((poolDetail*) bm->mgmtData)->topFrame;
            while (cfCopy != NULL){
                if(cfCopy->lruCount < cf->lruCount) {
                    cfCopy->lruCount ++;
                }
                cfCopy=cfCopy->nf;
            }
            cf->lruCount = 0;
        }
    }
    cf->fixCount++;
    flag=ensureCapacity(pageNum + 1, ((poolDetail *) bm->mgmtData)->fileHandle);
    cf->pageHandler->data = (char*) malloc(sizeof(char) * PAGE_SIZE);
    flag = readBlock(pageNum, ((poolDetail *) bm->mgmtData)->fileHandle, cf->pageHandler->data);
    page->data = cf->pageHandler->data;
    page->pageNum=pageNum;
    cf->pageHandler->pageNum=pageNum;
    //printPageContent(page);

    return flag;
}


RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) {

    Frame *cf = ((poolDetail *) bm->mgmtData)->topFrame;
    while (cf != NULL) {
        if (cf->pageHandler->pageNum == page->pageNum) {
            if(cf->fixCount > 0){
                cf->fixCount--;
                if (cf->isDirty) {
                    forcePage(bm, page);
                }
            } else{
                return -1;
            }
        }
        cf = cf->nf;
    }
    return RC_OK;
}

RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page) {

    RC rc = RC_OK;
    Frame *cf = ((poolDetail *) bm->mgmtData)->topFrame;
    while (cf != NULL) {
        if (cf->pageHandler->pageNum == page->pageNum){
            rc = writeBlock(cf->pageHandler->pageNum,((poolDetail *) bm->mgmtData)->fileHandle, cf->pageHandler->data);
            break;
        }
        else{
            cf = cf->nf;
        }
    }
    ((poolDetail *) bm->mgmtData)->writeNum++;

    return rc;
}

//============================================================
PageNumber *getFrameContents(BM_BufferPool * const bm) {
    Frame *cf = ((poolDetail*) bm->mgmtData)->topFrame;
    PageNumber *pageN = calloc(bm->numPages, sizeof(PageNumber));

    for(int i=0; i < bm->numPages; i++)
        pageN[i] = -1; // no page

    for (int i = 0; i < bm->numPages && cf != NULL; i++) {
        if (cf->pageHandler->pageNum != NO_PAGE){
            pageN[i] = cf->pageHandler->pageNum;
        }
        else {
            pageN[i] = -1; // no-page
        }
        cf = cf->nf;
    }
    return pageN;
}

bool * getDirtyFlags(BM_BufferPool * const bm) {
    Frame *cf = ((poolDetail*) bm->mgmtData)->topFrame;
    bool * dirties = calloc(bm->numPages, sizeof(bool));

    for(int i=0;i < bm->numPages;i++)
        dirties[i] = FALSE;

    for (int i = 0; i < bm->numPages && cf != NULL; i++) {
        if (cf->isDirty)
            dirties[i] = TRUE;
        else
            dirties[i] = FALSE;
        cf = cf->nf;
    }
    return dirties;
}

int *getFixCounts(BM_BufferPool * const bm) {
    Frame *currentFrame = ((poolDetail*) bm->mgmtData)->topFrame;
    int *fixCount = calloc(bm->numPages, sizeof(int));

    for(int i = 0;i<bm->numPages;i++){
        fixCount[i] = 0;
    }

    for (int i = 0; i < bm->numPages && currentFrame != NULL; i++) {
        fixCount[i] = currentFrame->fixCount;
        currentFrame = currentFrame->nf;
    }
    return fixCount;
}

int getNumReadIO(BM_BufferPool * const bm) {
    if (bm->mgmtData != NULL){
        return ((poolDetail*) bm->mgmtData)->readNum;
    }
    else {
        return 0;
    }
}

int getNumWriteIO(BM_BufferPool * const bm) {
    if (bm->mgmtData != NULL){
        return ((poolDetail*) bm->mgmtData)->writeNum;
    }
    else {
        return 0;
    }
}
