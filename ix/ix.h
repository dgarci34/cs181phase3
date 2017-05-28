#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <stack>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan
#define IX_CREATE_ERROR 1
#define IX_DESTROY_ERROR 2
#define IX_HANDLE_IN_USE 3
#define IX_FILE_DN_EXIST 4
#define IX_OPEN_FAILED 5
#define IX_NOT_OPEN 6
#define IX_INIT_FAILED 7
#define IX_APPEND_FAILED 8
#define IX_WRITE_FAILED 9
#define IX_PAGE_DN_EXIST 10
#define IX_READ_FAILED 11
#define IX_ENTRY_DOES_NOT_EXIST 12
#define IX_TYPE_ERROR 13
#define IX_KEY_DOES_NOT_EXIST 14
#define IX_CONFLICTING_TYPES 15
#define IX_SPLIT_FAILED 16
#define IX_PRINT_LEAF_NODE_ERROR 17
#define IX_PRINT_INTERNAL_NODE_ERROR 18
#define IX_TREE_ERROR 19
#define IX_TARGET_DOES_EXIST 20
#define IX_NOT_FOUND 21
#define IX_SCAN_ERROR 22

#define NO_PAGE 0
#define NO_ENTRIES 0
#define FIRST_ENTRY 0
#define FIRST_RID 0
#define INITIAL_HEIGHT 0
#define META_PAGE 0
#define INITIAL_PAGE 1

#define LESS_THAN 0
#define EQUAL_TO 1
#define GREATER_THAN 2

using namespace std;

class IX_ScanIterator;
class IXFileHandle;

typedef enum{ alive = 0, dead } ixStatus;

// MetaNode
// used for printBTree()
// store the necssary information to print B+ tree
typedef struct MetaNode {
    unsigned pageNum;
    unsigned height;
} MetaNode;

// IndexID
typedef struct IndexId {
    uint32_t pageId;
    uint32_t entryId;
} IndexId;

// Meta
typedef struct MetaHeader {
    unsigned rootPage;
    unsigned numOfInternalNodes;
    unsigned numOfLeafNodes;
    unsigned height;
    AttrType type;
} MetaHeader;

// Internal Node
typedef struct InternalNodeHeader {
    unsigned numOfEntries;
    unsigned freeSpaceOffset;
    unsigned parentPage;
} InternalNodeHeader;

typedef struct InternalNodeEntry {
    unsigned offset;
    unsigned length;
    unsigned leftChild;
    unsigned rightChild;
} InternalNodeEntry;

// Leaf Node
typedef struct LeafNodeHeader {
    unsigned numOfEntries;
    unsigned freeSpaceOffset;
    unsigned parentPage;
    unsigned leftNode;
    unsigned rightNode;
} LeafNodeHeader;

typedef struct LeafNodeEntry {
    unsigned offset;
    unsigned numberOfRIDs;
    unsigned length;
    ixStatus status;
} LeafNodeEntry;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
    friend class IX_ScanIterator;
    //private node methods
    static IndexManager *_index_manager;
    PagedFileManager * pfm;
    bool pfmPtr;

    // **************************** Helper Function **********************t ******
    bool fileExists(const string &fileName);
    void setKey(unsigned keyNum, void* data, unsigned size);
    void printKey(const void * key, AttrType attrType);
    void printRids(void * page, LeafNodeEntry leafNodeEntry);
    void showLeaf(void * page, AttrType attrType);
    void showLeafOffsetsAndLengths(void * page);
    unsigned getKeySize(AttrType att, const void* key);
    RC search(IXFileHandle &ixfileHandle, void *key, FILE * pfile, IndexId * indexId);

    // ****************************Node helper functions************************
    void initializeBTree(IXFileHandle ixfileHandle, AttrType attrType);

    //get/set struct helpers
    MetaHeader getMetaHeader(void * page);
    LeafNodeHeader getLeafNodeHeader(void * page);
    InternalNodeHeader getInternalNodeHeader(void * page);
    LeafNodeEntry getLeafNodeEntry(void * page, unsigned slotNumber);
    InternalNodeEntry getInternalNodeEntry(void * page, unsigned slotNumber);
    void setMetaHeader(void * page, MetaHeader metaHeader);
    void setLeafNodeHeader(void * page, LeafNodeHeader leafNodeHeader);
    void setInternalNodeHeader(void * page, InternalNodeHeader internalNodeHeader);
    void setLeafNodeEntry(void * page, LeafNodeEntry leafNodeEntry, unsigned slotNumber);
    void setInternalNodeEntry(void * page, InternalNodeEntry internalNodeEntry, unsigned slotNumber);

    //get/set key helpers
    void setLeafKeyAndRidAtOffset(void * page, const Attribute &attribute, const void *key, const RID &rid, unsigned offset, unsigned keylength);
    void setInternalKeyAtOffset(void * page, const Attribute &attribute, const void *key, unsigned keylength, unsigned offset);
    void getKeyAtOffset(void * page, void * dest, unsigned offset, unsigned length);
    void addAdditionalRID(void * page,LeafNodeHeader leafNodeHeader, LeafNodeEntry leafNodeEntry, RID newRid, unsigned entryPos);

    //comparison helpers
    RC compareInts(const void * key, const void * toCompareTo);
    RC compareReals(const void * key, const void * toCompareTo);
    RC compareVarChars(const void * key, const void * toCompareTo);

    //size helpers
    unsigned getSizeofLeafEntry(const void * key, AttrType attrType);
    unsigned getLeafFreeSpace(LeafNodeHeader leafNodeHeader);
    unsigned getInternalFreeSpace(InternalNodeHeader internalNodeHeader);

    //treebalance helpers
    RC splitLeafAtEntry(void * page, unsigned pageNum, MetaHeader &metaHeader,LeafNodeHeader &leafNodeHeader, IXFileHandle &ixfileHandle, unsigned midpoint);
    void splitInternalAtEntry(void * page, MetaHeader &metaHeader, InternalNodeHeader &internalNodeHeader, IXFileHandle &ixfileHandle, unsigned midpoint);
    RC pushUpSplitKey(unsigned pageNum, MetaHeader &metaHeader, IXFileHandle ixfileHandle, void * key, unsigned leftChildPage, unsigned rightChildPage);
    void fixPageOrderSplit(MetaHeader &metaHeader, void * right, IXFileHandle &ixfileHandle);
    void fixPageOrderSplitAndHeightIncrease(MetaHeader &metaHeader, void * right, IXFileHandle &ixfileHandle);

    //inserting to leaf helpers
    void injectBefore(void * page, LeafNodeHeader &leafNodeHeader, const void * key, RID rid, AttrType attrType);
    void injectBetween(void * page, unsigned position, LeafNodeHeader &leafNodeHeader, const void * key, RID rid, AttrType attrType);
    void injectAfter(void * page, LeafNodeHeader &leafNodeHeader, const void * key, RID rid, AttrType attrType);

    RC searchLeafNode(IXFileHandle ixfileHandle, unsigned pageNum, AttrType type, const void * key, RID rid, IndexId * indexId);
    unsigned getNextNodePageNum(IXFileHandle ixfileHandle,  unsigned pageNum, AttrType type, const void * key, RID rid);
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        friend class IndexManager;

      private:
        IndexManager * im;

        uint32_t currPage;
        uint32_t currEntry;
        uint32_t currRid;

        uint32_t totalLeaves;
        uint16_t totalEntry;

        AttrType attrType;

        void * pageData;
        const void * low;
        const void * high;
        bool lowInc;
        bool highInc;
        bool infiniteLow;
        bool infiniteHigh;
        bool allocatedPage;

        IXFileHandle *ixfh;
        vector<IndexId> skipList; //may have to be changed to leaf ids

        // helper functions
        RC scanInit(IXFileHandle &ixfH,
                const Attribute &attr,
                const void      *lowKey,
                const void      *highKey,
                bool  lowKeyInclusive,
                bool  highKeyInclusive);

        RC scanCurrPage(RID &rid, void *key);

        RC getNextEntry();
        RC getNextPage();
        RC handleMovedRecord(bool &status, const RID rid, void *data);
        bool checkScanCondition();
        RC checkScanCondition(bool &result, const RID rid);
        bool checkScanCondition(int, CompOp, const void*);
        bool checkScanCondition(float, CompOp, const void*);
        bool checkScanCondition(char*, CompOp, const void*);

};



class IXFileHandle {
    public:

    string fileName;
	PageNum rootPage; //where the root page lies

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // helper functions
    RC writePage(PageNum pageNum, void * data);
    RC readPage(PageNum pageNum, void * data);
    RC appendPage(void * data);

    unsigned getNumberOfPages();

    MetaHeader fhGetMetaHeader(void * page);
    LeafNodeHeader fhGetLeafNodeHeader(void * page);
    InternalNodeHeader fhGetInternalNodeHeader(void * page);
    LeafNodeEntry fhGetLeafNodeEntry(void * page, unsigned slotNumber);
    InternalNodeEntry fhGetInternalNodeEntry(void * page, unsigned slotNumber);
    RID getRid(void * page, unsigned offset);
    void setMetaNode(MetaNode * mNodeEntry, unsigned pageNum, unsigned height);

    RC fhPrintLeafNode(IXFileHandle ixfileHandle, unsigned height, unsigned pageNum, AttrType type, stack<MetaNode> * pageNumStack);
    RC fhPrintInternalNode(IXFileHandle ixfileHandle, unsigned height, unsigned pageNum, AttrType type, stack<MetaNode> * pageNumStack);

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    friend class IndexManager;
    private:
    //private helpers
    FILE * _file;
    void setFd(FILE * file);
    FILE * getFd();

};

#endif
