#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <iostream>
#include <stdio.h>
#include <string.h>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
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

using namespace std;

class IX_ScanIterator;
class IXFileHandle;

typedef enum{ leaf =0, intermidate }NodeType;

typedef struct{
  unsigned short leftNodePage;        //pointer to neighbors
  unsigned short ParentNodePage;
  unsigned short rightNodePage;
  unsigned short keyOffset[92];        //fanout of 120
  unsigned short numOfEntries;
  NodeType nodeType;
}Node;

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
    //private node methods
    void setNodeOnPage(void * page, Node node);
    Node getNodeOnPage(void * page);
    unsigned splitNodeAtPage(unsigned pageNum);
    unsigned mergeLeftNodeAtPage(unsigned pageNum);
    unsigned mergeRightNodeAtPage(unsigned pageNum);
    void setKey(unsigned keyNum, void* data, unsigned size);
	RC initializeBTree(void * newPage,Node &newNode, IXFileHandle &ixfileHandle,
                       const void * data, const RID &rid, const Attribute &attribute);
    unsigned getKeySize(AttrType att, const void* key);
    bool fileExists(const string &fileName);
    bool pfmPtr;
    static IndexManager *_index_manager;
    PagedFileManager * pfm;
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
};



class IXFileHandle {
    public:

    string fileName;
	PageNum rootPage; //where the root page lies

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    RC writePage(PageNum pageNum, void * data);
    RC readPage(PageNum pageNum, void * data);
    RC appendPage(void * data);

    unsigned getNumberOfPages();
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
