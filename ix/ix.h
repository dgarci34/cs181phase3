#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <iostream>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
#define IX_CREATE_ERROR 1
#define IX_DESTROY_ERROR 2
#define IX_HANDLE_IN_USE 3
#define IX_FILE_DN_EXIST 4
#define IX_OPEN_FAILED 5
#define IX_NOT_OPEN 6
#define NODE_KEY_EMPTY 0b0000
#define NODE_KEY_POINTER 0b0001
#define NODE_KEY_OCCUPIED 0b0010

using namespace std;

class IX_ScanIterator;
class IXFileHandle;

typedef enum{ child =0, index, root }NodeType;

typedef struct{
  unsigned short leftNodeOffset;        //pointer to neighbors
  unsigned short rightNodeOffset;
  unsigned short keyOffset[120];        //fanout of 120
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
    void setNodeAtOffset(unsigned off, Node node);
    void getNodeAtOffset(unsigned off, Node node);
    unsigned splitNodeAtOffset(unsigned off);
    unsigned mergeLeftNodeAtOffset(unsigned off);
    unsigned mergeRightNodeAtOffset(unsigned off);
    void setKey(unsigned keyNum, void* data, unsigned size);
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
