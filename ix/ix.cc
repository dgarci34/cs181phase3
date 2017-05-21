
#include "ix.h"
#include <sys/stat.h>
#include <sys/types.h>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
    if (pfmPtr)
        free(pfm);
}

RC IndexManager::createFile(const string &fileName)
{
    //cout<<"creating file\n";
    pfm = PagedFileManager::instance();
    if (pfm->createFile(fileName))
        return IX_CREATE_ERROR;
    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    pfm = PagedFileManager::instance();
    if (pfm->destroyFile(fileName))
        return IX_DESTROY_ERROR;
    return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
  //check for file existance
  if(!fileExists(fileName.c_str()))
      return IX_FILE_DN_EXIST;
    //check for a double open
  if(ixfileHandle.getFd() != NULL)
    return IX_HANDLE_IN_USE;
  //open file and attach
  FILE *pFile;
  pFile = fopen(fileName.c_str(), "rb+");
  // If we fail, error
  if (pFile == NULL)
      return IX_OPEN_FAILED;
  ixfileHandle.setFd(pFile);
  return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FILE *pFile = ixfileHandle.getFd();

    // If not an open file, error
    if (pFile == NULL)
        return IX_NOT_OPEN;

    // Flush and close the file
    fclose(pFile);
    ixfileHandle.setFd(NULL);
    return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){
    //if no file error
    if (ixfileHandle.fileName == "")
      return IX_FILE_DN_EXIST;

    //if no pages yet beging new tree
    if (!ixfileHandle.getNumberOfPages())
      initializeBTree(ixfileHandle);

    //get meta header
    void * pageData =  malloc(PAGE_SIZE);
    if (ixfileHandle.readPage(META_PAGE, pageData))
      return IX_READ_FAILED;
    MetaHeader mHeader = getMetaHeader(pageData);

    //begin at the root
    ixfileHandle.readPage(mHeader.rootPage, pageData);
    unsigned childPageNum = 0;
    bool childFound = false;

    //this will loop through intrnal nodes if any exists
    InternalNodeHeader iHeader;
    InternalNodeEntry iEntry;
    void * keyInMemory;                 //key you extract and compare to
    for (unsigned i =0; i < mHeader.height; i--){
      iHeader = getInternalNodeHeader(pageData);
      //compare through internal node values
      for (unsigned j =0; j < iHeader.numOfEntries; j++){
        iEntry = getInternalNodeEntry(pageData, j);
        //compare based on type
        switch (attribute.type) {
          case TypeInt:
          {
            keyInMemory = malloc(INT_SIZE);
            getKeyAtOffset(pageData, keyInMemory,iEntry.offset, iEntry.length);
            if (compareInts(key, keyInMemory) == LESS_THAN_OR_EQUAL){
              childPageNum = iEntry.leftChild;
              childFound = true;
              free(keyInMemory);
            }
            //if greater than and at the last follow rightchild
            else if(j == iHeader.numOfEntries -1){
              childPageNum = iEntry.rightChild;
              childFound = true;
              free(keyInMemory);
            }
            break;
          }
          case TypeReal:
          {
            keyInMemory = malloc(REAL_SIZE);
            getKeyAtOffset(pageData, keyInMemory,iEntry.offset, iEntry.length);
            if (compareReals(key, keyInMemory) == LESS_THAN_OR_EQUAL){
              childPageNum = iEntry.leftChild;
              childFound = true;
              free(keyInMemory);
            }
            //if greater than and at the last follow rightchild
            else if(j == iHeader.numOfEntries -1){
              childPageNum = iEntry.rightChild;
              childFound = true;
              free(keyInMemory);
            }
            break;
          }
          case TypeVarChar:
          {
            keyInMemory = malloc(iEntry.length);
            getKeyAtOffset(pageData, keyInMemory,iEntry.offset, iEntry.length);
            if (compareVarChars(key, keyInMemory) == LESS_THAN_OR_EQUAL){
              childPageNum = iEntry.leftChild;
              childFound = true;
              free(keyInMemory);
            }
            //if greater than and at the last follow rightchild
            else if(j == iHeader.numOfEntries -1){
              childPageNum = iEntry.rightChild;
              childFound = true;
              free(keyInMemory);
            }
            break;
          }
        }
        if (childFound){
          //found locaion for child node, check next height
          ixfileHandle.readPage(childPageNum, pageData);
          childFound = false;
          break;
        }
      }
    }

    //traversed through internal nodes now at leaf
    LeafNodeHeader lHeader = getLeafNodeHeader(pageData);

    //if there is no space we split the node
    unsigned potentialSize = getSizeofLeafEntry(key, attribute.type);
    unsigned freeSpaceOnPage = getLeafFreeSpace(lHeader);
    return -1;
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const{
  //there must be a tree
  if (!ixfileHandle.getNumberOfPages())
    return;
  cout<< "----------------BTREE " <<ixfileHandle.fileName<< "--------------------- \n\n";
  MetaHeader mHeader;
  memcpy(&mHeader, ixfileHandle._file, sizeof(MetaHeader));
/*  void * pageData = malloc(PAGE_SIZE);
  ixfileHandle.readPage(META_PAGE, pageData);
  MetaHeader mHeader;
  IXFileHandle *ix = &ixfileHandle;
  mHeader = ix->getMetaHeader(pageData);

  //get meta
  free(pageData);*/
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}

IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    _file = NULL;
}

IXFileHandle::~IXFileHandle()
{
  _file = NULL;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
  ixReadPageCounter = readPageCount;
  ixWritePageCounter = writePageCount;
  ixAppendPageCounter = appendPageCount;
    return SUCCESS;
}

unsigned IXFileHandle::getNumberOfPages(){
    // Use stat to get the file size
    struct stat sb;
    if (fstat(fileno(_file), &sb) != 0) //no pages
        return 0;
    // Filesize is always PAGE_SIZE * number of pages
    return (sb.st_size / PAGE_SIZE);
}

void IXFileHandle::setFd(FILE * file){
    _file = file;           //set file pointer
}

FILE * IXFileHandle::getFd(){
    return _file;           //return the file pointer
}

RC IXFileHandle::writePage(PageNum pageNum, void * data)
{
    // Check if the page exists
    if (getNumberOfPages() < pageNum)
        return IX_PAGE_DN_EXIST;

    // Seek to the start of the page
    if (fseek(_file, PAGE_SIZE * pageNum, SEEK_SET))
        return IX_WRITE_FAILED;

    // Write the page
    if (fwrite(data, 1, PAGE_SIZE, _file) == PAGE_SIZE)
    {
        // Immediately commit changes to disk
        fflush(_file);
        ixWritePageCounter++;
        return SUCCESS;
    }

    return IX_WRITE_FAILED;
}

RC IXFileHandle::readPage(PageNum pageNum, void * data)
{
    // If pageNum doesn't exist, error
    if (getNumberOfPages() < pageNum)
        return IX_PAGE_DN_EXIST;

    // Try to seek to the specified page
    if (fseek(_file, PAGE_SIZE * pageNum, SEEK_SET))
        return IX_READ_FAILED;

    // Try to read the specified page
    if (fread(data, 1, PAGE_SIZE, _file) != PAGE_SIZE)
        return IX_READ_FAILED;

    ixReadPageCounter++;
    return SUCCESS;
}

RC IXFileHandle::appendPage(void * data)
{
    // Seek to the end of the file
    if (fseek(_file, 0, SEEK_END))
        return IX_APPEND_FAILED;

    // Write the new page
    if (fwrite(data, 1, PAGE_SIZE, _file) == PAGE_SIZE)
    {
        fflush(_file);
        ixAppendPageCounter++;
        return SUCCESS;
    }
    return IX_APPEND_FAILED;
}

// **************************** Helper Function ****************************
bool IndexManager::fileExists(const string &fileName)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}

//return the length of the key
unsigned IndexManager::getKeySize(AttrType att,const  void * key){
    switch (att) {
        case TypeReal:
            return REAL_SIZE;
            break;
        case TypeInt:
            return INT_SIZE;
        case TypeVarChar:
            int * size = (int *)key;
            return size[0] + INT_SIZE; //the size plus the leading int
            break;
    }
  return -1;        //should not reach here
}

RC IndexManager::search(IXFileHandle &ixfileHandle, void *key, FILE * pfile, IndexId * indexId)
{
    // get the root page number and the height of the tree
    MetaHeader tempMetaHeader;
    void * page = malloc(PAGE_SIZE);
    unsigned rootPage;
    unsigned height;


    if (ixfileHandle.readPage(0, page))
        return IX_READ_FAILED;

    tempMetaHeader = getMetaHeader(page);
    rootPage = tempMetaHeader.rootPage;
    height = tempMetaHeader.height;

    // read the root page and compare the key
    LeafNodeHeader tempLeafNodeHeader;
    LeafNodeEntry tempLeafNodeEntry;

    if (ixfileHandle.readPage(rootPage, page))
        return IX_READ_FAILED;

    // Tree has only 1 node (root)
    if (height == 0) {
        tempLeafNodeHeader = getLeafNodeHeader(page);

        // no entry in the B+ tree
        if (tempLeafNodeHeader.numOfEntries == 0)
        {
            free(page);
            return IX_ENTRY_DOES_NOT_EXIST;
        }

        // at least 1 entry in the tree
        for (unsigned i = 0; i < tempLeafNodeHeader.numOfEntries; i++)
        {
            tempLeafNodeEntry = getLeafNodeEntry(page, i);

            // compare the key if the entry is NOT deleted
            if (tempLeafNodeEntry.status == alive)
            {
                switch (tempMetaHeader.type) {
                    case TypeInt:
                    {
                        int * tempKey = (int*) key;
                        int entryKey;
                        memcpy(&entryKey, (page + tempLeafNodeEntry.offset), sizeof(int));

                        // target key is found
                        if (entryKey == tempKey[0])
                        {
                            indexId->pageId = rootPage;
                            indexId->entryId = i;

                            free(page);
                            return SUCCESS;
                        }

                        // target key does not exist
                        if (entryKey > tempKey[0])
                        {
                            free(page);
                            return IX_KEY_DOES_NOT_EXIST;
                        }
                        break;
                    }

                    case TypeReal:
                    {
                        break;
                    }

                    case TypeVarChar:
                    {
                        break;
                    }

                    default:
                    {
                        free(page);
                        return IX_TYPE_ERROR;
                    }
                }

            }
        }
    }

    InternalNodeHeader tempInternalNodeHeader;
    InternalNodeEntry tempInternalNodesEntry;

    // free
    free(page);

    return -1;
}

// ****************************Node helper functions************************
// initialize B+ Tree
void IndexManager::initializeBTree(IXFileHandle ixfileHandle){
    MetaHeader mHeader;
    LeafNodeHeader lHeader;
    void * metaPage = malloc(PAGE_SIZE);
    void * firstPage = malloc(PAGE_SIZE);
    mHeader.rootPage =INITIAL_PAGE;      //the first node in will be both a leaf and the root
    mHeader.numOfLeafNodes = 1;
    mHeader.numOfInternalNodes = NO_ENTRIES;
    mHeader.height = INITIAL_HEIGHT;
    lHeader.numOfEntries = NO_ENTRIES;
    lHeader.parentPage = NO_PAGE;         //pointer pages are invalid at the begining
    lHeader.leftNode = NO_PAGE;
    lHeader.rightNode = NO_PAGE;
    lHeader.freeSpaceOffset = PAGE_SIZE;  //no entries
    setMetaHeader(metaPage, mHeader);
    setLeafNodeHeader(firstPage, lHeader);
    ixfileHandle.appendPage(metaPage);      //commit initial pages
    ixfileHandle.appendPage(firstPage);
    free(metaPage);
    free(firstPage);
}

//returns the metaHeader from page 0
MetaHeader IndexManager::getMetaHeader(void * page){
    MetaHeader mHeader;
    memcpy(&mHeader,page,sizeof(MetaHeader));
    return mHeader;
}

//get leaf node header from passed in passed in page
LeafNodeHeader IndexManager::getLeafNodeHeader(void * page){
    LeafNodeHeader lHeader;
    memcpy(&lHeader, page, sizeof(LeafNodeHeader));
    return lHeader;
}

//get InternalNodeHeader
InternalNodeHeader IndexManager::getInternalNodeHeader(void * page){
    InternalNodeHeader iHeader;
    memcpy(&iHeader, page, sizeof(InternalNodeHeader));
    return iHeader;
}

//return a specific leaf node entry
LeafNodeEntry IndexManager::getLeafNodeEntry(void * page, unsigned slotNumber){
    LeafNodeEntry lEntry;
    memcpy(&lEntry, page + sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry) * slotNumber), sizeof(LeafNodeEntry));
    return lEntry;
}

//return a specific Internal node entry
InternalNodeEntry IndexManager::getInternalNodeEntry(void * page, unsigned slotNumber){
    InternalNodeEntry iEntry;
    memcpy(&iEntry, page + sizeof(InternalNodeHeader) + (sizeof(InternalNodeEntry) * slotNumber), sizeof(InternalNodeEntry));
    return iEntry;
}

//set a metaHeader to a page
void IndexManager::setMetaHeader(void * page, MetaHeader metaHeader){
    memcpy(page, &metaHeader, sizeof(MetaHeader));
}

//used to set  leaf header to a page
void IndexManager::setLeafNodeHeader(void * page, LeafNodeHeader leafNodeHeader){
    memcpy(page, &leafNodeHeader, sizeof(LeafNodeHeader));
}

//used to set an internal header to a page
void IndexManager::setInternalNodeHeader(void * page, InternalNodeHeader internalNodeHeader){
    memcpy(page, &internalNodeHeader, sizeof(InternalNodeHeader));
}

//used to set a leaf entry to a specific slot
void IndexManager::setLeafNodeEntry(void * page, LeafNodeEntry leafNodeEntry, unsigned slotNumber){
    memcpy(page + sizeof(LeafNodeHeader) + (slotNumber * sizeof(LeafNodeEntry)), &leafNodeEntry, sizeof(LeafNodeEntry));
}

//used to set an internal node entry to a specific slot
void IndexManager::setInternalNodeEntry(void * page, InternalNodeEntry internalNodeEntry, unsigned slotNumber){
    memcpy(page + sizeof(InternalNodeHeader) + (slotNumber * sizeof(InternalNodeEntry)), &internalNodeEntry, sizeof(InternalNodeEntry));
}

//adds the key followed by an rid to leaf page
void IndexManager::setLeafKeyAndRidAtOffset(void * page, const Attribute &attribute, const void *key, const RID &rid, unsigned offset, unsigned keylength){
  memcpy(page + offset - sizeof(RID), &rid, sizeof(RID));
  memcpy(page + offset - sizeof(RID) - keylength, key, keylength);
}

//adds a key to a internal node page
void IndexManager::setInternalKeyAtOffset(void * page, const Attribute &attribute, const void *key, unsigned keylength, unsigned offset){
  memcpy(page + offset -keylength, key, keylength);
}
//compares integer values
RC IndexManager::compareInts(const void * key, const void * toCompareTo){
  int * val1 = (int*)key;
  int * val2 = (int *)toCompareTo;
  if (val1[0] <= val2[0])
    return LESS_THAN_OR_EQUAL;
  return GREATER_THAN;
}
//compares float values
RC IndexManager:: compareReals(const void * key, const void * toCompareTo){
  float * val1 = (float *)key;
  float * val2 = (float *)toCompareTo;
  if (val1[0] <= val2[0])
    return LESS_THAN_OR_EQUAL;
  return GREATER_THAN;
}
//compares varchar values
RC IndexManager::compareVarChars(const void * key, const void * toCompareTo){
  int *size1 = (int*)key;
  int *size2 = (int*)toCompareTo;
  char * cast1 = (char*)key;
  char * cast2 = (char*)toCompareTo;
  string str1 = "";
  string str2 = "";
  for (int i =0; i < size1[0]; i ++)
    str1[i] = cast1[i];
  for (int i =0; i < size2[0]; i++)
    str2[i] = cast2[i];
   if (str1.compare(str2) < 1)
    return LESS_THAN_OR_EQUAL;
  return GREATER_THAN;
}
//returns the key at offset
void IndexManager::getKeyAtOffset(void * page, void * dest, unsigned offset, unsigned length){
  memcpy(dest, page + offset, length);
}
//returns the size that a leaf node entry would take
unsigned IndexManager::getSizeofLeafEntry(const void * key, AttrType attrType){
  unsigned keyLength =0;
  switch (attrType) {
    case TypeInt:
      keyLength = INT_SIZE;
      break;
    case TypeReal:
      keyLength = REAL_SIZE;
      break;
    case TypeVarChar:
    keyLength = INT_SIZE;
    int * size = (int*)key;
    keyLength += size[0];
  }
  return (sizeof(LeafNodeEntry) + sizeof(RID) + keyLength);
}
//retuns the free space on the page
unsigned IndexManager::getLeafFreeSpace(LeafNodeHeader leafNodeHeader){
  return (leafNodeHeader.freeSpaceOffset - sizeof(LeafNodeHeader) - (sizeof(LeafNodeEntry) * leafNodeHeader.numOfEntries));
}
// *************************************************************************
