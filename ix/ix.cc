
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
//  cout<< "file exists\n";
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
  ixfileHandle.fileName = fileName;
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
//    cout<<"filename: "<<ixfileHandle.fileName<< " ";
    printKey(key, attribute.type);
	cout<<endl;
    if (ixfileHandle.fileName == "")
      return IX_FILE_DN_EXIST;
//    printKey(key, attribute.type);
    //if no pages yet beging new tree
//    cout<<"pages: "<<ixfileHandle.getNumberOfPages()<<endl;
    if (!ixfileHandle.getNumberOfPages())
      initializeBTree(ixfileHandle, attribute.type);
    //get meta header
    void * pageData =  malloc(PAGE_SIZE);
    if (ixfileHandle.readPage(META_PAGE, pageData))
      return IX_READ_FAILED;
    MetaHeader mHeader = getMetaHeader(pageData);
    if (mHeader.type != attribute.type)
      return IX_CONFLICTING_TYPES;
//    cout<<"meta header check: "<<mHeader.height<<" root: "<<mHeader.rootPage<< " leafs "<< mHeader.numOfLeafNodes<<endl;

    //begin at the root
    ixfileHandle.readPage(mHeader.rootPage, pageData);
    unsigned childPageNum = INITIAL_PAGE;
    bool childFound = false;

    //this will loop through intrnal nodes if any exists
    InternalNodeHeader iHeader;
    InternalNodeEntry iEntry;
    void * keyInMemory;                 //key you extract and compare to
    for (unsigned i =0; i < mHeader.height; i--){
      cout<< "iterating through height\n";
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
            int compare = compareInts(key, keyInMemory);
            if (compare == LESS_THAN || compare == EQUAL_TO){
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
            int compare = compareReals(key, keyInMemory);
            if (compare == LESS_THAN|| compare == EQUAL_TO){
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
            int compare = compareVarChars(key, keyInMemory);
            if (compare == LESS_THAN || compare == EQUAL_TO){
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
//    cout<< "checked height\n";
    //traversed through internal nodes now at leaf
    LeafNodeHeader lHeader = getLeafNodeHeader(pageData);
    LeafNodeEntry lEntry;
//    cout<< "got leaf, "<< childPageNum<< "\n";
    //if there is no space we split the node
    unsigned potentialSize = getSizeofLeafEntry(key, attribute.type);
    unsigned freeSpaceOnPage = getLeafFreeSpace(lHeader);
//    cout<<"freespace = "<< freeSpaceOnPage<< " potentialSize = "<< potentialSize<<endl;
    if (potentialSize > freeSpaceOnPage){
        cout<< "size exceeds free space\n";
        unsigned midpoint = lHeader.numOfEntries / 2;
        unsigned rightSplit = splitLeafAtEntry(pageData, mHeader, lHeader, ixfileHandle, midpoint);
        lEntry = getLeafNodeEntry(pageData, midpoint);
        void * midKey = malloc(lEntry.length);
        getKeyAtOffset(pageData, midKey, lEntry.offset, lEntry.length);
        switch (attribute.type) {
            case TypeInt:
            {
                int compare = compareInts(key, midKey);
                if (compare == LESS_THAN || compare == EQUAL_TO){
                    //insert into left split node
                    break;
                }
                else{   //insert into right
                    ixfileHandle.readPage(rightSplit, pageData);
                    childPageNum =rightSplit;
                    lHeader = getLeafNodeHeader(pageData);
                    break;
                }
            }
            case TypeReal:
            {
                int compare = compareReals(key,midKey);
                if (compare == LESS_THAN || compare == EQUAL_TO){
                    //insert into left split node
                    break;
                }
                else{   //insert into right
                    ixfileHandle.readPage(rightSplit, pageData);
                    childPageNum = rightSplit;
                    lHeader = getLeafNodeHeader(pageData);
                    break;
                }
            }
            case TypeVarChar:
            {
                int compare = compareVarChars(key,midKey);
                if (compare == LESS_THAN || compare == EQUAL_TO){
                    //insert into left split node
                    break;
                }
                else{   //insert into right
                    ixfileHandle.readPage(rightSplit, pageData);
                    childPageNum = rightSplit;
                    lHeader = getLeafNodeHeader(pageData);
                    break;
                }
            }
        }
        free(midKey);
    }

    //in the right page and there is enough space
    void * currentKey;
    bool spotFound = false;
    bool addedRID = false;
    int spot = 0;
    cout<< "number of entries in header: "<< lHeader.numOfEntries<<endl;
    for (unsigned i = 0; i < lHeader.numOfEntries; i ++){
//      cout<< "looking over entry "<<i<<" ";
        lEntry = getLeafNodeEntry(pageData, i);
        currentKey = malloc(lEntry.length);
        getKeyAtOffset(pageData, currentKey, lEntry.offset, lEntry.length);
        printKey(key, attribute.type);
        cout<< " is being compare to ";
        printKey(currentKey, attribute.type);
        cout<<endl;
        switch (attribute.type) {
            case TypeInt:
            {
                int compare = compareInts(key,currentKey);
                if (compare == EQUAL_TO){
                  cout<< "equal things so add rid\n";
                  addAdditionalRID(pageData, lHeader, lEntry, rid, i);
                  addedRID = true;
                }
                else if(compare == LESS_THAN){
//                    cout<<" ints less than\n";
                    spotFound = true;
                    spot = i-1;
                }
				else{
//					cout<< "ints larger than\n";
				}
                break;
            }
            case TypeReal:
            {
                int compare = compareReals(key,currentKey);
                if (compare == EQUAL_TO){
//                  cout<< "equal things so add rid\n";
                  addAdditionalRID(pageData, lHeader, lEntry, rid, i);
                  addedRID = true;
                }
                else if(compare == LESS_THAN){
                    spotFound = true;
                    spot = i-1;
                }
                break;
            }
            case TypeVarChar:
            {
                int compare = compareVarChars(key,currentKey);
                if (compare == EQUAL_TO){
//                  cout<< "equal things so add rid\n";
                  addAdditionalRID(pageData, lHeader, lEntry, rid, i);
                  addedRID = true;
                }
                else if(compare == LESS_THAN){
                    spotFound = true;
                    spot = i-1;
                }
                break;
            }
        }
        if (spotFound || addedRID){
            free(currentKey);
            break;
          }
        if(i == lHeader.numOfEntries -1){ //will have to be added to end of page
            free(currentKey);
            spot = lHeader.numOfEntries;
        }
    }
	cout<< "spot and entry total "<< spot<< " "<< lHeader.numOfEntries<<endl;
    if (!addedRID){
      //now inject the data where it belongs
      if (spot == lHeader.numOfEntries || !lHeader.numOfEntries){      //add at the end
          cout<< "inject after\n";
          injectAfter(pageData, lHeader, key, rid, attribute.type);
      }
      else if(spot < FIRST_ENTRY){  //add before
          cout<< "inject before\n";
          injectBefore(pageData, lHeader, key, rid, attribute.type);
      }
      else{   //add before
          cout<< "inject between\n";
          injectBetween(pageData, spot, lHeader, key, rid, attribute.type);

      }
    }
    else{
//      cout<< "added rid no insert\n";
    }
//    cout<< "writing to page "<<childPageNum<<endl;
    if(ixfileHandle.writePage(childPageNum,pageData))
      return IX_WRITE_FAILED;
	cout<< "check\n";
	ixfileHandle.readPage(1, pageData);
	LeafNodeHeader hCheck = getLeafNodeHeader(pageData);
	showLeaf(pageData, attribute.type);
  showLeafOffsetsAndLengths(pageData);
  free(pageData);
  return SUCCESS;
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
  readPageCount = ixReadPageCounter;
  writePageCount = ixWritePageCounter;
  appendPageCount = ixAppendPageCounter;
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
        return FH_SEEK_FAILED;

    // Write the new page
    if (fwrite(data, 1, PAGE_SIZE, _file) == PAGE_SIZE)
    {
        fflush(_file);
        ixAppendPageCounter++;
        return SUCCESS;
    }
    return FH_WRITE_FAILED;
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
void IndexManager::initializeBTree(IXFileHandle ixfileHandle, AttrType attrType){
//    cout<<"initialized BTREE\n";
    MetaHeader mHeader;
    LeafNodeHeader lHeader;
    void * metaPage = malloc(PAGE_SIZE);
    void * firstPage = malloc(PAGE_SIZE);
    mHeader.rootPage = INITIAL_PAGE;      //the first node in will be both a leaf and the root
    mHeader.numOfLeafNodes = 1;
    mHeader.numOfInternalNodes = NO_ENTRIES;
    mHeader.height = INITIAL_HEIGHT;
    mHeader.type = attrType;
    lHeader.numOfEntries = NO_ENTRIES;
    lHeader.parentPage = NO_PAGE;         //pointer pages are invalid at the begining
    lHeader.leftNode = NO_PAGE;
    lHeader.rightNode = NO_PAGE;
    lHeader.freeSpaceOffset = PAGE_SIZE;  //no entries
    setMetaHeader(metaPage, mHeader);
    setLeafNodeHeader(firstPage, lHeader);
  //  cout<<"added first pages\n";
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
  memcpy(page + offset, key, keylength);
  memcpy(page + offset + keylength, &rid, sizeof(RID));
}

//adds a key to a internal node page
void IndexManager::setInternalKeyAtOffset(void * page, const Attribute &attribute, const void *key, unsigned keylength, unsigned offset){
  memcpy(page + offset -keylength, key, keylength);
}
//compares integer values
RC IndexManager::compareInts(const void * key, const void * toCompareTo){
  int val1 = ((int*)key)[0];
  int val2 = ((int *)toCompareTo)[0];
  if (val1 > val2)
    return GREATER_THAN;
  return val1 == val2;
}
//compares float values
RC IndexManager:: compareReals(const void * key, const void * toCompareTo){
  float val1 = ((float*)key)[0];
  float val2 = ((float *)toCompareTo)[0];
  if (val1 > val2)
    return GREATER_THAN;
  return val1 == val2;
}
//compares varchar values
RC IndexManager::compareVarChars(const void * key, const void * toCompareTo){
  int *size1 = (int*)key;
  int *size2 = (int*)toCompareTo;
  char * cast1 = (char*)key;
  char * cast2 = (char*)toCompareTo;
  cast1 += INT_SIZE;
  cast2 += INT_SIZE;
  string str1 = "";
  string str2 = "";
  for (int i =0; i < size1[0]; i ++)
    str1[i] = cast1[i];
  for (int i =0; i < size2[0]; i++)
    str2[i] = cast2[i];
    int compare = str1.compare(str2);
   if (compare > 0)
    return GREATER_THAN;
  else if (compare < 0)
    return LESS_THAN;
  return EQUAL_TO;

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
//splits the page into two from the leaf node at the midpoint and returns the page of the right split node
unsigned IndexManager::splitLeafAtEntry(void * page, MetaHeader &metaHeader,LeafNodeHeader &leafNodeHeader, IXFileHandle &ixfileHandle, unsigned midpoint){
    return -1;
}
//splits an internal node at the midpoint
void IndexManager::splitInternalAtEntry(void * page, MetaHeader &metaHeader, InternalNodeHeader &internalNodeHeader, IXFileHandle &ixfileHandle, unsigned midpoint){

}
//used to insert at the begining of a leafnode
void IndexManager::injectBefore(void * page, LeafNodeHeader &leafNodeHeader, const void * key, RID rid, AttrType attrType){
    LeafNodeEntry leafNodeEntry;
    LeafNodeEntry following;
    //node to be inserted
    leafNodeEntry.length = getKeySize(attrType, key);
	leafNodeEntry.offset = PAGE_SIZE - leafNodeEntry.length - sizeof(RID);
    leafNodeEntry.status = alive;
    leafNodeEntry.numberOfRIDs = 1;
    //update shifted entry offsets
    for (unsigned i = 0; i< leafNodeHeader.numOfEntries; i++){
      following = getLeafNodeEntry(page, i);
      following.offset = following.offset - leafNodeEntry.length - sizeof(RID);
      setLeafNodeEntry(page, following, i);
    }
    // move data left
    memcpy(page + leafNodeHeader.freeSpaceOffset - leafNodeEntry.length - sizeof(RID), page + leafNodeHeader.freeSpaceOffset, PAGE_SIZE - leafNodeHeader.freeSpaceOffset);
    //move entries right
    memcpy(page + sizeof(LeafNodeHeader) + sizeof(LeafNodeEntry), page + sizeof(LeafNodeHeader), leafNodeHeader.numOfEntries * sizeof(LeafNodeEntry));
    //insert the new begining one
    memcpy(page + PAGE_SIZE - leafNodeEntry.length - sizeof(RID), key, leafNodeEntry.length);
    memcpy(page + PAGE_SIZE - sizeof(RID), &rid, sizeof(RID));
    setLeafNodeEntry(page, leafNodeEntry, FIRST_ENTRY);
    leafNodeHeader.numOfEntries++;
    //update leaf header
    leafNodeHeader.freeSpaceOffset = leafNodeHeader.freeSpaceOffset - leafNodeEntry.length - sizeof(RID);
    setLeafNodeHeader(page, leafNodeHeader);
}
//used to inject in between a leaf node
void IndexManager::injectBetween(void * page, unsigned position, LeafNodeHeader &leafNodeHeader, const void * key, RID rid, AttrType attrType){
    cout<<"getting put in position: "<<position<<endl;
    LeafNodeEntry following = getLeafNodeEntry(page, position +1);
    LeafNodeEntry original = getLeafNodeEntry(page, position);
    cout<< "original off: "<< original.offset<< " following off: "<<following.offset<<endl;
    //node to be inserted
    LeafNodeEntry leafNodeEntry;
    leafNodeEntry.length = getKeySize(attrType, key);
    leafNodeEntry.offset = original.offset - sizeof(RID) - leafNodeEntry.length;
    leafNodeEntry.status = alive;
    leafNodeEntry.numberOfRIDs = 1;

    //update shifted entry offsets
    LeafNodeEntry itr;
    for (unsigned i = position +1 ; i< leafNodeHeader.numOfEntries; i++){
      itr = getLeafNodeEntry(page, i);
      itr.offset = itr.offset - leafNodeEntry.length - sizeof(RID);
      cout<< i<< " new off: "<<itr.offset<<endl;
      setLeafNodeEntry(page, itr, i);
    }
    // move data left
    unsigned dest = leafNodeHeader.freeSpaceOffset - leafNodeEntry.length - sizeof(RID);
    unsigned source = leafNodeHeader.freeSpaceOffset;
    unsigned byteSize = original.offset - leafNodeHeader.freeSpaceOffset;
    memcpy(page + dest, page + source, byteSize);
    //move entries right
    dest = sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry)* (position +2));
    source = sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry)* (position +1));
    byteSize = (leafNodeHeader.numOfEntries - (position +1)) * sizeof(LeafNodeEntry) ;
    cout<< "moving "<< byteSize/sizeof(LeafNodeEntry)<< " entries \n";
    memcpy(page + dest, page + source, byteSize);
    //insert the new one
    following = getLeafNodeEntry(page, position +1);
    leafNodeEntry.offset = following.offset - leafNodeEntry.length - sizeof(RID);
    memcpy(page + leafNodeEntry.offset, key, leafNodeEntry.length);
    memcpy(page + leafNodeEntry.offset + leafNodeEntry.length, &rid, sizeof(RID));
    leafNodeHeader.numOfEntries++;
    setLeafNodeEntry(page, leafNodeEntry, position +1);
    //update leaf header
    leafNodeHeader.freeSpaceOffset = leafNodeHeader.freeSpaceOffset - leafNodeEntry.length - sizeof(RID);
    setLeafNodeHeader(page, leafNodeHeader);

}
//used to insert at the end of a leaf node
void IndexManager::injectAfter(void * page, LeafNodeHeader &leafNodeHeader, const void * key, RID rid, AttrType attrType){
    LeafNodeEntry leafNodeEntry;
    Attribute tempatt;
    tempatt.type = attrType;
    leafNodeEntry.length = getKeySize(attrType, key);
    leafNodeEntry.status = alive;
//    cout<< "header freespace offset "<<leafNodeHeader.freeSpaceOffset<<endl;
    leafNodeEntry.offset = leafNodeHeader.freeSpaceOffset - leafNodeEntry.length - sizeof(RID);
    setLeafKeyAndRidAtOffset(page, tempatt, key, rid, leafNodeEntry.offset, leafNodeEntry.length);
    leafNodeHeader.numOfEntries++;
    leafNodeHeader.freeSpaceOffset = leafNodeEntry.offset;
    leafNodeEntry.numberOfRIDs =1;
//    cout<< "entry offset "<<leafNodeEntry.offset<< " lenght "<< leafNodeEntry.length<<endl;
    setLeafNodeHeader(page, leafNodeHeader);
    setLeafNodeEntry(page, leafNodeEntry, leafNodeHeader.numOfEntries -1);

}
//shows an individual key for debuging purposes
void IndexManager::printKey(const void *key, AttrType attrType){
  switch (attrType) {
    case TypeInt:
    {
//        cout<< "printing int\n";
      int iKey = ((long*)key)[0];
        int * out = (int *) key;
        printf(" %d ", iKey);
        break;
    }
    case TypeReal:
    {
      float * rKey = (float *)key;
      cout<<" "<< rKey[0]<< " ";
        break;
    }
    case TypeVarChar:
    {
      int * size = (int *)key;
      char * cast = (char *)key;
      string out = "";
      for (int i = 0; i < size[0]; i++)
        out[i] = cast[i];
      cout<< " "<< out<< " ";
        break;
    }
  }
}
//prints rids in order for debugging purposes
void IndexManager::printRids(void * page, LeafNodeEntry leafNodeEntry){
	RID out;
	for (unsigned i =0; i < leafNodeEntry.numberOfRIDs; i ++){
		memcpy(&out, page + leafNodeEntry.offset + leafNodeEntry.length + (i * sizeof(RID)), sizeof(RID));
		printf(" [%d,%d] ",out.pageNum,out.slotNum);
		}
}

//adds an aditional RID to an existing leaf entry
void IndexManager::addAdditionalRID(void * page,LeafNodeHeader leafNodeHeader, LeafNodeEntry leafNodeEntry, RID newRid, unsigned entryPos){
    unsigned dest = leafNodeHeader.freeSpaceOffset - sizeof(RID);
    unsigned source = leafNodeHeader.freeSpaceOffset;
    unsigned byteSize = leafNodeEntry.offset + leafNodeEntry.length + (sizeof(RID)* leafNodeEntry.numberOfRIDs)  - leafNodeHeader.freeSpaceOffset;
    memcpy(page + dest, page + source, byteSize);
  leafNodeHeader.freeSpaceOffset = leafNodeHeader.freeSpaceOffset - sizeof(RID);
  //change offsets in all individaul entries
  LeafNodeEntry change;
  for (unsigned i =entryPos; i < leafNodeHeader.numOfEntries; i ++){
//	cout<< "changing all offsets\n";
    change = getLeafNodeEntry(page, i);
    change.offset = change.offset - sizeof(RID);
    setLeafNodeEntry(page, change, i);
  }
    leafNodeEntry = getLeafNodeEntry(page, entryPos);
    dest =leafNodeEntry.offset + leafNodeEntry.length + (leafNodeEntry.numberOfRIDs * sizeof(RID));
  memcpy(page + dest, &newRid, sizeof(RID));
  setLeafNodeHeader(page, leafNodeHeader);
  leafNodeEntry.numberOfRIDs++;
  setLeafNodeEntry(page, leafNodeEntry, entryPos);
}
//used to display everything in a leaf for debbugging purposes
void IndexManager::showLeaf(void * page, AttrType attrType){
	LeafNodeHeader lHeader = getLeafNodeHeader(page);
	LeafNodeEntry lEntry;
	void * key;
	for (unsigned i = 0; i < lHeader.numOfEntries; i ++){
		lEntry = getLeafNodeEntry(page, i);
		key = malloc(lEntry.length);
		getKeyAtOffset(page, key, lEntry.offset, lEntry.length);
		printKey(key, attrType);
		printRids(page, lEntry);
		free(key);
	}
	cout<<endl;
}
//used to print out the offsets for debugging purposes
void IndexManager::showLeafOffsetsAndLengths(void * page){
  LeafNodeHeader lHeader = getLeafNodeHeader(page);
  cout<< "H freeSpaceOffset: "<<lHeader.freeSpaceOffset<< " H entry total: "<< lHeader.numOfEntries;
  LeafNodeEntry lEntry;
  for (unsigned i =0; i < lHeader.numOfEntries; i ++){
    lEntry = getLeafNodeEntry(page, i);
    cout<< " E offset: "<<lEntry. offset<< " E length: "<< lEntry.length;
  }
  cout<< endl;
}
