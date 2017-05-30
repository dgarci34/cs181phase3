
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
      cout<< "destroyFile\n";
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
    if (ixfileHandle.fileName == "")
      return IX_FILE_DN_EXIST;
    //if no pages yet begin new tree
    if (!ixfileHandle.getNumberOfPages())
      initializeBTree(ixfileHandle, attribute.type);
    //get meta header
    void * pageData =  malloc(PAGE_SIZE);
    if (ixfileHandle.readPage(META_PAGE, pageData))
      return IX_READ_FAILED;
    MetaHeader mHeader = getMetaHeader(pageData);
    if (mHeader.type != attribute.type)
      return IX_CONFLICTING_TYPES;

    //begin at the root
//    cout<< "root is at: "<<mHeader.rootPage<<endl;
    ixfileHandle.readPage(mHeader.rootPage, pageData);
    unsigned childPageNum = INITIAL_PAGE;
    bool childFound = false;

    //this will loop through intrnal nodes if any exists
    InternalNodeHeader iHeader;
    InternalNodeEntry iEntry;
    void * keyInMemory;                 //key you extract and compare to
    for (unsigned i =mHeader.height; i > 0; i--){
//      cout<< "iterating through height "<< i<<endl;
      iHeader = getInternalNodeHeader(pageData);
      //compare through internal node values
      for (unsigned j = 0; j < iHeader.numOfEntries; j++){
//        cout<< "comparing entries\n";
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
            }
            //if greater than and at the last follow rightchild
            else if(j == iHeader.numOfEntries -1){
              childPageNum = iEntry.rightChild;
              childFound = true;
            }
            free(keyInMemory);
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
            }
            //if greater than and at the last follow rightchild
            else if(j == iHeader.numOfEntries -1){
              childPageNum = iEntry.rightChild;
              childFound = true;
            }
            free(keyInMemory);
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
            }
            //if greater than and at the last follow rightchild
            else if(j == iHeader.numOfEntries -1){
              childPageNum = iEntry.rightChild;
              childFound = true;
            }
            free(keyInMemory);
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
    LeafNodeEntry lEntry;
//    cout<< "got leaf, "<< childPageNum<< "\n";
    //if there is no space we split the node
    unsigned potentialSize = getSizeofLeafEntry(key, attribute.type);
    unsigned freeSpaceOnPage = getLeafFreeSpace(lHeader);
//    cout<<"freespace = "<< freeSpaceOnPage<< " potentialSize = "<< potentialSize<<endl;
    if (potentialSize > freeSpaceOnPage){
//        cout<< "size exceeds free space\n";
        unsigned midpoint = lHeader.numOfEntries / 2;
        if(splitLeafAtEntry(pageData, childPageNum, mHeader, lHeader, ixfileHandle, midpoint, attribute.type))
          return IX_SPLIT_FAILED;
        unsigned rightSplit = lHeader.rightNode;
//        cout<< "split done now fitting into self: "<<lHeader.selfPage <<" or right: "<<rightSplit<<endl;
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
//    cout<< "number of entries in header: "<< lHeader.numOfEntries<<endl;
    for (unsigned i = 0; i < lHeader.numOfEntries; i ++){
//      cout<< "looking over entry "<<i<<" ";
        lEntry = getLeafNodeEntry(pageData, i);
        currentKey = malloc(lEntry.length);
        getKeyAtOffset(pageData, currentKey, lEntry.offset, lEntry.length);
        switch (attribute.type) {
            case TypeInt:
            {
                int compare = compareInts(key,currentKey);
                if (compare == EQUAL_TO){
//                  cout<< "equal things so add rid\n";
                  if (addAdditionalRID(pageData, lHeader, lEntry, rid, i)){
                    free(pageData);
                    return IX_RID_DUPLICATE;
                  }
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
                  if (addAdditionalRID(pageData, lHeader, lEntry, rid, i)){
                    free(pageData);
                    return IX_RID_DUPLICATE;
                  }
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
                  if (addAdditionalRID(pageData, lHeader, lEntry, rid, i)){
                    free(pageData);
                    return IX_RID_DUPLICATE;
                  }
                  addedRID = true;
                }
                else if(compare == LESS_THAN){
                    spotFound = true;
                    spot = i-1;
                }
                break;
            }
        }
        free(currentKey);
        if (spotFound || addedRID){
            break;
          }
        if(i == lHeader.numOfEntries -1){ //will have to be added to end of page
            spot = lHeader.numOfEntries;
        }
    }
//	cout<< "spot and entry total "<< spot<< " "<< lHeader.numOfEntries<<endl;
    if (!addedRID){
      //now inject the data where it belongs
      if (spot == lHeader.numOfEntries || !lHeader.numOfEntries){      //add at the end
//          cout<< "inject after\n";
          injectAfter(pageData, lHeader, key, rid, attribute.type);
      }
      else if(spot < FIRST_ENTRY){  //add before
//          cout<< "inject before\n";
          injectBefore(pageData, lHeader, key, rid, attribute.type);
      }
      else{   //add before
//          cout<< "inject between\n";
          injectBetween(pageData, spot, lHeader, key, rid, attribute.type);

      }
    }
    else{
//      cout<< "added rid no insert\n";
    }
//    cout<< "writing to page "<<childPageNum<<endl;
    if(ixfileHandle.writePage(childPageNum,pageData))
      return IX_WRITE_FAILED;
  free(pageData);
  return SUCCESS;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){
      //if no pages yet begin new tree
      if (!ixfileHandle.getNumberOfPages())
        initializeBTree(ixfileHandle, attribute.type);
      //get meta header
      void * pageData =  malloc(PAGE_SIZE);
      if (ixfileHandle.readPage(META_PAGE, pageData))
        return IX_READ_FAILED;
      MetaHeader mHeader = getMetaHeader(pageData);
      if (mHeader.type != attribute.type)
        return IX_CONFLICTING_TYPES;

      //begin at the root
  //    cout<< "root is at: "<<mHeader.rootPage<<endl;
      ixfileHandle.readPage(mHeader.rootPage, pageData);
      unsigned childPageNum = INITIAL_PAGE;
      bool childFound = false;

      //this will loop through intrnal nodes if any exists
      InternalNodeHeader iHeader;
      InternalNodeEntry iEntry;
      void * keyInMemory;                 //key you extract and compare to
      for (unsigned i =mHeader.height; i > 0; i--){
  //      cout<< "iterating through height "<< i<<endl;
        iHeader = getInternalNodeHeader(pageData);
        //compare through internal node values
        for (unsigned j = 0; j < iHeader.numOfEntries; j++){
  //        cout<< "comparing entries\n";
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
              }
              //if greater than and at the last follow rightchild
              else if(j == iHeader.numOfEntries -1){
                childPageNum = iEntry.rightChild;
                childFound = true;
              }
              free(keyInMemory);
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
              }
              //if greater than and at the last follow rightchild
              else if(j == iHeader.numOfEntries -1){
                childPageNum = iEntry.rightChild;
                childFound = true;
              }
              free(keyInMemory);
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
              }
              //if greater than and at the last follow rightchild
              else if(j == iHeader.numOfEntries -1){
                childPageNum = iEntry.rightChild;
                childFound = true;
              }
              free(keyInMemory);
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
      LeafNodeHeader lHeader = getLeafNodeHeader(pageData);
//      cout<< lHeader.numOfEntries<<" has this many entries"<<endl;
      if (!lHeader.numOfEntries){
        free(pageData);
        return IX_NOTHING_TO_DELETE;
      }
      LeafNodeEntry lEntry;
      //in the right page
      void * currentKey;
      bool spotFound = false;
      bool addedRID = false;
      int spot = 0;
  //    cout<< "number of entries in header: "<< lHeader.numOfEntries<<endl;
      for (unsigned i = 0; i < lHeader.numOfEntries; i ++){
  //      cout<< "looking over entry "<<i<<" ";
          lEntry = getLeafNodeEntry(pageData, i);
          currentKey = malloc(lEntry.length);
          getKeyAtOffset(pageData, currentKey, lEntry.offset, lEntry.length);
          switch (attribute.type) {
              case TypeInt:
              {
                  int compare = compareInts(key,currentKey);
                  if (compare == EQUAL_TO){
                    if (deleteRID(pageData, lHeader, i, rid)){
                      free(pageData);
                      return IX_NO_MATCHING_RID_TO_DELETE;
                    }
                    else{
                      ixfileHandle.writePage(childPageNum, pageData);
                      free(pageData);
                      return SUCCESS;
                    }
                  }
                  break;
              }
              case TypeReal:
              {
                  int compare = compareReals(key,currentKey);
                  if (compare == EQUAL_TO){
                    if (deleteRID(pageData, lHeader, i, rid)){
                      ixfileHandle.writePage(childPageNum, pageData);
                      free(pageData);
                      return IX_NO_MATCHING_RID_TO_DELETE;
                    }
                    else{
                      ixfileHandle.writePage(childPageNum, pageData);
                      free(pageData);
                      return SUCCESS;
                    }
                  }
                  break;
              }
              case TypeVarChar:
              {
                  int compare = compareVarChars(key,currentKey);
                  if (compare == EQUAL_TO){
                    if (deleteRID(pageData, lHeader, i, rid)){
                      ixfileHandle.writePage(childPageNum, pageData);
                      free(pageData);
                      return IX_NO_MATCHING_RID_TO_DELETE;
                    }
                    else{
                      ixfileHandle.writePage(childPageNum, pageData);
                      free(pageData);
                      return SUCCESS;
                    }
                  }
                  break;
              }
          }
          free(currentKey);
      }
    //iterated through everything and no matching key
    free(pageData);
    return IX_NOTHING_TO_DELETE;
  }


/*RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    MetaHeader mHeader;
    unsigned treeHeight;
    unsigned pageNum;
    AttrType type;

    IndexId indexId;
    unsigned nextNodePageNum;

    void * page = malloc(PAGE_SIZE);

    mHeader = getMetaHeader(page);
    treeHeight = mHeader.height;
    pageNum = mHeader.rootPage;
    type = mHeader.type;

    // the search key and the key in tree should match
    if (type != attribute.type)
        return IX_TYPE_ERROR;

    // tree height must be >= 0
    if (treeHeight < 0)
        return IX_TREE_ERROR;

    // only one node in tree
    if (treeHeight == 0)
    {
        free(page);

        if (searchLeafNode(ixfileHandle, pageNum, type, key, rid, &indexId))
            return IX_TARGET_DOES_EXIST;
        cout<< "made it\n";
        // tbc -- mcmcpy the record
    }

    // find the leaf node that store the search key
    nextNodePageNum = pageNum;
    for (unsigned i = 0; i < (treeHeight - 1); i++)
    {
        nextNodePageNum = getNextNodePageNum(ixfileHandle, nextNodePageNum, type, key, rid);
    }

    // find the target in LeafNode
    if (searchLeafNode(ixfileHandle, nextNodePageNum, type, key, rid, &indexId))
        return IX_TARGET_DOES_EXIST;
    // tbc -- mcmcpy the record

    return SUCCESS;
}*/



RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
  if(!IndexManager::fileExists(ixfileHandle.fileName))
    return IX_NO_SCAN_FILE;
  return ix_ScanIterator.scanInit(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const{
    IXFileHandle tempFileHandle = ixfileHandle;

    //there must be a tree
    if (!ixfileHandle.getNumberOfPages())
        return;
    cout<< "----------------BTREE " <<ixfileHandle.fileName<< "--------------------- \n\n";

    void * page = malloc(PAGE_SIZE);

    MetaHeader mHeader;
    AttrType type;
    unsigned treeHeight;
    unsigned currHeight;
    unsigned prevHeight;

    MetaNode mNode;
    stack<MetaNode> pageNumStack;
    unsigned currPageNum;
    unsigned leftChildPageNum;
    unsigned rightChildPageNum;

    // find root page
    if (tempFileHandle.readPage(META_PAGE, page))
        return;

    mHeader = tempFileHandle.fhGetMetaHeader(page);
    type = mHeader.type;
    treeHeight = mHeader.height;
    currHeight = 0;
    prevHeight = 0;

    // push the page number of root to stack
    tempFileHandle.setMetaNode(&mNode, mHeader.rootPage, 0);
    pageNumStack.push(mNode);

    // print tree (More than one node)
    while (!pageNumStack.empty())
    {
        // get the top MetaNode on stack
        mNode = pageNumStack.top();
        pageNumStack.pop();
        currHeight = mNode.height;
        currPageNum = mNode.pageNum;

        if (currHeight > treeHeight)
        {
            cout << "***** ERROR: the height of node is greater than the maximum tree height *****" << endl;
            return;
        }

        // print Leaf Node if it is a Leaf Node
        if (currHeight == treeHeight)
        {
            ixfileHandle.fhPrintLeafNode(tempFileHandle, currHeight, currPageNum, type, &pageNumStack);
        }

        // print Internal Node if it is a Internal Node
        if (currHeight < treeHeight)
        {
            ixfileHandle.fhPrintInternalNode(tempFileHandle, currHeight, currPageNum, type, pageNumStack);
        }

        // print the symbol for formating the JSON string
        if (!pageNumStack.empty())
        {
            unsigned nextHeight = (pageNumStack.top()).height;

            // next node has the same height
            // this implies that next node has the same parent as the current node
            if (nextHeight == currHeight)
                cout << ",\n";
            // all the children belongs to the current node parent has been printed
            else if (nextHeight < currHeight)
                cout << "\n" << string((currHeight - 1), '\t') << "]}\n";
        }
        // print "]}" close the JSON
        else
        {
            for (unsigned i = currHeight; i > 0; i--)
                cout << string((currHeight -1), '\t') << "]}\n";
        }

        // update the info
        prevHeight = currHeight;
    }


    free(page);
}


IX_ScanIterator::IX_ScanIterator()
{
    im = IndexManager::instance();
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (totalLeaves == 0)
    {
        cout << "No Leaf Page" << endl;
        return IX_EOF;
    }

    LeafNodeHeader lNodeHeader;
    LeafNodeEntry lNodeEntry;
    RC rc;

    ixfh->readPage(currPage, pageData);
    lNodeHeader = im->getLeafNodeHeader(pageData);
    totalEntry = lNodeHeader.numOfEntries;

    // scan current node
    rc = scanCurrPage(rid, key);

    // scan until next entry is found or all entries are scanned
    while (rc == IX_NOT_FOUND)
    {
        // prepare to scan next node
        currPage = lNodeHeader.rightNode;
        currEntry = FIRST_ENTRY;
        currRid = FIRST_RID;

        // check if all the node has been scaned
        if (currPage == 0)
            return IX_EOF;

        ixfh->readPage(currPage, pageData);
        lNodeHeader = im->getLeafNodeHeader(pageData);
        totalEntry = lNodeHeader.numOfEntries;

        // scan the new node
        rc = scanCurrPage(rid, key);
    }

    return rc;
}

RC IX_ScanIterator::close()
{
    return SUCCESS;
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
    lHeader.selfPage = INITIAL_PAGE;
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
//splits the page into two from the leaf node at the midpoint
RC IndexManager::splitLeafAtEntry(void * page, unsigned pageNum, MetaHeader &metaHeader,LeafNodeHeader &leafNodeHeader, IXFileHandle &ixfileHandle, unsigned midpoint, AttrType attrType){
//    cout<< "spliting node\n";
    LeafNodeEntry midEntry = getLeafNodeEntry(page, midpoint);
    void * splitKey = malloc(midEntry.length);
    void * newRightPage = malloc(PAGE_SIZE);
    void * temp;                  //used for memcpy saftey
    unsigned originalNumOfEntries = leafNodeHeader.numOfEntries;
    unsigned originalFreeSpaceOffset = leafNodeHeader.freeSpaceOffset;
    LeafNodeHeader newRightHeader;
    getKeyAtOffset(page, splitKey, midEntry.offset, midEntry.length);
//    printKey(splitKey, metaHeader.type);
//    cout<< " <- spliting at this key\n";
//    cout<< "mid point #, offset: "<< midpoint<< " "<<midEntry.offset<<endl;
    //copy higher entry data into right page
    memcpy(newRightPage, page, PAGE_SIZE);

    //shift right page data right
    unsigned dest = (PAGE_SIZE - midEntry.offset) + leafNodeHeader.freeSpaceOffset;
    unsigned src =  leafNodeHeader.freeSpaceOffset;
    unsigned byteSize = midEntry.offset - leafNodeHeader.freeSpaceOffset;
//    cout<< "right node from, to, size "<< dest<< " "<<src<< " "<< byteSize<<endl;
    temp = malloc(byteSize);
    memcpy(temp, newRightPage + src, byteSize);
    memcpy(newRightPage + dest, temp, byteSize);
    free(temp);
    //change left's header's free space offset and number of entries to manipulate size
    LeafNodeEntry itr;
    for (unsigned i = midpoint +1; i <leafNodeHeader.numOfEntries; i++){
      itr = getLeafNodeEntry(newRightPage, i);
//      cout<< i<< " changing this offset: "<<itr.offset;
      itr.offset = itr.offset + PAGE_SIZE - midEntry.offset;
//      cout<< " to: "<<itr.offset<<endl;
      setLeafNodeEntry(newRightPage, itr, i);
    }
    //shift entries left
    dest = sizeof(leafNodeHeader);
    src = sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry) * (midpoint +1));
    byteSize = sizeof(LeafNodeEntry) *(leafNodeHeader.numOfEntries - midpoint -1);
    temp = malloc(byteSize);
    memcpy(temp, newRightPage + src, byteSize);
    memcpy(newRightPage+ dest, temp, byteSize);
    leafNodeHeader.freeSpaceOffset = midEntry.offset;
    leafNodeHeader.numOfEntries = midpoint +1;
    leafNodeHeader.rightNode = ixfileHandle.getNumberOfPages();
    setLeafNodeHeader(page, leafNodeHeader);
//    cout<< "the left leaf:\n";
//    showLeaf(page, metaHeader.type);
//    cout<< "new free space: "<< getLeafFreeSpace(leafNodeHeader)<<endl;

    //make a new leaf header for the right child
    newRightHeader.numOfEntries = originalNumOfEntries - midpoint -1;
    newRightHeader.freeSpaceOffset = (PAGE_SIZE - midEntry.offset) + originalFreeSpaceOffset;
    newRightHeader.leftNode = leafNodeHeader.selfPage;
    newRightHeader.selfPage = ixfileHandle.getNumberOfPages();
    newRightHeader.parentPage = leafNodeHeader.parentPage;
    setLeafNodeHeader(newRightPage, newRightHeader);
//    cout<< "the right leaf: \n";
//    showLeaf(newRightPage, metaHeader.type);
//    cout<< "new free space: "<<getLeafFreeSpace(newRightHeader)<<endl;
    if (ixfileHandle.appendPage(newRightPage))
      return IX_SPLIT_FAILED;
    metaHeader.numOfLeafNodes++;
    if (leafNodeHeader.parentPage){
      //push up key;
  //    cout<< "have parent to push up to\n";
      if (pushUpSplitKey(newRightHeader.parentPage, metaHeader, ixfileHandle, splitKey,attrType, pageNum, leafNodeHeader.rightNode))
        return IX_SPLIT_FAILED;
    }
    else{
//      cout<< "no parent to push up to\n";
      leafNodeHeader.parentPage = leafNodeHeader.rightNode +1;
      newRightHeader. parentPage = leafNodeHeader.parentPage;
      setLeafNodeHeader(newRightPage, newRightHeader);
      setLeafNodeHeader(page, leafNodeHeader);
      fixPageOrderSplitAndHeightIncrease(metaHeader, newRightPage, ixfileHandle);
      if (pushUpSplitKey(newRightHeader.parentPage, metaHeader,ixfileHandle, splitKey, attrType, pageNum, leafNodeHeader.rightNode))
        return IX_SPLIT_FAILED;
    }
    ixfileHandle.writePage(leafNodeHeader.selfPage, page);
    ixfileHandle.writePage(newRightHeader.selfPage, newRightPage);
    ixfileHandle.readPage(META_PAGE, newRightPage);
    setMetaHeader(newRightPage, metaHeader);
    ixfileHandle.writePage(META_PAGE, newRightPage);
    free(newRightPage);
    free(splitKey);
    return SUCCESS;
}
//adds the split right node to the end of the file
void IndexManager::fixPageOrderSplit(MetaHeader &metaHeader, void * right, IXFileHandle &ixfileHandle){
  cout<<"fixing pages no height increase\n";
  metaHeader.numOfLeafNodes++;
  ixfileHandle.appendPage(right);
}
//adds the split right node the the ned followed by the new parent following it
void IndexManager::fixPageOrderSplitAndHeightIncrease(MetaHeader & metaHeader, void * right, IXFileHandle & ixfileHandle){
//  cout<<"fixing pages height increase\n";
  void * newPage = malloc(PAGE_SIZE);
  InternalNodeHeader iHeader;
  iHeader.numOfEntries =0;
  iHeader.freeSpaceOffset = PAGE_SIZE;
  iHeader.parentPage = NO_PAGE;
  iHeader.selfPage = ixfileHandle.getNumberOfPages();
  iHeader.height =1;
  setInternalNodeHeader(newPage, iHeader);
  ixfileHandle.appendPage(newPage);
  metaHeader.numOfInternalNodes++;
  metaHeader.rootPage = iHeader.selfPage;
  metaHeader.height++;
//  showInternalStatistics(internalPage);
  ixfileHandle.readPage(META_PAGE, newPage);
  setMetaHeader(newPage, metaHeader);
  ixfileHandle.writePage(META_PAGE, newPage);
//  showMetaHeaderStatistics(newPage);
  free(newPage);
}
//inserts key and children page numbers to internal node
RC IndexManager::pushUpSplitKey(unsigned pageNum, MetaHeader &metaHeader, IXFileHandle ixfileHandle, void * key, AttrType attrType, unsigned leftChildPage, unsigned rightChildPage){
//  cout<< "attempting to split at: ";
//  printKey(key, metaHeader.type);
//  cout<< "to page: "<<pageNum<<endl;
  void * tempPage = malloc(PAGE_SIZE);
  ixfileHandle.readPage(pageNum, tempPage);
  InternalNodeHeader iHeader = getInternalNodeHeader(tempPage);
  unsigned freeSpace = getInternalFreeSpace(iHeader);
  unsigned potentialSize = getKeySize(attrType, key) + sizeof(InternalNodeEntry);
  //if it its then we will insert the key in node
  if(potentialSize <= freeSpace){
//    cout<<"fits in parent\n";
    bool spotFound;
    int spot = 0;
    InternalNodeEntry iEntry;
    void * keyInMemory;
    for (unsigned i =0; i < iHeader.numOfEntries; i ++){
//        cout<< "looking through internal entries\n";
        iEntry = getInternalNodeEntry(tempPage, i);
        keyInMemory = malloc(iEntry.length);
        getKeyAtOffset(tempPage, keyInMemory, iEntry.offset, iEntry.length);
        switch(attrType)
        {
          case(TypeInt):
          {
            unsigned compare = compareInts(key, keyInMemory);
            if(compare == LESS_THAN || compare == EQUAL_TO){
              spotFound = true;
            }
            free(keyInMemory);
            break;
          }
          case(TypeReal):
          {
            unsigned compare = compareReals(key, keyInMemory);
            if(compare == LESS_THAN || compare == EQUAL_TO){
              spotFound = true;
            }
            free(keyInMemory);
            break;
          }
          case(TypeVarChar):
          {
            unsigned compare = compareVarChars(key, keyInMemory);
            if(compare == LESS_THAN || compare == EQUAL_TO){
              spotFound = true;
            }
            free(keyInMemory);
            break;
          }
        }
        if(spotFound){
          spot = i-1;
          break;
        }
        //must be bgger then everything so goes at the end
        if (i == iHeader.numOfEntries -1)
          spot = iHeader.numOfEntries;
    }
    //now determine where to insert in internal header
    if (spot == iHeader.numOfEntries || !iHeader.numOfEntries){      //add at the end
//        cout<< "inject internal after\n";
        injectInternalAfter(tempPage, iHeader, key, attrType, leftChildPage, rightChildPage);
    }
    else if(spot < FIRST_ENTRY){  //add before
//        cout<< "inject before\n";
        injectInternalBefore(tempPage, iHeader, key, attrType,leftChildPage, rightChildPage);
    }
    else{   //add before
//        cout<< "inject between\n";
        injectInternalBetween(tempPage, spot, iHeader, key, attrType,leftChildPage,rightChildPage);

    }
  }
  else{
    cout<<"does not fit\n";
    //split the parent node recusively until it fits
    unsigned splitPos = iHeader.numOfEntries / 2;
    unsigned splitReturn =0;
    splitReturn = splitInternalAtEntry(tempPage, metaHeader, iHeader, ixfileHandle, splitPos);
  }
//  cout<< "pushed up\n";
  ixfileHandle.writePage(iHeader.selfPage, tempPage);
//  showInternalKeysAndChildren(tempPage, metaHeader.type);
  free(tempPage);
  return SUCCESS;
}

//used to insert at the begining of an internal node
void IndexManager::injectInternalBefore(void * page, InternalNodeHeader &internalNodeHeader, const void * key, AttrType attrType, unsigned leftChild, unsigned rightChild){
  InternalNodeEntry internalNodeEntry;
  InternalNodeEntry following;
  //node to be inserted
  internalNodeEntry.length = getKeySize(attrType, key);
  internalNodeEntry.offset = PAGE_SIZE - internalNodeEntry.length;
  internalNodeEntry.leftChild = leftChild;
  internalNodeEntry.rightChild = rightChild;
  //update shifted entry offsets
  for (unsigned i = 0; i< internalNodeHeader.numOfEntries; i++){
    following = getInternalNodeEntry(page, i);
    following.offset = following.offset - internalNodeEntry.length;
    setInternalNodeEntry(page, following, i);
  }
  // move data left
  unsigned dest = internalNodeHeader.freeSpaceOffset - internalNodeEntry.length;
  unsigned src = internalNodeHeader.freeSpaceOffset;
  unsigned byteSize = PAGE_SIZE- internalNodeHeader.freeSpaceOffset;
  //memcpy saftey move
  void * temp = malloc(byteSize);
  memcpy(temp, page + src, byteSize);
  memcpy(page + dest, temp, byteSize);
  free(temp);
  //move entries right
  dest = sizeof(InternalNodeHeader) + sizeof(InternalNodeEntry);
  src = sizeof(InternalNodeHeader);
  byteSize = sizeof(InternalNodeEntry) * internalNodeHeader.numOfEntries;
  temp = malloc(byteSize);
  memcpy(temp, page + src, byteSize);
  memcpy(page + dest, temp, byteSize);
  free(temp);
  //insert the new begining one
  memcpy(page + PAGE_SIZE - internalNodeEntry.length, key, internalNodeEntry.length);
  setInternalNodeEntry(page, internalNodeEntry, FIRST_ENTRY);
  //update internal header
  internalNodeHeader.numOfEntries++;
  internalNodeHeader.freeSpaceOffset = internalNodeHeader.freeSpaceOffset - internalNodeEntry.length;
  setInternalNodeHeader(page, internalNodeHeader);
}
//used to insert in bewteen an internal node
void IndexManager::injectInternalBetween(void * page, unsigned position, InternalNodeHeader &internalNodeHeader, const void * key, AttrType attrType, unsigned leftChild, unsigned rightChild){
//  cout<<"getting put in position: "<<position<<endl;
  InternalNodeEntry following = getInternalNodeEntry(page, position +1);
  InternalNodeEntry original = getInternalNodeEntry(page, position);
//  cout<< "original off: "<< original.offset<< " following off: "<<following.offset<<endl;
  //node to be inserted
  InternalNodeEntry internalNodeEntry;
  internalNodeEntry.length = getKeySize(attrType, key);
  internalNodeEntry.offset = original.offset - internalNodeEntry.length;
//  cout<< "new off: "<<leafNodeEntry.offset<<endl;
  //update shifted entry offsets
  InternalNodeEntry itr;
  for (unsigned i = position +1 ; i< internalNodeHeader.numOfEntries; i++){
    itr = getInternalNodeEntry(page, i);
    itr.offset = itr.offset - internalNodeEntry.length;
//    cout<< i<< " new off: "<<itr.offset<<endl;
    setInternalNodeEntry(page, itr, i);
  }
  // move data left
  unsigned dest = internalNodeHeader.freeSpaceOffset - internalNodeEntry.length;
  unsigned source = internalNodeHeader.freeSpaceOffset;
  unsigned byteSize = original.offset - internalNodeHeader.freeSpaceOffset;
  void * temp = malloc(byteSize);
  memcpy(temp, page + source, byteSize);
  memcpy(page + dest, temp, byteSize);
  free(temp);
  //move entries right
  dest = sizeof(InternalNodeHeader) + (sizeof(InternalNodeEntry)* (position +2));
  source = sizeof(InternalNodeHeader) + (sizeof(InternalNodeEntry)* (position +1));
  byteSize = (internalNodeHeader.numOfEntries - (position +1)) * sizeof(InternalNodeEntry);
//  cout<< "moving "<< byteSize/sizeof(LeafNodeEntry)<< " entries \n";
  temp = malloc(byteSize);
  memcpy(temp, page + source, byteSize);
  memcpy(page + dest, temp, byteSize);
  free(temp);
  //insert the new one
  following = getInternalNodeEntry(page, position +1);
//    leafNodeEntry.offset = following.offset - leafNodeEntry.length - sizeof(RID);
  memcpy(page + internalNodeEntry.offset, key, internalNodeEntry.length);
  internalNodeHeader.numOfEntries++;
  setInternalNodeEntry(page, internalNodeEntry, position +1);
  //update leaf header
  internalNodeHeader.freeSpaceOffset = internalNodeHeader.freeSpaceOffset - internalNodeEntry.length;
  setInternalNodeHeader(page, internalNodeHeader);

}
//used to insert at the end of an internal node
void IndexManager::injectInternalAfter(void * page, InternalNodeHeader &internalNodeHeader, const void * key, AttrType attrType, unsigned leftChild, unsigned rightChild){
  InternalNodeEntry internalNodeEntry;
  internalNodeEntry.length = getKeySize(attrType, key);
  internalNodeEntry.offset = internalNodeHeader.freeSpaceOffset - internalNodeEntry.length;
  internalNodeEntry.rightChild = rightChild;
  internalNodeEntry.leftChild = leftChild;
  //add data at offset then entry
  memcpy(page + internalNodeEntry.offset, key, internalNodeEntry.length);
  internalNodeHeader.numOfEntries++;
  internalNodeHeader.freeSpaceOffset = internalNodeEntry.offset;
  setInternalNodeHeader(page, internalNodeHeader);
  setInternalNodeEntry(page, internalNodeEntry, internalNodeHeader.numOfEntries -1);
}
//fetches the size of available space in internal node
unsigned IndexManager::getInternalFreeSpace(InternalNodeHeader internalNodeHeader){
  return internalNodeHeader.freeSpaceOffset - sizeof(InternalNodeHeader) - (sizeof(InternalNodeEntry) * internalNodeHeader.numOfEntries);
}
//splits an internal node at the midpoint
RC IndexManager::splitInternalAtEntry(void * page, MetaHeader &metaHeader, InternalNodeHeader &internalNodeHeader, IXFileHandle &ixfileHandle, unsigned midpoint){
  cout<<"spliting an internal node\n";
  //make another internal node
  InternalNodeEntry midEntry = getInternalNodeEntry(page, midpoint);
  InternalNodeHeader rightHeader;
  void * newRight = malloc(PAGE_SIZE);
  memcpy(newRight, page, PAGE_SIZE);
  //move new one's data right
  unsigned dest = internalNodeHeader.freeSpaceOffset + (PAGE_SIZE - midEntry.offset);
  unsigned source = internalNodeHeader.freeSpaceOffset;
  unsigned byteSize = midEntry.offset - internalNodeHeader.freeSpaceOffset;
  memcpy(newRight + dest, page + source, byteSize);
  rightHeader.freeSpaceOffset = internalNodeHeader.freeSpaceOffset + (PAGE_SIZE - midEntry.offset);
  //change offsets
  InternalNodeEntry itr;
  for(unsigned i = midpoint; i < internalNodeHeader.numOfEntries; i++){
    itr = getInternalNodeEntry(newRight, i);
    itr.offset = itr.offset + (PAGE_SIZE - midEntry.offset);
    setInternalNodeEntry(newRight,itr ,i);
  }
  //move new one's entries left
  dest = sizeof(InternalNodeHeader);
  source = sizeof(InternalNodeHeader) + (sizeof(InternalNodeEntry) * (midpoint +1));
  byteSize = sizeof(InternalNodeEntry) *(internalNodeHeader.numOfEntries - midpoint -1);
  void * temp= malloc(byteSize);
  memcpy(temp, newRight + source, byteSize);
  memcpy(newRight + dest, temp, byteSize);
  free(temp);
  rightHeader.numOfEntries = internalNodeHeader.numOfEntries - midpoint -1;
  rightHeader.selfPage = ixfileHandle.getNumberOfPages();
  rightHeader.parentPage = internalNodeHeader.parentPage;
  setInternalNodeHeader(newRight, rightHeader);
  //update old one's offset and number of entries
  internalNodeHeader.freeSpaceOffset = midEntry.offset;
  internalNodeHeader.numOfEntries = midpoint +1;
  setInternalNodeHeader(page,internalNodeHeader);
  //append new one to end of file
  if(ixfileHandle.appendPage(newRight))
    return IX_SPLIT_FAILED;
  metaHeader.numOfInternalNodes++;
  //update the children of the new right to point to it as parent
  if (internalNodeHeader.height -1)
    fixParentPointersOnInternal(newRight, ixfileHandle);
  else
    fixParentPointersOnLeaf(newRight, ixfileHandle);
  //push up mid key if there is a parent
  void * midKey = malloc(midEntry.length);
  getKeyAtOffset(page, midKey, midEntry.offset, midEntry.length);
  if(internalNodeHeader.parentPage){
    pushUpSplitKey(internalNodeHeader.parentPage, metaHeader, ixfileHandle, midKey, metaHeader.type, internalNodeHeader.selfPage, rightHeader.selfPage);
  }
  //otherwise make a parent and push up to it
  else{
    InternalNodeHeader additionalIHeader;
    additionalIHeader.selfPage = ixfileHandle.getNumberOfPages();
    additionalIHeader.numOfEntries =0;
    additionalIHeader.freeSpaceOffset = PAGE_SIZE;
    additionalIHeader.height = internalNodeHeader.height +1;
    internalNodeHeader.parentPage = additionalIHeader.selfPage;
    rightHeader.parentPage = internalNodeHeader.parentPage;
    setInternalNodeHeader(page,internalNodeHeader);
    setInternalNodeHeader(newRight, rightHeader);
    void * newPage = malloc(PAGE_SIZE);
    setInternalNodeHeader(newPage, additionalIHeader);
    ixfileHandle.appendPage(newPage);
    metaHeader.height++;
    metaHeader.rootPage = additionalIHeader.selfPage;
    free(newPage);
    pushUpSplitKey(internalNodeHeader.parentPage, metaHeader, ixfileHandle, midKey, metaHeader.type, internalNodeHeader.selfPage, rightHeader.selfPage);
  }
  free(midKey);
  free(newRight);
  return SUCCESS;
}
//used to change parent pointers in leaves
void IndexManager::fixParentPointersOnLeaf(void * page, IXFileHandle &ixfileHandle){
  InternalNodeHeader iHeader = getInternalNodeHeader(page);
  InternalNodeEntry itr;
  LeafNodeHeader headItr;
  unsigned newParentPage = ixfileHandle.getNumberOfPages() -1;
  void * workingPage = malloc(PAGE_SIZE);
  for (unsigned i =0; i <iHeader.numOfEntries; i ++){
    itr = getInternalNodeEntry(page,i);
    ixfileHandle.readPage(itr.leftChild,workingPage);
    headItr = getLeafNodeHeader(workingPage);
    headItr.parentPage = newParentPage;
    setLeafNodeHeader(workingPage, headItr);
    ixfileHandle.writePage(itr.leftChild, workingPage);
    ixfileHandle.readPage(itr.rightChild, workingPage);
    headItr = getLeafNodeHeader(workingPage);
    headItr.parentPage = newParentPage;
    setLeafNodeHeader(workingPage, headItr);
    ixfileHandle.writePage(itr.rightChild, workingPage);
  }
  free(workingPage);
}
//used to change parent pointer in internals
void IndexManager::fixParentPointersOnInternal(void * page, IXFileHandle &ixfileHandle){
  InternalNodeHeader iHeader = getInternalNodeHeader(page);
  InternalNodeEntry itr;
  InternalNodeHeader headItr;
  unsigned newParentPage = ixfileHandle.getNumberOfPages() -1;
  void * workingPage = malloc(PAGE_SIZE);
  for (unsigned i =0; i <iHeader.numOfEntries; i ++){
    itr = getInternalNodeEntry(page,i);
    ixfileHandle.readPage(itr.leftChild,workingPage);
    headItr = getInternalNodeHeader(workingPage);
    headItr.parentPage = newParentPage;
    setInternalNodeHeader(workingPage, headItr);
    ixfileHandle.writePage(itr.leftChild, workingPage);
    ixfileHandle.readPage(itr.rightChild, workingPage);
    headItr = getInternalNodeHeader(workingPage);
    headItr.parentPage = newParentPage;
    setInternalNodeHeader(workingPage, headItr);
    ixfileHandle.writePage(itr.rightChild, workingPage);
  }
  free(workingPage);
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
    unsigned dest =leafNodeHeader.freeSpaceOffset - leafNodeEntry.length - sizeof(RID);
    unsigned source = leafNodeHeader.freeSpaceOffset;
    unsigned byteSize = PAGE_SIZE - leafNodeHeader.freeSpaceOffset;
    void * temp = malloc(byteSize);
    memcpy(temp, page + source, byteSize);
    memcpy(page + dest, temp, byteSize);
    free(temp);
    //move entries right
    dest = sizeof(LeafNodeHeader) + sizeof(LeafNodeEntry);
    source = sizeof(LeafNodeHeader);
    byteSize = leafNodeHeader.numOfEntries * sizeof(LeafNodeEntry);
    temp = malloc(byteSize);
    memcpy(temp, page + source, byteSize);
    memcpy(page + dest, temp, byteSize);
    free(temp);
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
//    cout<<"getting put in position: "<<position<<endl;
    LeafNodeEntry following = getLeafNodeEntry(page, position +1);
    LeafNodeEntry original = getLeafNodeEntry(page, position);
//    cout<< "original off: "<< original.offset<< " following off: "<<following.offset<<endl;
    //node to be inserted
    LeafNodeEntry leafNodeEntry;
    leafNodeEntry.length = getKeySize(attrType, key);
    leafNodeEntry.offset = original.offset - sizeof(RID) - leafNodeEntry.length;
//    cout<< "new off: "<<leafNodeEntry.offset<<endl;
    leafNodeEntry.status = alive;
    leafNodeEntry.numberOfRIDs = 1;

    //update shifted entry offsets
    LeafNodeEntry itr;
    for (unsigned i = position +1 ; i< leafNodeHeader.numOfEntries; i++){
      itr = getLeafNodeEntry(page, i);
      itr.offset = itr.offset - leafNodeEntry.length - sizeof(RID);
//      cout<< i<< " new off: "<<itr.offset<<endl;
      setLeafNodeEntry(page, itr, i);
    }
    // move data left
    unsigned dest = leafNodeHeader.freeSpaceOffset - leafNodeEntry.length - sizeof(RID);
    unsigned source = leafNodeHeader.freeSpaceOffset;
    unsigned byteSize = original.offset - leafNodeHeader.freeSpaceOffset;
    void * temp = malloc(byteSize);
    memcpy(temp, page + source, byteSize);
    memcpy(page + dest, temp, byteSize);
    free(temp);
    //move entries right
    dest = sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry)* (position +2));
    source = sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry)* (position +1));
    byteSize = (leafNodeHeader.numOfEntries - (position +1)) * sizeof(LeafNodeEntry);
    temp = malloc(byteSize);
//    cout<< "moving "<< byteSize/sizeof(LeafNodeEntry)<< " entries \n";
    memcpy(temp, page + source, byteSize);
    memcpy(page + dest, temp, byteSize);
    //insert the new one
    following = getLeafNodeEntry(page, position +1);
//    leafNodeEntry.offset = following.offset - leafNodeEntry.length - sizeof(RID);
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
RC IndexManager::addAdditionalRID(void * page,LeafNodeHeader leafNodeHeader, LeafNodeEntry leafNodeEntry, RID newRid, unsigned entryPos){
  //move data left
//  cout<< "size ="<<leafNodeEntry.length+ sizeof(RID)<<endl;
  if(checkRidDuplicate(page, leafNodeEntry, newRid))
    return IX_RID_DUPLICATE;
  unsigned dest = leafNodeHeader.freeSpaceOffset - sizeof(RID);
  unsigned source = leafNodeHeader.freeSpaceOffset;
//  cout<<"preiviously has this many rid's: "<<leafNodeEntry.numberOfRIDs<<endl;
  unsigned byteSize = leafNodeEntry.offset  + leafNodeEntry.length + (sizeof(RID) * leafNodeEntry.numberOfRIDs)- leafNodeHeader.freeSpaceOffset;
//  cout<< "dest, origin, size "<<dest<< " "<<source<< " "<<byteSize<<endl;
  void * temp = malloc(byteSize);
  memcpy(temp, page + source, byteSize);
  memcpy(page + dest, temp, byteSize);
  free(temp);
  leafNodeHeader.freeSpaceOffset = leafNodeHeader.freeSpaceOffset - sizeof(RID);
  setLeafNodeHeader(page, leafNodeHeader);
  LeafNodeEntry check;
  void * tempKey = malloc(INT_SIZE);
  for (unsigned i = entryPos; i < leafNodeHeader.numOfEntries; i ++){
//    cout<< "checking key at changed offset\n";
    check = getLeafNodeEntry(page, i);
    check.offset = check.offset - sizeof(RID);
    getKeyAtOffset(page,tempKey,check.offset, INT_SIZE);
//    printKey(tempKey, TypeInt);
//    cout<< "\n";
    setLeafNodeEntry(page, check, i);
  }
  free(tempKey);
  leafNodeEntry = getLeafNodeEntry(page, entryPos);
  memcpy(page + leafNodeEntry.offset + leafNodeEntry.length + (leafNodeEntry.numberOfRIDs * sizeof(RID)), &newRid, sizeof(RID));
  leafNodeEntry.numberOfRIDs++;
  setLeafNodeEntry(page, leafNodeEntry, entryPos);
  return SUCCESS;
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
	cout<<" has parent on page: "<<lHeader.parentPage<<endl;
}
//used to print out the offsets for debugging purposes
void IndexManager::showLeafOffsetsAndLengths(void * page){
  LeafNodeHeader lHeader = getLeafNodeHeader(page);
  cout<< "H freeSpaceOffset: "<<lHeader.freeSpaceOffset<< " H entry total: "<< lHeader.numOfEntries;
  LeafNodeEntry lEntry;
  for (unsigned i =0; i < lHeader.numOfEntries; i ++){
    lEntry = getLeafNodeEntry(page, i);
    cout<< " E offset: "<<lEntry. offset<< " E length: "<< lEntry.length<< " E rids: "<<lEntry.numberOfRIDs;
  }
  cout<< endl;
}

//displays all keys in internal node for debugging
void IndexManager::showInternalKeysAndChildren(void * page, AttrType attrType){
  InternalNodeHeader iHeader = getInternalNodeHeader(page);
  void * key;
  InternalNodeEntry itr;
  cout<< "internal keys and children: \n";
  for (unsigned i =0; i < iHeader.numOfEntries; i ++){
    itr = getInternalNodeEntry(page, i);
    key = malloc(itr.length);
    getKeyAtOffset(page, key, itr.offset, itr.length);
    cout<< " key:";
    printKey(key, attrType);
    cout<< "left: "<<itr.leftChild<< " right: "<<itr.rightChild;
    free(key);
  }
  cout<<endl;
}
//shows all internal node stored values for debugging
void IndexManager::showInternalStatistics(void * page){
  InternalNodeHeader iHeader = getInternalNodeHeader(page);
  cout<< "On internal page: "<<iHeader.selfPage<<" height: "<< iHeader.height<< " parent: "<< iHeader.parentPage<<endl;
  cout<< "freespace offset: "<< iHeader.freeSpaceOffset<< " total entries: "<< iHeader.numOfEntries<<endl;
  InternalNodeEntry itr;
  for (unsigned i =0; i < iHeader.numOfEntries; i++){
    itr = getInternalNodeEntry(page, i);
    cout<< i<< " offset: "<<itr.offset<< " ";
  }
  cout<<endl;
}
//shows all metaheader stats for debbugging
void IndexManager::showMetaHeaderStatistics(void * page){
  MetaHeader mHeader = getMetaHeader(page);
  cout<< "meta header stats:\nroot: "<<mHeader.rootPage<< " # of internal: "<<mHeader.numOfInternalNodes<< " # of leafs: "<<mHeader.numOfLeafNodes<< " height: "<<mHeader.height<<endl;
}
//returns the metaHeader from page 0
MetaHeader IXFileHandle::fhGetMetaHeader(void * page)
{
    MetaHeader mHeader;
    memcpy(&mHeader,page,sizeof(MetaHeader));
    return mHeader;
}
//sets the meta node
void IXFileHandle::setMetaNode(MetaNode * mNodeEntry, unsigned pageNum, unsigned height)
{
    mNodeEntry->pageNum = pageNum;
    mNodeEntry->height = height;
}
//print tree helper
RC IXFileHandle::fhPrintLeafNode(IXFileHandle ixfileHandle, unsigned height, unsigned pageNum, AttrType type, stack<MetaNode> * pageNumStack)
{
    LeafNodeHeader lNodeHeader;
    LeafNodeEntry lNodeEntry;

    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);

    lNodeHeader = ixfileHandle.fhGetLeafNodeHeader(page);

    // entries should be >= 0
    if (lNodeHeader.numOfEntries < 0)
    {
        free(page);
        return IX_READ_FAILED;
    }

    int iKey;
    float rKey;
    void * rawKey;
    char * charKey;
    string strKey;
    unsigned keyLength;

    RID rid;
    unsigned ridOffset;
    unsigned k;

    cout << string(height, '\t') << "{\"keys\":[";
    // print each key and corresponding RIDs in the leaf node
    for (unsigned i = 0; i < lNodeHeader.numOfEntries; i++)
    {
        lNodeEntry = ixfileHandle.fhGetLeafNodeEntry(page, i);

        switch (type)
        {
            case TypeInt:
            {
                // print key
                memcpy(&iKey, (page + lNodeEntry.offset), sizeof(int));
                cout << "\"" << iKey << ":[";

                // print RIDs
                for (k = 0; k < lNodeEntry.numberOfRIDs; k++)
                {
                    ridOffset = lNodeEntry.offset + sizeof(int) + (k * sizeof(RID));
                    rid = getRid(page, ridOffset);
                    cout << "(" << rid.pageNum << "," << rid.slotNum << ")";

                    // print "," when it is not the last RID of key
                    if (k < (lNodeEntry.numberOfRIDs - 1))
                        cout << ",";
                }
                break;
            }
            case TypeReal:
            {
                // print key
                memcpy(&rKey, (page + lNodeEntry.offset), sizeof(float));
                cout << "\"" << rKey << ":[";

                // print RIDs
                for (k = 0; k < lNodeEntry.numberOfRIDs; k++)
                {
                    ridOffset = lNodeEntry.offset + sizeof(float) + (k * sizeof(RID));
                    rid = getRid(page, ridOffset);
                    cout << "(" << rid.pageNum << "," << rid.slotNum << ")";

                    // print "," when it is not the last RID of key
                    if (k < (lNodeEntry.numberOfRIDs - 1))
                        cout << ",";
                }
                break;
            }
            case TypeVarChar:
            {
                // print key
                memcpy(&keyLength, (page + lNodeEntry.offset), sizeof(int));
                rawKey = malloc(keyLength);
                memcpy(rawKey, (page + lNodeEntry.offset + sizeof(int)), keyLength);
                charKey = (char *) rawKey;
                strKey = charKey;
                cout << "\"" << strKey << ":[";

                // print RIDs
                for (k = 0; k < lNodeEntry.numberOfRIDs; k++)
                {
                    ridOffset = lNodeEntry.offset + sizeof(int) + keyLength + (k * sizeof(RID));
                    rid = getRid(page, ridOffset);
                    cout << "(" << rid.pageNum << "," << rid.slotNum << ")";

                    // print "," when it is not the last RID of key
                    if (k < (lNodeEntry.numberOfRIDs - 1))
                        cout << ",";
                }

                free(rawKey);
                break;
            }
            default:
            {
                free(page);
                return IX_TYPE_ERROR;
            }
        }

        cout << "]\"";
        if (i < (lNodeHeader.numOfEntries -1))
            cout << ",";
    }
    cout << "]}";

    if (!pageNumStack->empty())
    {
        // if next node on stack is leaf node
        // print a ","
        if ( (pageNumStack->top()).height == height)
            cout << ",";
    }
    cout << '\n';

    free(page);
    return SUCCESS;
}
//prints internal node
RC IXFileHandle::fhPrintInternalNode(IXFileHandle ixfileHandle, unsigned height, unsigned pageNum, AttrType type, stack<MetaNode> &pageNumStack)
{
    InternalNodeHeader iNodeHeader;
    InternalNodeEntry iNodeEntry;

    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);

    iNodeHeader = ixfileHandle.fhGetInternalNodeHeader(page);

    // entries should be >= 0
    if (iNodeHeader.numOfEntries < 0)
    {
        free(page);
        return IX_READ_FAILED;
    }

    int iKey;
    float rKey;
    void * rawKey;
    char * charKey;
    string strKey;
    unsigned keyLength;
    unsigned childrenPageNumArr[iNodeHeader.numOfEntries + 1];
    MetaNode mNode;

    RID rid;
    unsigned ridOffset;

    cout << string(height, '\t') << "{\"keys\":[";

    // print keys
    for (unsigned i = 0; i < iNodeHeader.numOfEntries; i++)
    {
        iNodeEntry = ixfileHandle.fhGetInternalNodeEntry(page, i);

        switch (type)
        {
            case TypeInt:
            {
                // print key
                memcpy(&iKey, (page + iNodeEntry.offset), sizeof(int));
                cout << iKey;
                break;
            }

            case TypeReal:
            {
                // print key
                memcpy(&rKey, (page + iNodeEntry.offset), sizeof(float));
                cout << rKey;
                break;
            }

            case TypeVarChar:
            {
                // print key
                memcpy(&keyLength, (page + iNodeEntry.offset), sizeof(int));
                rawKey = malloc(keyLength);
                memcpy(rawKey, (page + iNodeEntry.offset + sizeof(int)), keyLength);
                charKey = (char *) rawKey;
                strKey = charKey;
                cout << "\"" << strKey << "\"";
                break;
            }

            default:
            {
                free(page);
                return IX_TYPE_ERROR;
            }
        }

        if (i < (iNodeHeader.numOfEntries -1))
            cout << ",";

        // store the page number of each child
        // push them to stack afterward
        childrenPageNumArr[i] = iNodeEntry.leftChild;
    }

    // store last child (right child of the last entry in node)
    childrenPageNumArr[iNodeHeader.numOfEntries] = iNodeEntry.rightChild;

    // push the page number of childern to stack
    // in reversed order (the most right child pushed first)
    // so the most left child would be at the top of the stack
    for (int k = iNodeHeader.numOfEntries; k >= 0; k--)
    {
        setMetaNode(&mNode, childrenPageNumArr[k], (height + 1));
        pageNumStack.push(mNode);
    }

    cout << "],\n";
    cout << "\n" << string(height, '\t') << "\"children\":[\n";

    return SUCCESS;
}
//get leaf node header from passed in passed in page
LeafNodeHeader IXFileHandle::fhGetLeafNodeHeader(void * page){
    LeafNodeHeader lHeader;
    memcpy(&lHeader, page, sizeof(LeafNodeHeader));
    return lHeader;
}

//get InternalNodeHeader
InternalNodeHeader IXFileHandle::fhGetInternalNodeHeader(void * page){
    InternalNodeHeader iHeader;
    memcpy(&iHeader, page, sizeof(InternalNodeHeader));
    return iHeader;
}
//return a specific leaf node entry
LeafNodeEntry IXFileHandle::fhGetLeafNodeEntry(void * page, unsigned slotNumber){
    LeafNodeEntry lEntry;
    memcpy(&lEntry, page + sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry) * slotNumber), sizeof(LeafNodeEntry));
    return lEntry;
}

//return a specific Internal node entry
InternalNodeEntry IXFileHandle::fhGetInternalNodeEntry(void * page, unsigned slotNumber){
    InternalNodeEntry iEntry;
    memcpy(&iEntry, page + sizeof(InternalNodeHeader) + (sizeof(InternalNodeEntry) * slotNumber), sizeof(InternalNodeEntry));
    return iEntry;
}
//returns rid
RID IXFileHandle::getRid(void * page, unsigned offset)
{
    RID rid;
    memcpy(&rid, (page + offset), sizeof(RID));
    return rid;
}
//************************************scan helpers
//used to initialize a scan iterator
RC IX_ScanIterator::scanInit(IXFileHandle &ixfH,
        const Attribute &attr,
        const void      *lowKey,
        const void      *highKey,
        bool  lowKeyInclusive,
        bool  highKeyInclusive)
{
//  cout<<"initializing ix iterator\n";
  //start at the first leaf first entry;
  currPage = INITIAL_PAGE;
  currEntry = FIRST_ENTRY;
  currRid = FIRST_RID;
  totalLeaves =0;
  totalEntry =0;
  attrType = attr.type;
  //buffer for the current pageNum
  pageData = malloc(PAGE_SIZE);
  allocatedPage = true;

  //store variables passed into
  ixfh = &ixfH;
  low = lowKey;
  //check for unconditional scans
  if (lowKey == NULL)
    infiniteLow = true;
  if (highKey == NULL)
    infiniteHigh = true;
  high = highKey;
  lowInc = lowKeyInclusive;
  highInc = highKeyInclusive;

  skipList.clear();

  //use metahaeder to get the total number of leaves
  if (!ixfh->getNumberOfPages()){
//    cout<< "no pages\n";
    return SUCCESS;
  }
  ixfh->readPage(META_PAGE, pageData);
  MetaHeader mHeader = im->getMetaHeader(pageData);
  totalLeaves = mHeader.numOfLeafNodes;
//  cout<< "total leaves: "<<totalLeaves<<endl;
  if(totalLeaves > 0){
    if(ixfh->readPage(INITIAL_PAGE, pageData))
      return IX_READ_FAILED;
  }
  //get number of entries in the initial page
  LeafNodeHeader lHeader = im->getLeafNodeHeader(pageData);
  totalEntry = lHeader.numOfEntries;

  return SUCCESS;
}

RC IX_ScanIterator::scanCurrPage(RID &rid, void *key)
{
    LeafNodeEntry lNodeEntry;
    void * tempKey;
    unsigned keyLength;
    bool isMatchedLowCondition;
    bool isMatchedHighCondition;

    isMatchedLowCondition = false;
    isMatchedHighCondition = false;
    int compareValue;
    unsigned ridOffset;
    unsigned numberOfRIDs;

    switch (attrType)
    {
        case TypeInt:
        {
            while(currEntry < totalEntry)
            {
                // get key
                lNodeEntry = im->getLeafNodeEntry(pageData, currEntry);

                // check if the key is deleted
                if (lNodeEntry.status == dead)
                {
                    currEntry += 1;
                    continue;
                }

                // check if all the RIDs of the current entry are printed
                numberOfRIDs = lNodeEntry.numberOfRIDs;
                if (currRid >= numberOfRIDs)
                {
                    currRid = FIRST_RID;
                    currEntry += 1;
                    continue;
                }

                tempKey = malloc(sizeof(int));
                memcpy(tempKey, (pageData + lNodeEntry.offset), sizeof(int));

                // find if key low key conditions
                if (infiniteLow)
                {
                    // no limitation on low key
                    isMatchedLowCondition = true;
                }
                else
                {
                    compareValue = im->compareInts(tempKey, low);

                    // find if key in entry is greater than the low key
                    if (compareValue == GREATER_THAN)
                        isMatchedLowCondition = true;
                    else if (lowInc && compareValue == EQUAL_TO)
                        isMatchedLowCondition = true;
                    else
                        isMatchedLowCondition = false;
                }

                // find if key high key conditions
                if (infiniteHigh)
                {
                    // no limitation on high key
                    isMatchedHighCondition = true;
                }
                else
                {
                    compareValue = im->compareInts(high, tempKey);

                    // find if key in entry is less than the high key
                    if (compareValue == GREATER_THAN)
                        isMatchedHighCondition = true;
                    else if (highInc && compareValue == EQUAL_TO)
                        isMatchedHighCondition = true;
                    else
                    {
                        // all the keys that have not been scan > high key
                        free(tempKey);
                        return IX_EOF;
                    }
                }

                // found key that matched both low and high conditions
                if (isMatchedLowCondition && isMatchedHighCondition)
                {
                    // set RID
                    ridOffset = lNodeEntry.offset + sizeof(int) + (currRid * sizeof(RID));
                    memcpy(&rid, (pageData + ridOffset), sizeof(RID));

                    // set key
                    memcpy(key, tempKey, sizeof(int));

                    // update the info
                    currRid += 1;

                    free(tempKey);
                    return SUCCESS;
                }

                currRid = FIRST_RID;
                currEntry += 1;
            }

            free(tempKey);
            break;
        }

        case TypeReal:
        {
            while(currEntry < totalEntry)
            {
                // get key
                lNodeEntry = im->getLeafNodeEntry(pageData, currEntry);

                // check if the key is deleted
                if (lNodeEntry.status == dead)
                {
                    currEntry += 1;
                    continue;
                }

                // check if all the RIDs of the current entry are printed
                numberOfRIDs = lNodeEntry.numberOfRIDs;
                if (currRid >= numberOfRIDs)
                {
                    currRid = FIRST_RID;
                    currEntry += 1;
                    continue;
                }

                tempKey = malloc(sizeof(float));
                memcpy(tempKey, (pageData + lNodeEntry.offset), sizeof(float));

                // find if key low key conditions
                if (infiniteLow)
                {
                    // no limitation on low key
                    isMatchedLowCondition = true;
                }
                else
                {
                    compareValue = im->compareReals(tempKey, low);

                    // find if key in entry is greater than the low key
                    if (compareValue == GREATER_THAN)
                        isMatchedLowCondition = true;
                    else if (lowInc && compareValue == EQUAL_TO)
                        isMatchedLowCondition = true;
                    else
                        isMatchedLowCondition = false;
                }

                // find if key high key conditions
                if (infiniteHigh)
                {
                    // no limitation on high key
                    isMatchedHighCondition = true;
                }
                else
                {
                    compareValue = im->compareReals(high, tempKey);

                    // find if high key is greater than key in entry
                    if (compareValue == GREATER_THAN)
                        isMatchedHighCondition = true;
                    else if (highInc && compareValue == EQUAL_TO)
                        isMatchedHighCondition = true;
                    else
                    {
                        // all the keys that have not been scan are greter than high key
                        free(tempKey);
                        return IX_EOF;
                    }
                }

                // found key that matched both low and high conditions
                if (isMatchedLowCondition && isMatchedHighCondition)
                {
                    // set RID
                    ridOffset = lNodeEntry.offset + sizeof(float) + (currRid * sizeof(RID));
                    memcpy(&rid, pageData + ridOffset, sizeof(RID));

                    // set key
                    memcpy(key, tempKey, sizeof(int));

                    // update the info
                    currRid += 1;

                    free(tempKey);
                    return SUCCESS;
                }

                // key in current entry is not matched
                // go the the next entry
                currRid = FIRST_RID;
                currEntry += 1;
            }

            free(tempKey);
            break;
        }

        case TypeVarChar:
        {
            while(currEntry < totalEntry)
            {
                // get key
                lNodeEntry = im->getLeafNodeEntry(pageData, currEntry);

                // check if the key is deleted
                if (lNodeEntry.status == dead)
                {
                    currEntry += 1;
                    continue;
                }

                // check if all the RIDs of the current entry are printed
                numberOfRIDs = lNodeEntry.numberOfRIDs;
                if (currRid >= numberOfRIDs)
                {
                    currRid = FIRST_RID;
                    currEntry += 1;
                    continue;
                }

                // read key from leaf node
                memcpy(&keyLength, (pageData + lNodeEntry.offset), sizeof(int));

                tempKey = malloc(keyLength);
                memcpy(tempKey, (pageData + lNodeEntry.offset + sizeof(int)), keyLength);

                // find if key low key conditions
                if (infiniteLow)
                {
                    // no limitation on low key
                    isMatchedLowCondition = true;
                }
                else
                {
                    compareValue = im->compareVarChars(tempKey, low);

                    // find if key in entry is greater than the low key
                    if (compareValue == GREATER_THAN)
                        isMatchedLowCondition = true;
                    else if (lowInc && compareValue == EQUAL_TO)
                        isMatchedLowCondition = true;
                    else
                        isMatchedLowCondition = false;
                }

                // find if key high key conditions
                if (infiniteHigh)
                {
                    // no limitation on high key
                    isMatchedHighCondition = true;
                }
                else
                {
                    compareValue = im->compareVarChars(high, tempKey);

                    // find if high key is greater than key in entry
                    if (compareValue == GREATER_THAN)
                        isMatchedHighCondition = true;
                    else if (highInc && compareValue == EQUAL_TO)
                        isMatchedHighCondition = true;
                    else
                    {
                        // all the keys that have not been scan are greter than high key
                        free(tempKey);
                        return IX_EOF;
                    }
                }

                // found key that matched both low and high conditions
                if (isMatchedLowCondition && isMatchedHighCondition)
                {
                    // set RID
                    ridOffset = lNodeEntry.offset + keyLength + (currRid * sizeof(RID));
                    memcpy(&rid, pageData + ridOffset, sizeof(RID));

                    // set key
                    memcpy(key, tempKey, keyLength);

                    // update the info
                    currRid += 1;

                    free(tempKey);
                    return SUCCESS;
                }

                // key in current entry is not matched
                // go the the next entry
                currRid = FIRST_RID;
                currEntry += 1;
            }

            free(tempKey);
            break;
        }

        default:
            return IX_TYPE_ERROR;
    }

    // all keys in current leaf node is scanned
    // and printed if they are between low and high key
    return IX_NOT_FOUND;
}
//delete helpers
RC IndexManager::searchLeafNode(IXFileHandle ixfileHandle, unsigned pageNum, AttrType type, const void * key, RID rid, IndexId * indexId)
{
    LeafNodeHeader lNodeHeader;
    LeafNodeEntry lNodeEntry;

    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);

    lNodeHeader = getLeafNodeHeader(page);

    // entries should be >= 0
    if (lNodeHeader.numOfEntries < 0)
    {
        free(page);
        return IX_READ_FAILED;
    }

    void * rawKey;
    unsigned keyLength;
    RID tempRid;
    unsigned ridOffset;
    unsigned k;

    // find key
    for (unsigned i = 0; i < lNodeHeader.numOfEntries; i++)
    {
        lNodeEntry = getLeafNodeEntry(page, i);

        switch (type)
        {
            case TypeInt:
            {
                rawKey = malloc(sizeof(int));
                memcpy(rawKey, (page + lNodeEntry.offset), sizeof(int));
                if (compareInts(key, rawKey) == EQUAL_TO)
                {
                    // find RID
                    for (k = 0; k < lNodeEntry.numberOfRIDs; i++)
                    {
                        ridOffset = lNodeEntry.offset + sizeof(int) + (k * sizeof(RID));
                        tempRid = ixfileHandle.getRid(page, ridOffset);

                        if (tempRid.pageNum == rid.pageNum && tempRid.slotNum == rid.slotNum)
                        {
                            indexId->pageId = pageNum;
                            indexId->entryId = i;

                            free(rawKey);
                            free(page);
                            return SUCCESS;
                        }
                    }
                }
                free(rawKey);
                break;
            }

            case TypeReal:
            {
                rawKey = malloc(sizeof(float));
                memcpy(rawKey, (page + lNodeEntry.offset), sizeof(float));
                if (compareReals(key, rawKey) == EQUAL_TO)
                {
                    // find RID
                    for (k = 0; k < lNodeEntry.numberOfRIDs; i++)
                    {
                        ridOffset = lNodeEntry.offset +  sizeof(float) + (k * sizeof(RID));
                        rid = ixfileHandle.getRid(page, ridOffset);

                        if (tempRid.pageNum == rid.pageNum && tempRid.slotNum == rid.slotNum)
                        {
                            indexId->pageId = pageNum;
                            indexId->entryId = i;

                            free(rawKey);
                            free(page);
                            return SUCCESS;
                        }
                    }
                }
                free(rawKey);
                break;
            }

            case TypeVarChar:
            {
                memcpy(&keyLength, (page + lNodeEntry.offset), sizeof(int));
                rawKey = malloc(keyLength);
                memcpy(rawKey, (page + lNodeEntry.offset + sizeof(int)), keyLength);

                if (compareVarChars(key, rawKey) == EQUAL_TO)
                {
                    for (k = 0; k < lNodeEntry.numberOfRIDs; i++)
                    {
                        ridOffset = lNodeEntry.offset + sizeof(int) + keyLength + (k * sizeof(RID));
                        rid = ixfileHandle.getRid(page, ridOffset);

                        if (tempRid.pageNum == rid.pageNum && tempRid.slotNum == rid.slotNum)
                        {
                            indexId->pageId = pageNum;
                            indexId->entryId = i;

                            free(rawKey);
                            free(page);
                            return SUCCESS;
                        }
                    }
                }
                free(rawKey);
                break;
            }

            default:
            {
                free(page);
                return IX_TYPE_ERROR;
            }
        }
    }
    cout<< "did not exists\n";
    free(page);
    return IX_TARGET_DOES_EXIST;
}

unsigned IndexManager::getNextNodePageNum(IXFileHandle ixfileHandle,  unsigned pageNum, AttrType type, const void * key, RID rid)
{
    InternalNodeHeader iNodeHeader;
    InternalNodeEntry iNodeEntry;

    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);

    iNodeHeader = getInternalNodeHeader(page);

    void * rawKey;
    unsigned keyLength;
    RID tempRid;
    unsigned ridOffset;
    unsigned k;

    // compare the keys in entry with search key
    // find the proper child
    for (unsigned i = 0; i < iNodeHeader.numOfEntries; i++)
    {
        iNodeEntry = getInternalNodeEntry(page, i);

        switch (type)
        {
            case TypeInt:
            {
                rawKey = malloc(sizeof(int));
                memcpy(rawKey, (page + iNodeEntry.offset), sizeof(int));
                if (compareInts(key, rawKey) == GREATER_THAN)
                {
                    free(rawKey);
                    return iNodeEntry.leftChild;
                }
                free(rawKey);
                break;
            }

            case TypeReal:
            {
                rawKey = malloc(sizeof(float));
                memcpy(rawKey, (page + iNodeEntry.offset), sizeof(float));
                if (compareReals(key, rawKey) == GREATER_THAN)
                {
                    free(rawKey);
                    return iNodeEntry.leftChild;
                }
                free(rawKey);
                break;
            }

            case TypeVarChar:
            {
                memcpy(&keyLength, (page + iNodeEntry.offset), INT_SIZE);
                rawKey = malloc(keyLength);
                memcpy(rawKey, (page + iNodeEntry.offset + sizeof(int)), keyLength);

                if (compareVarChars(key, rawKey) == GREATER_THAN)
                {
                    free(rawKey);
                    return iNodeEntry.leftChild;
                }
                free(rawKey);
                break;
            }
        }
    }

    free(page);

    // when the search key the greatest
    // the right most child would be the proper position
    return iNodeEntry.rightChild;
}
//physical deletion of an rid in memory
RC IndexManager::deleteRID(void * page, LeafNodeHeader &leafNodeHeader, unsigned entryPos,const RID &rid){
  //iterate through rids see if there is a match
//  cout<< "deleting rid\n";
  LeafNodeEntry leafNodeEntry = getLeafNodeEntry(page, entryPos);
  LeafNodeEntry itr;
  unsigned dest;
  unsigned source;
  unsigned byteSize;
  void * temp;
  for(int i =0; i <leafNodeEntry.numberOfRIDs; i++){
    if(!memcmp(page + leafNodeEntry.offset + leafNodeEntry.length + (i * sizeof(RID)), &rid, sizeof(RID))){
      //there is a match
      if (leafNodeEntry.numberOfRIDs ==1){
        //clear the entire thing
        return clearEntry(page, leafNodeHeader, leafNodeEntry, entryPos);
      }
      //clear a single entry
      dest = leafNodeHeader.freeSpaceOffset + sizeof(RID);
      source = leafNodeHeader.freeSpaceOffset;
      byteSize = leafNodeEntry.offset + leafNodeEntry.length + (sizeof(RID) * i) - leafNodeHeader.freeSpaceOffset;
      temp = malloc(byteSize);
      memcpy(temp, page + source, byteSize);
      memcpy(page + dest, temp, byteSize);
      free(temp);
      leafNodeEntry.offset += sizeof(RID);
      leafNodeHeader.freeSpaceOffset += sizeof(rid);
      leafNodeEntry.numberOfRIDs--;
      for (unsigned i = entryPos + 1; i <leafNodeHeader.numOfEntries; i ++){
        itr = getLeafNodeEntry(page, i);
        itr.offset += sizeof(RID);
        setLeafNodeEntry(page, itr, i);
      }
      setLeafNodeHeader(page, leafNodeHeader);
      setLeafNodeEntry(page, leafNodeEntry, entryPos);
      return SUCCESS;
    }
  }
  return IX_RID_NOT_FOUND;
}
//physically clear and entire entry
RC IndexManager::clearEntry(void * page, LeafNodeHeader &leafNodeHeader, LeafNodeEntry leafNodeEntry, unsigned entryPos){
  //shift memory right over key and rid
//  cout<< "clearing entry\n";
  unsigned dest = leafNodeHeader.freeSpaceOffset + leafNodeEntry.length + sizeof(RID);
  unsigned source = leafNodeHeader.freeSpaceOffset;
  unsigned byteSize = leafNodeEntry.offset - leafNodeHeader.freeSpaceOffset;
//  cout<< "dest, source, byteSize: "<< dest<< " "<< source<< " "<< byteSize<<endl;
  void * temp = malloc(byteSize);
  memcpy(temp, page + source, byteSize);
  memcpy(page + dest, temp, byteSize);
  free(temp);
  //update all following offsets
  LeafNodeEntry itr;
  for(unsigned i = entryPos +1; i < leafNodeHeader.numOfEntries; i ++){
//    cout<< "changing offset\n";
    itr = getLeafNodeEntry(page, i);
    itr.offset = itr.offset + leafNodeEntry.length + sizeof(RID);
    setLeafNodeEntry(page, itr, i);
  }
//  cout<< "offsets done\n";
  //move following entries left to remove original entry
  dest = sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry) * entryPos);
  source = sizeof(LeafNodeHeader) + (sizeof(LeafNodeEntry) * (entryPos +1));
  byteSize = sizeof(LeafNodeEntry) * (leafNodeHeader.numOfEntries - entryPos -1);
//  cout<< "dest, source, byteSize: "<< dest<< " "<< source<< " "<< byteSize<<endl;
  temp = malloc(byteSize);
  memcpy(temp, page + source, byteSize);
  memcpy(page + dest, temp, byteSize);
  free(temp);
//  cout<< "memcpy done\n";
  leafNodeHeader.numOfEntries--;
//  cout<< "entries left: "<< leafNodeHeader.numOfEntries<<endl;
  leafNodeHeader.freeSpaceOffset = leafNodeHeader.freeSpaceOffset + leafNodeEntry.length + sizeof(RID);
  setLeafNodeHeader(page, leafNodeHeader);
//  showLeafOffsetsAndLengths(page);
  return SUCCESS;
}
//used to check if there is no prexisting rid in an entry
RC IndexManager::checkRidDuplicate(void * page, LeafNodeEntry &leafNodeEntry, RID rid){
  unsigned source = leafNodeEntry.offset + leafNodeEntry.length;
  //loop and see if there is a memory exiilancy
  for (unsigned i = 0; i < leafNodeEntry.numberOfRIDs; i++){
    if(!memcmp(page + source + (i * sizeof(RID)), &rid, sizeof(RID)))
      return IX_RID_DUPLICATE;
  }
  //no duplicate found
  return SUCCESS;
}
