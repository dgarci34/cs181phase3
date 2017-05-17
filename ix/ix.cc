
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
    //check for a double open
   if(ixfileHandle.getFd() != NULL)
       return IX_HANDLE_IN_USE;
    //check for file existance
    if(!fileExists(fileName.c_str()))
        return IX_FILE_DN_EXIST;
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

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
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

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
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

bool IndexManager::fileExists(const string &fileName)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
  ixReadPageCounter = readPageCount;
  ixWritePageCounter = writePageCount;
  ixAppendPageCounter = appendPageCount;
    return SUCCESS;
}

void IXFileHandle::setFd(FILE * file){
    _file = file;           //set file pointer
}

FILE * IXFileHandle::getFd(){
    return _file;           //return the file pointer
}

RC IXFileHandle::writePage(PageNum pageNum, void * data){
  return -1;
}

RC IXFileHandle::readPage(PageNum pageNum, void * data){
  return -1;
}

RC IXFileHandle::appendPage(void * data){
  return -1;
}
