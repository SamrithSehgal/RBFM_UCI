#include <string>
#include <unistd.h>
#include <cstring> 
#include <iostream>
#include "PagedFileManager.h"
using namespace std;



PagedFileManager::PagedFileManager(/* args */)
{
}

PagedFileManager::~PagedFileManager()
{
}




PagedFileManager* PagedFileManager::createPFM(){
    if(!pfm){
        pfm = new PagedFileManager();
    }
    return pfm;
}

RC PagedFileManager::createFile(const std::string &filename){
    int fileFound = access(filename.c_str(), F_OK);
    if(fileFound == 0){
        cout << "Error this file already exists :(";
        return -1;
    }
    else{
        FILE *pfile = fopen(filename.c_str(), "wb");
        int closeSucsessful = fclose(pfile);
        if(closeSucsessful != 0){
            cout << "Something went wrong with closing";
        }
        else{
            return 0;
        }
    }
}

RC PagedFileManager::destroyFile(const std::string &filename){
    int deleteSucsessful = remove(filename.c_str());
    if(deleteSucsessful != 0){
        return -1;
        cout << "Error in deletion";
    }
    else{
        return 0;
    }
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle){
    FILE *foundFile = fopen(fileName.c_str(), "rb+wb");
    if(foundFile != NULL){
        
        fileHandle.fileName = fileName;
        fileHandle.filePointer = foundFile; 

    }
    else{
        cout << "Error in opening file";
        return -1;
    }
} 

RC PagedFileManager::closeFile(FileHandle &fileHandle){}

//==========================================================================


FileHandle::FileHandle(/* args */)
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(unsigned pageNum, void *data){
    if(pageNum < appendPageCounter || pageNum > appendPageCounter){
        cout << "Page doesnt exist";
        return -1;
    }
    int foundData = fseek(filePointer, pageNum * 4096, SEEK_SET);
    if(foundData != 0){
        cout << "Error in finding page";
        return -1;
    }
    fread(data, 1, 4096, filePointer); 
    if(ferror(filePointer)){
        cout << "Error in reading file";
        return -1;
    }
    appendPageCounter++;
    return 0;
}

RC FileHandle::writePage(unsigned pageNum, void *data){
    if(pageNum < appendPageCounter || pageNum > appendPageCounter){
        cout << "Page doesnt exist";
        return -1;
    }

    int foundData = fseek(filePointer, pageNum * 4096, SEEK_SET); //moves to start then offsets current position by page number * # of bytes in page
    if(foundData != 0){
        cout << "Error in finding page";
        return -1;
    }

    fwrite(data, 1, 4096, filePointer); //writes 1 element of size 4096
    if(ferror(filePointer)){
        cout << "Error in writing to file file";
        return -1;
    }
    writePageCounter++;
}

RC FileHandle::appendPage(void *data){
    int foundData = fseek(filePointer, 0, SEEK_END); //moves to end
    if(foundData != 0){ //Is this even possible or am I a dumbass?!?!
        cout << "Error in finding end of file";
        return -1;
    }

    fwrite(data, 1, 4096, filePointer);
    if(ferror(filePointer)){
        cout << "Error in appending the page";
        return -1;
    }
    appendPageCounter++;
    return 0;
}

unsigned FileHandle::getNumberOfPages(){
    if(filePointer == NULL){
        cout << "File opening error";
        return -1;
    }

    fseek(filePointer, 0, SEEK_END);
    unsigned long size = ftell(filePointer);
    return size/4096;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount){
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;

    return 0;
}





