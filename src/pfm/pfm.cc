#include "src/include/pfm.h"
#include <stdio.h>
#include <string.h>
#include<cmath>
#include<iostream>
#include <unistd.h>
using namespace std;

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager pfm = PagedFileManager();
        return pfm;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const string &fileName) {
        int fileFound = access(fileName.c_str(), F_OK);
        if(fileFound == 0){
            cout << "Error this file already exists :(";
            return -1;
        }
        else{
            FILE *pfile = fopen(fileName.c_str(), "wb");
            int closeSucsessful = fclose(pfile);
            if(closeSucsessful != 0){
                cout << "Something went wrong with closing";
            }
            else{
                return 0;
            }
        }
    }

    RC PagedFileManager::destroyFile(const string &fileName) {
        int deleteSucsessful = remove(fileName.c_str());
        string cVals = "counterVals";
        remove(cVals.c_str());
        if(deleteSucsessful != 0){
            return -1;
            cout << "Error in deletion";
        }
        else{
            return 0;
        }
    }

    RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
        FILE *foundFile = fopen(fileName.c_str(), "rb+wb");
        fileHandle.ct.initCT();
        if(foundFile != NULL){
            
            fileHandle.fileName = fileName;
            fileHandle.filePointer = foundFile; 

        }
        else{
            cout << "Error in opening file";
            return -1;
        }
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        FILE* fp = fileHandle.filePointer;
        FILE* ctp = fileHandle.ct.trackerPointer;
        int sucsess = fclose(fp);
        fclose(ctp);
        if(sucsess != 0){
            cout << "Error in closing file";
            return -1;
        }
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if(pageNum >= getNumberOfPages()){
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
        ct.writeRCounter();
        //readPageCounter++;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if(pageNum > getNumberOfPages()){
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
        ct.writeWCounter();
        //writePageCounter++;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
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
        ct.writeACounter();
        //appendPageCounter++;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        if(filePointer == NULL){
            cout << "File opening error";
            return -1;
        }

        fseek(filePointer, 0, SEEK_END);
        unsigned long size = ftell(filePointer);
        return size/4096;  
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ct.getRCounter();
        writePageCount = ct.getWCounter();
        appendPageCount = ct.getACounter(); 
        /*readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter; */

        return 0;
    }


    RC CounterTracker::initCT(){
        unsigned rpc = 0;
        unsigned wpc = 0;
        unsigned apc = 0;
        int fileFound = access(fileName.c_str(), F_OK);
        if(fileFound == 0){
            FILE *pfile = fopen(fileName.c_str(), "rb+wb");
            trackerPointer = pfile;
        }
        else{
            FILE *pfile = fopen(fileName.c_str(), "wb");
            fclose(pfile);
            FILE *reFile = fopen(fileName.c_str(), "rb+wb");
            trackerPointer = reFile;
            char* data = (char*) malloc(PAGE_SIZE);
            memcpy(data, &rpc, sizeof(unsigned));
            memcpy(data+sizeof(unsigned), &wpc, sizeof(unsigned));
            memcpy(data+sizeof(unsigned) * 2, &apc, sizeof(unsigned));

            size_t sucsess = fwrite(data, 1, PAGE_SIZE, trackerPointer);
            if(sucsess != PAGE_SIZE){
                cout << "couldnt write" << endl;
                return -1;
            }

            fflush(trackerPointer);
            free(data);
        }
        return 0;
    }

    RC CounterTracker::getRCounter(){
        int findData = fseek(trackerPointer, 0, SEEK_SET);
        unsigned rpc;
        if(findData != 0){
            cout << "Cant find read counter";
            return -1;
        }
        fread(&rpc, sizeof(unsigned), 1, trackerPointer);
        return rpc;
    }

    RC CounterTracker::getWCounter(){
        int findData = fseek(trackerPointer, sizeof(unsigned), SEEK_SET);
        unsigned wpc;
        if(findData != 0){
            cout << "Cant find read counter";
            return -1;
        }
        fread(&wpc, sizeof(unsigned), 1, trackerPointer);
        return wpc;
    }

    RC CounterTracker::getACounter(){
        int findData = fseek(trackerPointer, sizeof(unsigned)*2, SEEK_SET);
        unsigned apc;
        if(findData != 0){
            cout << "Cant find read counter";
            return -1;
        }
        fread(&apc, sizeof(unsigned), 1, trackerPointer);
        if(ferror(trackerPointer)){
            cout << "error in reading";
            return -1;
        }
        return apc;
    }

    RC CounterTracker::writeRCounter(){
        unsigned rpc = getRCounter() + 1;
        int findData = fseek(trackerPointer, 0, SEEK_SET);
        int sucsess = fwrite(&rpc, sizeof(unsigned), 1, trackerPointer);
        if(sucsess == 0){
            cout << "error writing";
            return -1;
        }
        fflush(trackerPointer);
        return 0;
    }

    RC CounterTracker::writeWCounter(){
        unsigned wpc = getWCounter() + 1;
        int findData = fseek(trackerPointer, sizeof(unsigned), SEEK_SET);
        int sucsess = fwrite(&wpc, sizeof(unsigned), 1, trackerPointer);
        if(sucsess == 0){
            cout << "error writing";
            return -1;
        }
        fflush(trackerPointer);
        return 0;
    }

    RC CounterTracker::writeACounter(){
        unsigned apc = getACounter() + 1;
        int findData = fseek(trackerPointer, sizeof(unsigned)*2, SEEK_SET);
        size_t sucsess = fwrite(&apc, sizeof(unsigned), 1, trackerPointer);
        if(sucsess != 1){
            cout << "error writing";
            return -1;
        }
        if(ferror(trackerPointer)){
            cout << "error in writing";
            return -1;
        }
        fflush(trackerPointer);
        return 0;
    }


} // namespace PeterDB