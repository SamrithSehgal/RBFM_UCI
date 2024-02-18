#include <stdio.h>
#include <string.h>
#include<cmath>
#include<iostream>
#include "PagedFileManager.cpp"
#include "RecordBasedFileManager.h"
using namespace std;




RecordBasedFileManager::RecordBasedFileManager(/* args */)
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName){
   int sucsess = pfm->createFile(fileName);
   if(sucsess != 0){
    cout << "Something went wrong with creating a file";
    return -1;
   }
   pages = 0;
   return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName){
    int sucsess = pfm->destroyFile(fileName);
    if(sucsess != 0){
        return -1;
    }
    return 0;   
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle){
    int sucsess = pfm->openFile(fileName, fileHandle);
    if(sucsess != 0){
        return -1;
    }
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle){
    int sucsess = pfm->closeFile(fileHandle);
    if(sucsess != 0){
        return -1;
    }
    return 0;
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

    int numPages = fileHandle.getNumberOfPages();
    char* usableData = (char*) data;
    int length = 0;
    
    for(vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it){
        Attribute currentAttr = *it;

        if(currentAttr.type == TypeInt || currentAttr.type == TypeReal){
            length += 4;
            usableData += 4;
        }
        else if(currentAttr.type == TypeVarChar){
            int* varcharSize = (int*) usableData;
            usableData += 4; // cuz int
            length += 4;
            usableData += *varcharSize;
            length += *varcharSize;
        }
    }


    for(int i = 0; i < numPages; i++){
        
    }

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data, ostream &out) {
    return -1;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute, const CompOp compOp, const void *value,
                                const vector<string> &attributeNames,
                                RBFM_ScanIterator &rbfm_ScanIterator) {
    return -1;
}
