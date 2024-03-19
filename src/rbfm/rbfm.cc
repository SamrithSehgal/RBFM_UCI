#include "src/include/rbfm.h"
#include <stdio.h>
#include <string.h>
#include<cmath>
#include<iostream>

using namespace std;

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager &pfm = PagedFileManager::instance();
        int sucsess = pfm.createFile(fileName);
        if(sucsess != 0){
            cout << "Something went wrong with creating a file";
            return -1;
        }
        return 0;    
   }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager &pfm = PagedFileManager::instance();
        int sucsess = pfm.destroyFile(fileName);
        if(sucsess != 0){
            return -1;
        }
        return 0;   
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager &pfm = PagedFileManager::instance();
        int sucsess = pfm.openFile(fileName, fileHandle);
        if(sucsess != 0){
            return -1;
        }
        return 0;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager &pfm = PagedFileManager::instance();
        int sucsess = pfm.closeFile(fileHandle);
        if(sucsess != 0){
            return -1;
        }
        return 0;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
        int nulls = ceil((double) recordDescriptor.size()/8);
        int offset = nulls;
        int dir = nulls + 4;
        int fields = nulls + 4 + recordDescriptor.size() * 4;
        int totalSize = 0;

        for(int field = 0; field < recordDescriptor.size(); field++){
            totalSize += recordDescriptor[field].length;
            if(recordDescriptor[field].type == TypeVarChar){
                totalSize += 4;
            }
        }
        unsigned char* record = (unsigned char*) malloc(totalSize + fields);

        char* dataPointer = (char*) data;
        char* nullPointer = dataPointer;

        int numFields = recordDescriptor.size();

        memcpy(record, &numFields, 4);
        memcpy(record + 4, dataPointer, nulls);

        for(int dataF = 0; dataF < recordDescriptor.size(); dataF++){
            Attribute ap = recordDescriptor[dataF];
            if (!(nullPointer[dataF / 8] & (1 << (7 - dataF % 8)))){
                switch(ap.type){
                    case TypeVarChar:
                    {
                        int varCharSize;
                        memcpy(&varCharSize, &dataPointer[offset], 4);
                        offset += 4;
                        memcpy(record + fields, &dataPointer[offset], varCharSize);
                        fields += varCharSize;
                        offset += varCharSize;
                        memcpy(record + dir, &fields, 4);
                        dir += 4;
                        break;
                    }
                    case TypeInt:
                    case TypeReal:
                        memcpy(record + fields, &dataPointer[offset], 4);
                        offset += 4;
                        fields += 4;
                        memcpy(record + dir, &fields, 4);
                        dir += 4;
                        break;
                }
            }
            else{ 
                memcpy(record + dir, &fields, 4);
                dir += 4;
            }
        }

        int pages = fileHandle.getNumberOfPages()-1;
        int freeSpace;
        int takenSlots;
        void* alloPage = malloc(PAGE_SIZE);
        char* curPage = (char*) alloPage;
        int times = 0;
        if(pages < 0){
            pageInit(fileHandle, curPage, record, fields);
            pages++;
            rid.slotNum = 1;
        }
        else{
            while(pages < fileHandle.getNumberOfPages()){
                fileHandle.readPage(pages, alloPage);
                memcpy(&freeSpace, &curPage[PAGE_SIZE-4], 4);
                memcpy(&takenSlots, &curPage[PAGE_SIZE-8], 4);
                int dirLocation = PAGE_SIZE - takenSlots * 8 - 8;
                int deletedSlot = -1;
                for(int i = 1; i <= takenSlots; i++){
                    int curOffset;
                    memcpy(&curOffset, &curPage[PAGE_SIZE-8-i*8], 4);
                    if(curOffset == -1){
                        deletedSlot = i;
                        break;
                    }
                }
                if((dirLocation - freeSpace >= fields + 8) || (dirLocation - freeSpace >= fields && deletedSlot != -1)){
                    if(deletedSlot == -1){
                        takenSlots++;
                        memcpy(curPage + freeSpace, record, fields);
                        memcpy(curPage+dirLocation - 8, &freeSpace, 4);
                        memcpy(curPage + dirLocation - 4, &fields, 4);
                        freeSpace += fields;
                        memcpy(curPage + PAGE_SIZE - 8, &takenSlots, 4);
                        memcpy(curPage + PAGE_SIZE - 4, &freeSpace, 4);
                        fileHandle.writePage(pages, curPage);
                        rid.slotNum = takenSlots;
                        break;
                    }
                    else{
                        if(deletedSlot == 1){
                            memmove(curPage+fields, curPage, freeSpace);
                            memcpy(curPage, record, fields);

                            int offset = 0;
                            memcpy(curPage+PAGE_SIZE-16, &offset, 4); //PAGE_SIZE-16 because its -8 for data and then -8 because its the first slot
                            memcpy(curPage+PAGE_SIZE-12, &fields, 4); //PAGE_SIZE-12 because -8 for data and then -4 to get to length

                            freeSpace += fields;
                            memcpy(curPage+PAGE_SIZE-4, &freeSpace, 4);
                            rid.slotNum = 1;
                        }
                        else{
                            int prevOffset;
                            int prevLength;
                            memcpy(&prevOffset, &curPage[PAGE_SIZE-8-(deletedSlot-1)*8], 4);
                            memcpy(&prevLength, &curPage[PAGE_SIZE-4-(deletedSlot-1)*8], 4);

                            int offset = prevOffset + prevLength;
                            memmove(curPage+offset+fields, curPage+offset, freeSpace-prevOffset-prevLength);
                            memcpy(curPage+offset, record, fields);

                            memcpy(curPage+PAGE_SIZE-8-deletedSlot*8, &offset, 4);
                            memcpy(curPage+PAGE_SIZE-4-deletedSlot*8, &fields, 4);
                            rid.slotNum = deletedSlot;
                        }
                        fileHandle.writePage(pages, curPage);
                        break;
                    }                
                }
                else{
                    if((pages+1) == fileHandle.getNumberOfPages() && times == 0){
                        pages = 0;
                        times++;
                    }
                    else if((pages+1) == fileHandle.getNumberOfPages() && times == 1){
                        pageInit(fileHandle, curPage, record, fields);
                        pages++;
                        rid.slotNum = 1;
                        break;
                    }
                    else{
                        pages++;
                    }
                }
            }
        }

        rid.pageNum = pages;
        free(record);
        free(alloPage);
        return 0;

    }

    RC RecordBasedFileManager::pageInit(FileHandle &fileHandle, char* page, unsigned char* record, int fields){
        int numSlots;
        int freeSpace;

        memcpy(page, record, fields);
        numSlots = 1;
        int pd = PAGE_SIZE - 16;
        int start = 0;
        memcpy(page + pd, &start, 4);
        memcpy(page + pd + 4, &fields, 4);
        freeSpace = fields;
        memcpy(page+PAGE_SIZE - 8, &numSlots, 4);
        memcpy(page+PAGE_SIZE - 4, &freeSpace, 4);
        fileHandle.appendPage(page);
    

    }

    int updateSlots(int slotNum, int numSlots, char* curPage, int slotLen){
        for(int i = (slotNum+1); i <= numSlots; i++){
            int curOffset;
            memcpy(&curOffset, &curPage[PAGE_SIZE-8-i*8], 4);
            //cout << "Offset before: " << curOffset << endl;
            curOffset -= slotLen;
            //cout << "Offset after: " << curOffset << endl;
            memcpy(curPage+PAGE_SIZE-8-i*8, &curOffset, 4);
        }
    }

    int isTombstone(const RID &rid, RID &newRid, char* curPage){
        int isTombstone;
        int slotOffset;
        //cout << "Checking: " << endl;
        memcpy(&slotOffset, &curPage[PAGE_SIZE-8-rid.slotNum*8], 4);
        memcpy(&isTombstone, &curPage[slotOffset], 4);
        if(isTombstone == -1){
            //cout << isTombstone << endl;
            memcpy(&newRid.pageNum, &curPage[slotOffset+4], 4);
            memcpy(&newRid.slotNum, &curPage[slotOffset+8], 4);
            return 0;
        }
        else{
            return -1;
        }
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

        void* alloPage = malloc(PAGE_SIZE);
        char* page = (char*) alloPage;
        fileHandle.readPage(rid.pageNum, page);

        int slotOffset = 0;
        memcpy(&slotOffset,page+PAGE_SIZE - 8 - (8 * rid.slotNum), 4);
        char* record = page + slotOffset;

        int nulls = ceil((double) recordDescriptor.size() / 8);
        memcpy(data, record + 4, nulls);
        char* nullLoc = record + 4;
        int offset = nulls;
        int fields  =  nulls + 4 * recordDescriptor.size() + 4;
        int dir = nulls + 4;
        char* dataPointer = (char*)data;

        int old = 0;
        int cur = 0; 
        int size = 0; 
        int slotStatus;
        memcpy(&slotStatus, &page[PAGE_SIZE-8-rid.slotNum*8], 4);
        if (slotStatus == -1) {
            free(alloPage);
            return -1; 
        }

        RID newRid;
        cout << "Page Num: " << rid.pageNum << " Slot Num: " << rid.slotNum << endl;
        if(isTombstone(rid, newRid, page) == 0){
            int sucsess = readRecord(fileHandle, recordDescriptor, newRid, data);
            return sucsess;
        }

        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (!(nullLoc[i / 8] & (1 << (7 - i % 8)))) {
                switch(recordDescriptor[i].type) {
                    case TypeReal: case TypeInt:
                    memcpy(&dataPointer[offset], record + fields, 4);
                    offset += 4;
                    fields += 4;
                    break;
                case TypeVarChar:
                    if(i == 0) old = fields;
                    else memcpy(&old,record + dir + (i-1) * 4, 4);
                    memcpy(&cur,record + dir + i * 4, 4);
                    size = cur - old; 
                    memcpy(&dataPointer[offset],&size, 4);
                    memcpy(&dataPointer[offset + 4],record + fields,size);
                    fields += size;
                    offset += size + 4;
                    break;
                }
            }
        }

        free(page);
        return 0;


    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid) {
        void* alloPage = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, alloPage);
        char* curPage = (char*) alloPage;

        int numSlots;
        int usedSpace;
        memcpy(&numSlots, &curPage[PAGE_SIZE-8], 4);
        memcpy(&usedSpace, &curPage[PAGE_SIZE-4], 4);

        int slotOffset;
        int slotLen;
        memcpy(&slotOffset, &curPage[PAGE_SIZE-8 - rid.slotNum * 8], 4);
        memcpy(&slotLen, &curPage[PAGE_SIZE-4 - rid.slotNum * 8], 4);

        //cout << "Number of slots: " << numSlots << " Used Space: " << usedSpace << " Slot Offset: " << slotOffset << " Slot Length: " << slotLen << " Slot Number: " << rid.slotNum << endl;  
        RID newRid;
        if(isTombstone(rid, newRid, curPage) == 0){
            int succsess = deleteRecord(fileHandle, recordDescriptor, newRid);
            return succsess;
        }

        memmove(alloPage+slotOffset, alloPage+slotOffset+slotLen, usedSpace-(slotOffset+slotLen));
        int removedOffset = -1;
        int removedLen = -1;
        memcpy(curPage+PAGE_SIZE-8-rid.slotNum*8, &removedOffset, 4);
        memcpy(curPage+PAGE_SIZE-4-rid.slotNum*8, &removedLen, 4);

        updateSlots(rid.slotNum, numSlots, curPage, slotLen);
        usedSpace -= slotLen;
        memcpy(curPage+PAGE_SIZE-4, &usedSpace, 4);
        fileHandle.writePage(rid.pageNum, alloPage);
        free(alloPage);
        return 0;
    }


    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        int offset = ceil((double) recordDescriptor.size()/8);
        const char* pointer = (char*)data;
        string recordString;

        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (!(pointer[i / 8] & (1 << (7 - i % 8)))) {

                switch(recordDescriptor[i].type) {
                case TypeVarChar: {
                    int size;
                    memcpy(&size, &pointer[offset], 4);
                    char* content = new char[size + 1];
                    memcpy(content, &pointer[offset + 4], size);
                    content[size] = '\0';
                    recordString += recordDescriptor[i].name.c_str();
                    recordString += ": ";
                    recordString += content;
                    recordString += ", ";
                    offset += size + 4;
                    break;
                }
                case TypeInt: {
                    int intNumber;
                    memcpy(&intNumber, &pointer[offset], 4);
                    recordString += recordDescriptor[i].name.c_str();
                    recordString += ": ";
                    recordString += to_string(intNumber);
                    recordString += ", ";
                    offset += 4;
                    break;
                }
                case TypeReal: {
                    float realNumber;
                    memcpy(&realNumber, &pointer[offset], 4);
                    recordString += recordDescriptor[i].name.c_str();
                    recordString += ": ";
                    recordString += to_string(realNumber);
                    recordString += ", ";
                    offset += 4;
                    break;
                }
                default: break;
                }
            }
            else {
                    recordString += recordDescriptor[i].name.c_str();
                    recordString += ": ";
                    recordString += "NULL";
                    recordString += ", ";
            }
        }
            out << recordString;
            return 0;
    }

    int getSize(const vector<Attribute> &recordDescriptor, const void* data){
        int nulls = ceil((double) recordDescriptor.size()/8);
        int offset = nulls;
        int fields = nulls + 4 + recordDescriptor.size() * 4;

        char* dataPointer = (char*) data;
        char* nullPointer = dataPointer;

        for(int i = 0; i < recordDescriptor.size(); i++){
            const Attribute attr = recordDescriptor[i];
            if(!(nullPointer[i/8] & (1 << (7 - i % 8)))){
                switch(attr.type){
                    case TypeVarChar:
                    {
                        int varCharSize;
                        memcpy(&varCharSize, &dataPointer[offset], 4);
                        offset += 4;
                        fields += varCharSize;
                        offset += varCharSize;
                        break;
                    }
                    case TypeInt:
                    case TypeReal:
                        offset += 4;
                        fields += 4;
                        break;
                }
            }                
        }
        return fields;
    }

    unsigned char* alterRecord(const vector<Attribute> &recordDescriptor, const void* data, const RID &rid){
        int nulls = ceil((double) recordDescriptor.size()/8);
        int offset = nulls;
        int dir = nulls + 4;
        int fields = nulls + 4 + recordDescriptor.size() * 4;
        int totalSize = 0;

        for(int field = 0; field < recordDescriptor.size(); field++){
            totalSize += recordDescriptor[field].length;
            if(recordDescriptor[field].type == TypeVarChar){
                totalSize += 4;
            }
        }
        unsigned char* record = (unsigned char*) malloc(totalSize + fields);

        char* dataPointer = (char*) data;
        char* nullPointer = dataPointer;

        int numFields = recordDescriptor.size();

        memcpy(record, &numFields, 4);
        memcpy(record + 4, dataPointer, nulls);

        for(int dataF = 0; dataF < recordDescriptor.size(); dataF++){
            Attribute ap = recordDescriptor[dataF];
            if (!(nullPointer[dataF / 8] & (1 << (7 - dataF % 8)))){
                switch(ap.type){
                    case TypeVarChar:
                    {
                        int varCharSize;
                        memcpy(&varCharSize, &dataPointer[offset], 4);
                        offset += 4;
                        memcpy(record + fields, &dataPointer[offset], varCharSize);
                        fields += varCharSize;
                        offset += varCharSize;
                        memcpy(record + dir, &fields, 4);
                        dir += 4;
                        break;
                    }
                    case TypeInt:
                    case TypeReal:
                        memcpy(record + fields, &dataPointer[offset], 4);
                        offset += 4;
                        fields += 4;
                        memcpy(record + dir, &fields, 4);
                        dir += 4;
                        break;
                }
            }
            else{ 
                memcpy(record + dir, &fields, 4);
                dir += 4;
            }
        }
        return record;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
        void* alloPage = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, alloPage);
        char* curPage = (char*) alloPage;

        int dataSize = getSize(recordDescriptor, data);
        
        int usedSpace;
        int numSlots;
        memcpy(&numSlots, &curPage[PAGE_SIZE-8], 4);
        memcpy(&usedSpace, &curPage[PAGE_SIZE-4], 4);

        int slotOffset; 
        int slotLen;
        memcpy(&slotOffset, &curPage[PAGE_SIZE-8-rid.slotNum*8], 4);
        memcpy(&slotLen, &curPage[PAGE_SIZE-4-rid.slotNum*8], 4); 

        int dirLoc = PAGE_SIZE - 8 - numSlots*8;

        RID newRid;
        if(isTombstone(rid, newRid, curPage) == 0){
            int succsess = updateRecord(fileHandle, recordDescriptor, data, newRid);
            return succsess;
        }

        if(dirLoc - usedSpace >= dataSize){ 
            //cout << "First slot: " << endl; 
            cout << dataSize << endl;
            //cout << slotLen << endl;
            //cout << (dataSize-slotLen) << endl;
            cout << "Inserting on same page" << endl;
            memmove(curPage+slotOffset+dataSize, curPage+slotOffset+slotLen, usedSpace-slotOffset-slotLen);
            unsigned char* newRecord = alterRecord(recordDescriptor, data, rid);
            memcpy(curPage+slotOffset, newRecord, dataSize);
            if(dataSize-slotLen != 0){
                updateSlots(rid.slotNum, numSlots, curPage, -(dataSize-slotLen));
            }
            memcpy(curPage+PAGE_SIZE-4-rid.slotNum*8, &dataSize, 4);
            fileHandle.writePage(rid.pageNum, curPage);
            free(alloPage);
            return 0;   
        }
        else{
            //cout << "Not first slot: " << endl;
            //cout << dataSize << endl;
            //cout << slotLen << endl;
            //cout << (dataSize-slotLen) << endl;
            cout << "Inserting on new page" << endl;
            RID newRid;
            insertRecord(fileHandle, recordDescriptor, data, newRid);
            int moved = -1;
            memmove(curPage+slotOffset+12, curPage+slotOffset+slotLen, usedSpace - slotOffset - slotLen);
            memcpy(curPage+slotOffset, &moved, 4);
            memcpy(curPage+slotOffset+4, &newRid.pageNum, 4);
            memcpy(curPage+slotOffset+8, &newRid.slotNum, 4);

            updateSlots(rid.slotNum, numSlots, curPage, -(slotLen-12));
            int size = 12;
            memcpy(curPage+PAGE_SIZE-4-rid.slotNum*8, &size, 4);
            fileHandle.writePage(rid.pageNum, curPage);
            free(alloPage);
            return 0;
        }
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, const std::string &attributeName, void *data) {
        void* alloPage = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, alloPage);
        char* curPage = (char*) curPage;

        int slotOffset;
        int slotLen;
        memcpy(&slotOffset, &curPage[PAGE_SIZE-8-rid.slotNum*8], 4);
        memcpy(&slotLen, &curPage[PAGE_SIZE-4-rid.slotNum*8], 4);

        char* record = (char*) malloc(slotLen);
        memcpy(record, curPage+slotOffset, slotLen);
        int nulls = ceil((double) recordDescriptor.size()/8);
        int offset = nulls;
        int fields = nulls + 4 + recordDescriptor.size() * 4;

        char* dataPointer = (char*) data;
        char* nullPointer = dataPointer;
        int old = 0;
        int cur = 0; 
        int size = 0; 
        int dir = nulls + 4;
        for(int i = 0; i < recordDescriptor.size(); i++){
            const Attribute attr = recordDescriptor[i];
            if(attr.name == attributeName){
                if(!(nullPointer[i/8] & (1 << (7 - i % 8)))){
                    switch(attr.type){
                        case TypeReal: case TypeInt:
                            memcpy(&dataPointer[offset], record + fields, 4);
                            offset += 4;
                            fields += 4;
                            break;
                        case TypeVarChar:
                            if(i == 0) old = fields;
                            else memcpy(&old,record + dir + (i-1) * 4, 4);
                            memcpy(&cur,record + dir + i * 4, 4);
                            size = cur - old; 
                            memcpy(&dataPointer[offset],&size, 4);
                            memcpy(&dataPointer[offset + 4],record + fields,size);
                            fields += size;
                            offset += size + 4;
                            break;
                    }
                }  
            }   
            else{
                if(!(nullPointer[i/8] & (1 << (7 - i % 8)))){
                switch(attr.type){
                    case TypeVarChar:
                    {
                        int varCharSize;
                        memcpy(&varCharSize, &dataPointer[offset], 4);
                        offset += 4;
                        fields += varCharSize;
                        offset += varCharSize;
                        break;
                    }
                    case TypeInt:
                    case TypeReal:
                        offset += 4;
                        fields += 4;
                        break;
                }
            }                
            }           
        }
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }
    

} // namespace PeterDB
