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
            freeSpace = fields;
            takenSlots = 1;
            pages++;
        }

        RID deletedSlotRID;
    if (findDeletedSlot(fileHandle, deletedSlotRID) == 0) {
        rid.pageNum = deletedSlotRID.pageNum;
        rid.slotNum = deletedSlotRID.slotNum;
        updateSlotStatus(fileHandle, rid, 0); 
    } 
        else{

            while(pages < fileHandle.getNumberOfPages()){
                fileHandle.readPage(pages, alloPage);
                memcpy(&freeSpace, &curPage[PAGE_SIZE-4], 4);
                memcpy(&takenSlots, &curPage[PAGE_SIZE-8], 4);
                int dirLocation = PAGE_SIZE - takenSlots * 8 - 8;
                if(dirLocation - freeSpace >= fields + 8){
                    takenSlots++;
                    memcpy(curPage + freeSpace, record, fields);
                    memcpy(curPage+dirLocation - 8, &freeSpace, 4);
                    memcpy(curPage + dirLocation - 4, &fields, 4);
                    memcpy(curPage + PAGE_SIZE - 8, &takenSlots, 4);
                    freeSpace += fields;
                    memcpy(curPage + PAGE_SIZE - 4, &freeSpace, 4);
                    fileHandle.writePage(pages, curPage);
                    break;
                }
                else{
                    if((pages+1) == fileHandle.getNumberOfPages() && times == 0){
                        pages = 0;
                        times++;
                    }
                    else if((pages+1) == fileHandle.getNumberOfPages() && times == 1){
                        pageInit(fileHandle, curPage, record, fields);
                        freeSpace = fields;
                        takenSlots = 1;
                        pages++;
                        break;
                    }
                    else{
                        pages++;
                    }
                }

            }
        }

        rid.slotNum = takenSlots;
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

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	void* alloPage = malloc(PAGE_SIZE);
    char* page = (char*) alloPage;
	fileHandle.readPage(rid.pageNum, page);

	int slotDir = 0;
	memcpy(&slotDir,page+PAGE_SIZE - 8 - (8 * rid.slotNum), 4);
	char* record = page + slotDir;

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
        int slotDirOffset = PAGE_SIZE - 8 - (rid.slotNum + 1) * 8;
        int slotStatus;
        memcpy(&slotStatus, page + slotDirOffset + 2 * sizeof(int), sizeof(int));

        if (slotStatus == -1) {
            free(alloPage);
            return -1; 
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

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    void* pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, pageData);
    char* page = (char*) (pageData);

    int slotDirOffset = PAGE_SIZE - 8 - rid.slotNum * 8;
    int slotOffset, slotLength;
    int recordOffset = PAGE_SIZE - 8 - (rid.slotNum + 1) * 8;
    int recordStatus = -1; 
    memcpy(&slotOffset, page + slotDirOffset, 4);
    memcpy(&slotLength, page + slotDirOffset + 4, 4);
    memcpy(page + recordOffset + 2 * sizeof(int), &recordStatus, sizeof(int));

    compactPage(page, slotOffset, slotLength);

    fileHandle.writePage(rid.pageNum, page);

    free(pageData);
    return 0;
}

void RecordBasedFileManager::compactPage(char* page, int slotOffset, int slotLength) {
    int numSlots, freeSpace;
    memcpy(&numSlots, page + PAGE_SIZE - 8, 4);
    memcpy(&freeSpace, page + PAGE_SIZE - 4, 4);

    char* endOfRecords = page + freeSpace;
    int slotTableStart = PAGE_SIZE - 8 - numSlots * 8;
    char* slotTableEnd = page + PAGE_SIZE - 8;

    char* source = endOfRecords - slotOffset - slotLength;
    char* destination = endOfRecords;
    int shiftAmount = slotLength;

    memmove(destination, source, shiftAmount);

    int slotDirOffset = slotTableStart - (slotOffset + slotLength);
    int newSlotOffset = slotOffset + shiftAmount;
    int newSlotLength = slotLength;
    memcpy(page + slotDirOffset, &newSlotOffset, 4);
    memcpy(page + slotDirOffset + 4, &newSlotLength, 4);

    freeSpace += shiftAmount;
    memcpy(page + PAGE_SIZE - 4, &freeSpace, 4);
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

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }
    

} // namespace PeterDB
