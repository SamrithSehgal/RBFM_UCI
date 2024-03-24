#include "src/include/rm.h"
#include <string.h>
#include <iostream>
#include <stdio.h>
#include<cmath>
#include<iostream>
#include <unistd.h>
#include <fstream>     

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    int fillTableCatalog(int tableId, string tableName, string fileName, char* curPage){

        int offset = 0;
        char nulls;
        memset(&nulls, 0, 1);

        memcpy(curPage, &nulls, 1);
        offset += 1;
        memcpy(curPage+offset, &tableId, 4);
        offset += 4;

        int tableLen = tableName.length();
        const char* tableString = tableName.c_str();
        memcpy(curPage+offset, &tableLen, 4);
        offset += 4;
        memcpy(curPage+offset, tableString, tableLen);
        offset += tableLen;

        int fileLen = fileName.length();
        const char* fileString = fileName.c_str();
        memcpy(curPage+offset, &fileLen, 4);
        offset += 4;
        memcpy(curPage+offset, fileString, fileLen);

        return 0;
    }

    int fillColumnCatalog(int tableId, string columnName, int columnType, int columnLength, int columnPosition, int deleted, char* curPage){
        
        int offset = 0;
        char nulls;
        memset(&nulls, 0, 1);
        memcpy(curPage, &nulls, 1);
        offset+=1;

        memcpy(curPage+offset, &tableId, 4);
        offset += 4;

        int nameLen = columnName.length();
        const char* nameString = columnName.c_str();
        memcpy(curPage+offset, &nameLen, 4);
        offset += 4;
        memcpy(curPage+offset, nameString, nameLen);
        offset += nameLen;

        memcpy(curPage+offset, &columnType, 4);
        offset += 4;
        memcpy(curPage+offset, &columnLength, 4);
        offset += 4;
        memcpy(curPage+offset, &columnPosition, 4);
        offset += 4;
        memcpy(curPage+offset, &deleted, 4);
        return 0;
    }

    //add isSystem
    vector<Attribute> createTableDescriptor(){
        vector<Attribute> tableDesc;
        Attribute attr;
        attr.length = 4;
        attr.name = "table-id";
        attr.type = TypeInt;
        tableDesc.push_back(attr);

        attr.length = 50;
        attr.name = "table-name";
        attr.type = TypeVarChar;
        tableDesc.push_back(attr);

        attr.name = "file-name";
        tableDesc.push_back(attr);
        

        return tableDesc;
    }

    vector<Attribute> createColumnDescriptor(){
        vector<Attribute> columnsDesc;
        Attribute attr;
        attr.length = 4;
        attr.name="table-id";
        attr.type = TypeInt;
        columnsDesc.push_back(attr);

        attr.length = 50;
        attr.name = "column-name";
        attr.type = TypeVarChar;
        columnsDesc.push_back(attr);

        attr.length = 4;
        attr.name = "column-type";
        attr.type = TypeInt;
        columnsDesc.push_back(attr);

        attr.name = "column-length";
        columnsDesc.push_back(attr);

        attr.name = "column-position";
        columnsDesc.push_back(attr);

        attr.name = "deleted";
        columnsDesc.push_back(attr);
        
        return columnsDesc;
    }

    void RelationManager::readCatalogs(){
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle tableHandle;
        rbfm.openFile("Tables", tableHandle);
        
        vector<Attribute> tableDesc = createTableDescriptor();
        string attrName = "table-id";
        vector<string> desiredAttrs;
        desiredAttrs.push_back(attrName);
        attrName = "table-name";
        desiredAttrs.push_back(attrName);
        attrName = "file-name";
        desiredAttrs.push_back(attrName);

        void* value = nullptr;
        RBFM_ScanIterator rbfmIter;
        rbfm.scan(tableHandle, tableDesc, string(""), NO_OP, value, desiredAttrs, rbfmIter);
        
        void* outBuffer = malloc(PAGE_SIZE);
        RID rid;
        while(rbfmIter.getNextRecord(rid, outBuffer) != -1){
            int tableId;
            int offset = 1;
            
            memcpy(&tableId, outBuffer+offset, 4);
            offset += 4;

            int nameLength;
            memcpy(&nameLength, outBuffer+offset, 4);
            offset += 4;
            cout << "Length: " << nameLength << endl;
            char* name = new char[nameLength+1];
            memcpy(name, outBuffer+offset, nameLength);
            name[nameLength] = '\0';
            offset += nameLength;
            cout << "Name: " << name << endl;

            cout << "Catalog Offset: " << offset << endl;
            int fileLength;
            memcpy(&fileLength, outBuffer+offset, 4);
            cout << "File Len: " << fileLength << endl;
            offset += 4;
            char* fileName = new char [fileLength+1];
            memcpy(fileName, outBuffer+offset, fileLength);
            fileName[fileLength] = '\0';
            
            //cout << "Record Info: {" << " Table Id: " << tableId << ", Name: " << name << ", File Name: " << fileName << " }" << endl;
        }
        free(outBuffer);
        rbfmIter.close();
        rbfm.closeFile(tableHandle);
        cout << endl << endl << "------------------------------ COLUMNS ------------------------------" << endl << endl;

        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);

        vector<Attribute> columnDesc = createColumnDescriptor();

        string columnAttr = "table-id";
        vector<string> columnDesired;
        columnDesired.push_back(columnAttr);
        columnAttr = "column-name";
        columnDesired.push_back(columnAttr);
        columnAttr = "column-type";
        columnDesired.push_back(columnAttr);
        columnAttr = "column-length";
        columnDesired.push_back(columnAttr);
        columnAttr = "column-position";
        columnDesired.push_back(columnAttr);

        void* val = nullptr;
        RBFM_ScanIterator columnIter;
        rbfm.scan(columnHandle, columnDesc, string(""), NO_OP, val, columnDesired, columnIter);

        void* outFile = malloc(PAGE_SIZE);
        int newTable;


        while(columnIter.getNextRecord(rid, outFile) != -1){
            int tableId;
            int offset = 1;

            memcpy(&tableId, outFile+offset, 4);
            offset += 4;
            
            if(newTable != tableId){
                newTable = tableId;
                cout << endl << endl;
            }

            int nameLength;
            memcpy(&nameLength, outFile+offset, 4);
            offset += 4;
            char* name = new char[nameLength+1];
            memcpy(name, outFile+offset, nameLength);
            name[nameLength] = '\0';
            offset += nameLength;

            int columnType;
            string typeString;
            memcpy(&columnType, outFile+offset, 4);
            offset += 4;
            if(columnType == 0){
                typeString = "TypeInt";
            }
            else if(columnType == 1){
                typeString = "TypeReal";
            }
            else{
                typeString = "TypeVarChar";
            }

            int columnLength;
            memcpy(&columnLength, outFile+offset, 4);
            offset += 4;

            int columnPosition;
            memcpy(&columnPosition, outFile+offset, 4);
            
            cout << "Record Info: {" << " Table Id: " << tableId << ", Name: " << name << ", Type: " << typeString << ", Length: " << columnLength << ", Position: " << columnPosition << " }" << endl;

        }
        free(outFile);
        columnIter.close();
        rbfmIter.close();
        rbfm.closeFile(columnHandle);
    }


    RC RelationManager::createCatalog() {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        rbfm.createFile("Tables");
        FileHandle tableHandle;
        rbfm.openFile("Tables", tableHandle);
        
        rbfm.createFile("Columns");
        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);

        RID rid;
        int tableId = 1;
        string tableName = "Tables";
        string fileName = "Tables";

        vector<Attribute> tableDesc = createTableDescriptor();
        vector<Attribute> columnsDesc = createColumnDescriptor();

        void* alloPage = malloc(PAGE_SIZE);

        fillTableCatalog(tableId, tableName, fileName, (char*) alloPage);
        rbfm.insertRecord(tableHandle, tableDesc, alloPage, rid);
        free(alloPage);
        tableId++;
        for(int i = 0; i < tableDesc.size(); i++){
            void* colPage = malloc(PAGE_SIZE);
            fillColumnCatalog(tableId-1, tableDesc[i].name, tableDesc[i].type, tableDesc[i].length, i+1, 0, (char*) colPage);
            rbfm.insertRecord(columnHandle, columnsDesc, colPage, rid);
            free(colPage);
        }

        void* colTablePage = malloc(PAGE_SIZE);

        tableName = "Columns";
        fileName = "Columns";
        fillTableCatalog(tableId, tableName, fileName, (char*) colTablePage);
        rbfm.insertRecord(tableHandle, tableDesc, colTablePage, rid);
        free(colTablePage);

        for(int i = 0; i < columnsDesc.size(); i++){
            void* colColPage = malloc(PAGE_SIZE);
            fillColumnCatalog(tableId, columnsDesc[i].name, columnsDesc[i].type, columnsDesc[i].length, i+1, 0, (char*) colColPage);
            rbfm.insertRecord(columnHandle, columnsDesc, colColPage, rid);
            free(colColPage);
        }
        rbfm.closeFile(tableHandle);
        rbfm.closeFile(columnHandle);
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        int sucsess = rbfm.destroyFile("Tables");
        if(sucsess == -1){return -1;}
        sucsess = rbfm.destroyFile("Columns");
        if(sucsess == -1){return -1;}
        return 0;

    }

    int findTableId(RecordBasedFileManager &rbfm, FileHandle &tableHandle, const string &newTableName){
        vector<Attribute> tableDesc = createTableDescriptor();

        RBFM_ScanIterator rbfmIter;
        string attrName = "table-id";
        vector<string> desiredAttrs;
        desiredAttrs.push_back(attrName);
        attrName = "table-name";
        desiredAttrs.push_back(attrName);
        void* value = nullptr;

        rbfm.scan(tableHandle, tableDesc, attrName, NO_OP, value, desiredAttrs, rbfmIter);

        RID findRid;
        void* outBuffer = malloc(PAGE_SIZE);
        int newTableId = 1;
        while(rbfmIter.getNextRecord(findRid, outBuffer) != -1){
            int tableId;
            memcpy(&tableId, outBuffer+1, 4);
            //cout << "Table Id: " << tableId << endl;
            if(tableId > newTableId){
                newTableId = tableId;
            }
            
            int length;
            memcpy(&length, outBuffer+5, 4);
            char* tableName = new char[length+1];
            memcpy(tableName, outBuffer+9, length);
            tableName[length] = '\0';
            if(tableName == newTableName){
                return -1;
            }
        }
        free(outBuffer);
        //cout << endl << endl;
        rbfmIter.close();
        return newTableId+1;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        if(access("Tables", F_OK) != 0){
            return -1;
        }

        rbfm.createFile(tableName);
        FileHandle tableHandle;
        rbfm.openFile("Tables", tableHandle);
        
        int tableId = findTableId(rbfm, tableHandle, tableName);
        if(tableId == -1){
            return -1;
        }

        void* alloPage = malloc(PAGE_SIZE);
        fillTableCatalog(tableId, tableName, tableName, (char*) alloPage);

        vector<Attribute> tableDesc = createTableDescriptor();
        RID rid;

        rbfm.insertRecord(tableHandle, tableDesc, alloPage, rid);
        free(alloPage);
        rbfm.closeFile(tableHandle);

        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);

        vector<Attribute> columnsDesc = createColumnDescriptor();
        for(int i = 0; i < attrs.size(); i++){
            void* colPage = malloc(PAGE_SIZE);
            fillColumnCatalog(tableId, attrs[i].name, attrs[i].type, attrs[i].length, i+1, 0, (char*) colPage);
            rbfm.insertRecord(columnHandle, columnsDesc, colPage, rid);
            free(colPage);
        }
        rbfm.closeFile(columnHandle);
        return 0;
    }

    int deleteFromTableCatalog(RecordBasedFileManager &rbfm, const string &tableName){
        FileHandle tableHandle;
        rbfm.openFile("Tables", tableHandle);
        vector<Attribute> tableDesc = createTableDescriptor();
        
        RBFM_ScanIterator rbfmIter;
        int nameLength = tableName.length();
        void* value = malloc(PAGE_SIZE);
        memcpy((char*)value, &nameLength, 4);
        memcpy((char*)value+4, tableName.c_str(), nameLength);

        string desiredAttr = "table-id";
        vector<string> attrList;
        attrList.push_back(desiredAttr);

        rbfm.scan(tableHandle, tableDesc, string("table-name"), EQ_OP, value, attrList, rbfmIter);

        RID rid;
        void* outBuffer = malloc(PAGE_SIZE);
        int tableId;
        if(rbfmIter.getNextRecord(rid, outBuffer) != -1){
            memcpy(&tableId, outBuffer+1, 4);
            //cout << "Table Id: " << tableId << endl;
            rbfm.deleteRecord(tableHandle, tableDesc, rid);
        }
        free(value);
        free(outBuffer);
        rbfmIter.close();
        rbfm.closeFile(tableHandle);
        return tableId;
    }

    int deleteFromColumnCatalog(RecordBasedFileManager &rbfm, const int tableId){
        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);

        vector<Attribute> columnDesc = createColumnDescriptor();
        vector<string> allAttrs;
        allAttrs.push_back("column-name");
        void* inBuffer = malloc(PAGE_SIZE);
        memcpy(inBuffer, &tableId, 4);
        RBFM_ScanIterator rbfmIter;

        rbfm.scan(columnHandle, columnDesc, string("table-id"), EQ_OP, inBuffer, allAttrs, rbfmIter);

        void* outBuffer = malloc(PAGE_SIZE);
        vector<RID> allRids;
        RID curRid;

        while(rbfmIter.getNextRecord(curRid, outBuffer) != -1){
            //cout << "Page: " << curRid.pageNum << " Slot: " << curRid.slotNum << endl;
            allRids.push_back(curRid);
        } 

        for(int i = 0; i < allRids.size(); i++){
            rbfm.deleteRecord(columnHandle, columnDesc, allRids[i]);
        }

        free(inBuffer);
        free(outBuffer);
        rbfmIter.close();
        rbfm.closeFile(columnHandle);

    }

    int getIdFromName(RecordBasedFileManager &rbfm, const string &tableName){
        FileHandle tableHandle;
        rbfm.openFile("Tables", tableHandle);
        vector<Attribute> tableDesc = createTableDescriptor();
        
        RBFM_ScanIterator rbfmIter;
        int nameLength = tableName.length();
        void* value = malloc(PAGE_SIZE);
        memcpy((char*)value, &nameLength, 4);
        memcpy((char*)value+4, tableName.c_str(), nameLength);

        string desiredAttr = "table-id";
        vector<string> attrList;
        attrList.push_back(desiredAttr);

        rbfm.scan(tableHandle, tableDesc, string("table-name"), EQ_OP, value, attrList, rbfmIter);

        RID rid;
        void* outBuffer = malloc(PAGE_SIZE);
        int tableId;
        if(rbfmIter.getNextRecord(rid, outBuffer) != -1){
            memcpy(&tableId, outBuffer+1, 4);
        }
        rbfmIter.close();
        rbfm.closeFile(tableHandle);
        free(value);
        free(outBuffer);
        return tableId;
    }

    int verifyTable(const string& tableName, RecordBasedFileManager &rbfm){
        int tableId = getIdFromName(rbfm, tableName);
        if(tableId == -1 || tableId == 1 || tableId == 2){
            return -1;
        }
        else{
            return 0;
        }
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        if(verifyTable(tableName, rbfm) == -1){
            return -1;
        }

        int tableId = deleteFromTableCatalog(rbfm, tableName);

        deleteFromColumnCatalog(rbfm, tableId);
        
        int succsess = rbfm.destroyFile(tableName);
        if(succsess != 0){
            return succsess;
        }

        return 0;


    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        int tableId = getIdFromName(rbfm, tableName);

        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);

        vector<Attribute> columnDesc = createColumnDescriptor();

        vector<string> columnDesired;
        string columnAttr = "column-name";
        columnDesired.push_back(columnAttr);
        columnAttr = "column-type";
        columnDesired.push_back(columnAttr);
        columnAttr = "column-length";
        columnDesired.push_back(columnAttr);
        columnAttr = "deleted";
        columnDesired.push_back(columnAttr);

        void* inBuffer = malloc(PAGE_SIZE);
        memcpy(inBuffer, &tableId, 4);

        RBFM_ScanIterator rbfmIter;
        rbfm.scan(columnHandle, columnDesc, string("table-id"), EQ_OP, inBuffer, columnDesired, rbfmIter);

        
        void* outBuffer = malloc(PAGE_SIZE);
        RID rid;

        while(rbfmIter.getNextRecord(rid, outBuffer) != -1){
            int offset = 1;

            int nameLength;
            memcpy(&nameLength, outBuffer+offset, 4);
            offset += 4;
            //cout << "Length: " << nameLength << endl;
            char* name = new char[nameLength+1];
            memcpy(name, outBuffer+offset, nameLength);
            name[nameLength] = '\0';
            //cout << "Name: " << name << endl;
            offset += nameLength;

            int columnType;
            memcpy(&columnType, outBuffer+offset, 4);
            offset += 4;
            //cout << "Type: " << columnType << endl;

            int columnLength;
            memcpy(&columnLength, outBuffer+offset, 4);
            offset += 4;
            //cout << "Length: " << columnLength << endl;
            //cout << "-----------------" << endl;
            
            int deleted;
            memcpy(&deleted, outBuffer+offset, 4);

            if(deleted == 0){
                Attribute attr;
                attr.name = name;
                attr.type = AttrType(columnType);
                attr.length = columnLength;
                attrs.push_back(attr);
            }


        }
        cout << tableName << ": ";
        for(int i = 0; i < attrs.size(); i++){
            cout << "{ " << "Name: " << attrs[i].name << ", Type: " << attrs[i].type << ", Length: " << attrs[i].length << " }" << endl;

        }
        cout << endl << "---------------------------------------------" << endl;
        rbfmIter.close();
        rbfm.closeFile(columnHandle);
        free(outBuffer);
        free(inBuffer);
        return 0;
    }

    int handleDeletedAttr(vector<Attribute> &recordDesc, int position, int attrType){
        int numFields = recordDesc.size();
        Attribute oldAttr = recordDesc[position-1];
        Attribute curAttr;
        Attribute deletedAttr;
        for(int i = position-1; i <= numFields; i++){
            if(i == position-1){
                deletedAttr.length = 0;
                deletedAttr.name = "DELETED";
                deletedAttr.type = AttrType(attrType);
                recordDesc[i] = deletedAttr;
            }
            else if(i < numFields){
                curAttr = recordDesc[i];
                recordDesc[i] = oldAttr;
                oldAttr = curAttr;
            }
            else{
                recordDesc.push_back(oldAttr);
            }

        }
    }

    //If have to update position when attr is deleted then just add deleted attr to column table, redact values from getAttributes based on that and then just search for table id that has deleted attr as true
    int findDeletedAttrs(const string &tableName, vector<Attribute> &recordDesc){
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);

        string resAttr = "column-position";
        vector<string> desiredAttrs;
        desiredAttrs.push_back(resAttr);
        resAttr = "deleted";
        desiredAttrs.push_back(resAttr);
        resAttr = "column-type";
        desiredAttrs.push_back(resAttr);

        vector<Attribute> columnDesc = createColumnDescriptor();

        int tableId = getIdFromName(rbfm, tableName);
        void* value = malloc(PAGE_SIZE);
        memcpy((char*)value, &tableId, 4);
        RBFM_ScanIterator rbfmIter;

        rbfm.scan(columnHandle, columnDesc, string("table-id"), EQ_OP, value, desiredAttrs, rbfmIter);

        RID rid;
        void* outBuffer = malloc(PAGE_SIZE);
        while(rbfmIter.getNextRecord(rid, outBuffer) != -1){
            int curPosition;
            memcpy(&curPosition, outBuffer+1, 4);
            int offset = 5;

            int deleted;
            memcpy(&deleted, outBuffer+offset, 4);
            offset += 4;

            int columnType;
            memcpy(&columnType, outBuffer+offset, 4);

            if(deleted == 1){
                handleDeletedAttr(recordDesc, curPosition, columnType);
            }
        }
        free(value);
        free(outBuffer);
        rbfmIter.close();
        rbfm.closeFile(columnHandle);

    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        
        if(verifyTable(tableName, rbfm) == -1){
            return -1;
        }

        FileHandle insertHandle;
        int succsess = rbfm.openFile(tableName, insertHandle);
        if(succsess == -1){
            return -1;
        }
        vector<Attribute> recordDesc;
        getAttributes(tableName, recordDesc);

        //findDeletedAttrs(tableName, recordDesc);
        
        succsess = rbfm.insertRecord(insertHandle, recordDesc, data, rid);
        if(succsess == -1){
            return -1;
        }
        rbfm.closeFile(insertHandle);
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        if(verifyTable(tableName, rbfm) == -1){
            return -1;
        }

        FileHandle deleteHandle;
        int succsess = rbfm.openFile(tableName, deleteHandle);
        if(succsess == -1){
            return -1;
        }
        
        vector<Attribute> recordDesc;
        getAttributes(tableName, recordDesc);
        
        succsess = rbfm.deleteRecord(deleteHandle, recordDesc, rid);
        if(succsess == -1){
            return -1;
        }
        rbfm.closeFile(deleteHandle);

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        if(verifyTable(tableName, rbfm) == -1){
            return -1;
        }

        FileHandle updateHandle;
        int succsess = rbfm.openFile(tableName, updateHandle);
        if(succsess == -1){
            return -1;
        }

        vector<Attribute> recordDesc;
        getAttributes(tableName, recordDesc);

        succsess = rbfm.updateRecord(updateHandle, recordDesc, data, rid);
        if(succsess == -1){
            return -1;
        }
        rbfm.closeFile(updateHandle);
        
        return 0;
        
    }


    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();



        FileHandle readHandle;
        int succsess = rbfm.openFile(tableName, readHandle);
        if(succsess == -1){
            return -1;
        }

        vector<Attribute> recordDesc;
        getAttributes(tableName, recordDesc);    



        //findDeletedAttrs(tableName, recordDesc);
        

        succsess = rbfm.readRecord(readHandle, recordDesc, rid, data);
        if(succsess == -1){
            return -1;
        }
        rbfm.closeFile(readHandle);

        return 0;
    
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        int succsess = rbfm.printRecord(attrs, data, out);
        if(succsess == -1){
            return -1;
        }
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        FileHandle attrHandle;
        int succsess = rbfm.openFile(tableName, attrHandle);
        if(succsess == -1){
            return -1;
        }

        vector<Attribute> recordDesc;
        getAttributes(tableName, recordDesc);
        
        succsess = rbfm.readAttribute(attrHandle, recordDesc, rid, attributeName, data);
        if(succsess == -1){
            return -1;
        }
        rbfm.closeFile(attrHandle);
        return 0;
    }


    void RelationManager::readEntireTable(const string &tableName){
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle readHandle;

        rbfm.openFile(tableName, readHandle);

        vector<Attribute> recordDesc;
        getAttributes(tableName, recordDesc);

        vector<string> desiredAttrs;
        for(int i = 0; i < recordDesc.size(); i++){
            desiredAttrs.push_back(recordDesc[i].name);
        }
        void* value = nullptr;
        RBFM_ScanIterator rbfmIter;

        rbfm.scan(readHandle, recordDesc, string(""), NO_OP, value, desiredAttrs, rbfmIter);

        RID rid;
        void* outBuffer = malloc(PAGE_SIZE);
        while(rbfmIter.getNextRecord(rid, outBuffer) != -1){
            char* dataPointer = (char*) outBuffer;
            int offset = ceil((double) desiredAttrs.size()/8);
            unsigned char* nulls = new unsigned char[offset];
            memcpy(nulls, outBuffer, offset);
            cout << "Record #" << rid.slotNum << " Info: { ";
            int x = 0;
            for(int i = 0; i < recordDesc.size(); i++){
                const Attribute &attr = recordDesc[i];
                string attrName = desiredAttrs[x];
                if(attr.name == attrName){
                    //cout << "Attr Name: " << attr.name << ", Attr Offset: " << offset << ", ";
                    x++;
                    if((nulls[i / 8] >> (7 - i % 8)) & 1){
                        cout << attr.name << ": " << "NULL, "; 
                        continue;
                    }
                    else if(attr.type == TypeInt){
                        cout << attr.name << ": ";
                        int number;
                        memcpy(&number, dataPointer+offset, 4);
                        offset += 4;
                        cout << number << ", ";
                    }
                    else if(attr.type == TypeReal){
                        cout << attr.name << ": ";
                        float decimal;
                        memcpy(&decimal, dataPointer+offset, 4);
                        offset += 4;
                        cout << decimal << ", ";
                    }
                    else if(attr.type == TypeVarChar){
                        cout << attr.name << ": ";
                        int length;
                        memcpy(&length, dataPointer+offset, 4);
                        offset += 4;
                        char* attrString = new char[length+1];
                        memcpy(attrString, dataPointer+offset, length);
                        attrString[length] = '\0';
                        offset += length;
                        cout << attrString << ", ";
                    }
                    if(i == (recordDesc.size()-1) && x < desiredAttrs.size()){
                        i = 0;
                        offset = 0;
                    }
                    if(x >= desiredAttrs.size()){
                        break;
                    }
                }
            }
            cout << " }" << endl << "-----------------------------------" << endl;
        }
        free(outBuffer);
        rbfmIter.close();
        rbfm.closeFile(readHandle);
        
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        int succsess = rbfm.openFile(tableName, rm_ScanIterator.fileHandle);
        if(succsess == -1){
            return -1;
        }


        vector<Attribute> recordDesc;
        getAttributes(tableName, recordDesc);

        rbfm.scan(rm_ScanIterator.fileHandle, recordDesc, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmIter);
        //cout << "Scanning: " << rm_ScanIterator.fileHandle.getNumberOfPages() << endl;

        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
        //cout << "Page: " << rbfmIter.curRid.pageNum+1 << " Slot: " << rbfmIter.curRid.slotNum << endl;
        int res = rbfmIter.getNextRecord(rid, data);
        
        return res;

    }

    RC RM_ScanIterator::close(){
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        rbfmIter.close();
        if(fileHandle.filePointer != NULL){
            rbfm.closeFile(fileHandle);
        }
        
        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);

        vector<Attribute> recordDesc = createColumnDescriptor();

        string resAttr = "table-id";
        vector<string> desiredAttrs;
        desiredAttrs.push_back(resAttr);
        resAttr = "column-type";
        desiredAttrs.push_back(resAttr);
        resAttr = "column-length";
        desiredAttrs.push_back(resAttr);
        resAttr = "column-position";
        desiredAttrs.push_back(resAttr);

        int attrLength = attributeName.length();
        void* value = malloc(4+attrLength);
        const char* attrString = attributeName.c_str();
        memcpy((char*)value, &attrLength, 4);
        memcpy((char*)value+4, attrString, attrLength);

        RBFM_ScanIterator rbfmIter;
        rbfm.scan(columnHandle, recordDesc, string("column-name"), EQ_OP, value, desiredAttrs, rbfmIter);
        int tableId = getIdFromName(rbfm, tableName);

        RID rid;
        RID newRid;
        int columnType;
        int columnLength;
        int columnPosition;

        void* outBuffer = malloc(PAGE_SIZE);
        while(rbfmIter.getNextRecord(rid, outBuffer) != -1){
            int curId;
            memcpy(&curId, outBuffer+1, 4);
            if(curId == tableId){
                newRid = rid;
                memcpy(&columnType, outBuffer+5, 4);
                memcpy(&columnLength, outBuffer+9, 4);
                memcpy(&columnPosition, outBuffer+13, 4);
                break;
            }
        }

        char* curPage = (char*) malloc(PAGE_SIZE);
        fillColumnCatalog(tableId, attributeName, columnType, columnLength, columnPosition, 1, curPage);

        int succsess = rbfm.updateRecord(columnHandle, recordDesc, curPage, newRid);
        if(succsess == -1){
            return -1;
        }
        return 0;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);

        vector<Attribute> recordDesc = createColumnDescriptor();
        int tableId = getIdFromName(rbfm, tableName);
      
        string resAttr = "column-position";
        vector<string> desiredAttrs;
        desiredAttrs.push_back(resAttr); 

        void* value = malloc(PAGE_SIZE);
        memcpy(value, &tableId, 4);

        RBFM_ScanIterator rbfmIter;
        rbfm.scan(columnHandle, recordDesc, string("table-id"), EQ_OP, value, desiredAttrs, rbfmIter);

        int newestPos = 1;

        RID rid;
        void* outBuffer = malloc(PAGE_SIZE);
        while(rbfmIter.getNextRecord(rid, outBuffer) != -1){
            int curPos;
            memcpy(&curPos, outBuffer+1, 4);
            if(curPos>newestPos){
                newestPos = curPos;
            }
        } 

        char* curPage = (char*) malloc(PAGE_SIZE);
        fillColumnCatalog(tableId, attr.name, attr.type, attr.length, newestPos, 0, curPage);
        
        RID insertRid;
        rbfm.insertRecord(columnHandle, recordDesc, curPage, insertRid);
        rbfm.closeFile(columnHandle);
        free(outBuffer);
        free(curPage);
        return 0;


    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB