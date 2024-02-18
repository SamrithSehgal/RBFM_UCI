#include <string>
typedef int RC;
#define PAGE_SIZE 4096
typedef unsigned PageNum;
using namespace std;


class FileHandle
{
    public:
        FileHandle(/* args */);
        ~FileHandle();

        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        RC readPage(unsigned pageNum, void *data);
        RC writePage(unsigned pageNum, void *data);
        RC appendPage(void *data);
        unsigned getNumberOfPages();
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
        FILE *filePointer = NULL;
        string fileName = NULL;

};

class PagedFileManager
{
    protected:
        PagedFileManager(/* args */);
        ~PagedFileManager();
        PagedFileManager(const PagedFileManager &); // learn
        PagedFileManager &operator=(const PagedFileManager &);  
    public:
        RC createFile(const std::string &filename);
        RC destroyFile(const std::string &filename);
        RC openFile(const std::string &fileName, FileHandle &fileHandle);
        RC closeFile(FileHandle &fileHandle);
        static PagedFileManager* PagedFileManager::createPFM();
        static PagedFileManager* pfm;
};

