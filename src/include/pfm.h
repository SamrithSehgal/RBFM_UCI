#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096
#include <string>
using namespace std;

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const string &fileName);                         // Create a new file
        RC destroyFile(const string &fileName);                        // Destroy a file
        RC openFile(const string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file
    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    private:
    };

    class CounterTracker {
        public:
            static CounterTracker &instance();
            RC initCT();
            RC getRCounter();
            RC getWCounter();
            RC getACounter();
            RC writeRCounter();
            RC writeWCounter();
            RC writeACounter();
            FILE* trackerPointer = nullptr;
            string fileName = "counterVals";
    };

    class FileHandle {
        public:
            // variables to keep the counter for each operation
            unsigned readPageCounter;
            unsigned writePageCounter;
            unsigned appendPageCounter;

            FileHandle();                                                       // Default constructor
            ~FileHandle();                                                      // Destructor

            RC readPage(PageNum pageNum, void *data);                           // Get a specific page
            RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
            RC appendPage(const void *data);                                    // Append a specific page
            unsigned getNumberOfPages();                                        // Get the number of pages in the file
            RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                    unsigned &appendPageCount);                 // Put current counter values into variables
            FILE *filePointer;
            string fileName;
            CounterTracker ct = CounterTracker();
                       
    };

} // namespace PeterDB

#endif // _pfm_h_