#include <sys/stat.h>
#include "Relation.h"

using namespace std;

Relation::Relation(string file,MemoryManager* mngr) {
    this->mngr = mngr;
    this->file = file;
    this->size = getFileSize(file);
}

/*
long Relation::getFileSize(std::string filename)
{
    std::ifstream file(filename, std::ifstream::ate | std::ifstream::binary);
    file.exceptions(ifstream::badbit);
    return file.tellg();
}*/

long Relation::getFileSize(std::string filename)
{
	struct stat stat_buf;
	int rc = stat(filename.c_str(), &stat_buf);
	return rc == 0 ? stat_buf.st_size : -1;
}

BlockReader* Relation::getReader(){
	BlockReader* reader = new BlockReader(this,mngr);
	return reader;
}


string Relation::getFile() {
	return file;
}

long Relation::getSize() {
	return size;
}
