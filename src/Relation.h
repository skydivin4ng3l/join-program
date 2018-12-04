#ifndef RELATION
#define RELATION

#include <string>
#include <fstream>
#include <vector>
#include <iostream>
#include <tuple>
#include "Block.h"
#include "BlockReader.h"

using namespace std;


class Relation {
	public:
		 //constructs a relation from a csv-file (commas in values are not allowed, as escaping is not handled!) and the given MemoryManger
		Relation(string file,MemoryManager* mngr);
		 //returns a BlockReader that can read the Relation sequentially (Block by Block)
		BlockReader* getReader();
		//returns the file of this Relation
		string getFile();
		//Returns the size of the file associated with this relation (in Bytes)
    	long getSize();

private:
		string file;
        MemoryManager* mngr;
		long size;
		long getFileSize(std::string filename);
};


#endif