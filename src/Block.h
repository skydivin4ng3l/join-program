#ifndef BLOCK
#define BLOCK

#include <vector>
#include <algorithm>
#include <tuple>
#include <string>
#include <fstream>
#include "Tuple.h"

using namespace std;

/***
 * Implementation of a Block containing a number of tuples. The Block is fixed in terms of Memory, not tuples.
 */
class Block {

	friend class MemoryManager;

	public:
		//returns a vector containing all of the tuples loaded with this block
		vector<Tuple*> getTuples();
		//adds the specified Tuple object to the end of the tuples vector of this block; returns true if successful, false otherwise (if the Block was full)
		bool addTuple(Tuple*);
		//returns the total memory occupied by the tuples that are currently in this block
		int getCurrentSizeBytes();
		//Appends all Tuples in this Block to the specified file
		void writeBlockToDisk(string file);


private:
		vector<Tuple*> tuples;
		int blocksize;
		int currentSizeInBytes;
		//creates a new block with the maximum memory footprint of the passed int in Byte
		Block(int);
		//reads a block with the maximum memory footprint of the passed int in Byte from the specified file
		//Block(int, string, vector<tuple<string, char, int16_t>>);

};

#endif