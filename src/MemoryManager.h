//
// Created by leon on 11/14/18.
//

#ifndef JOIN_PROGRAM_MEMORYMANAGER_H
#define JOIN_PROGRAM_MEMORYMANAGER_H


#include "Block.h"

/***
 * Class that keeps track of Memory Usage and also works as a Factory that creates Blocks and Tuples.
 * Memory of the created Tuples is accounted for. If you try to create more tuples than could fit into the Memory (as defined by the number of Blocks and the Block size) you will get an exception.
 * Additionally you can never create more than the maximum number of Blocks and will get an exception if you try.
 * Blocks can be cleared or deleted. Tuples can only be destroyed by clearing/deleting a Block that contains them.
 * All Blocks that are created anywhere are registered in this class, also those created by the BlockReader (see BlockReader.h)
 *
 */
class MemoryManager {
    friend class BlockReader;

public:
    //creates a new MemoryManager with the specified constraints
    MemoryManager(int maxNumBlocks,int blockSizeInBytes);
    //allocates a new Block, throws an exception if there is no enough memory available.
    Block* allocateEmptyBlock();
    //deletes the Block and all its contained tuples, freeing the memory.
    void deleteBlock(Block*);
    //deletes all tuples in this block and releases the accumulated memory of all tuples. The block object itsself is NOT deleted, which means it can be reused.
    void clearBlock(Block*);
    //getters:

    //returns the number of available (unallocated) blocks.
    int getNumFreeBlocks();
    //returns the maximum number of blocks that can be allocated
    int getMaxNumBlocks();
    //returns the block size in bytes
    int getBlockSizeInBytes();
    //creates a new tuple from all values given in the arguments, throws an exception if there is not enough available memory.
    Tuple* createTuple(std::vector<string>);
    //Returns true if there is enough memory to create a tuple from the given fields, false otherwise
    bool canCreateTuple(vector<string> vector);
    //returns the total number of bytes occupied by all tuples that are currently in memory
    int getMemoryUsedByTuples();
    //returns the maximum total memory (in Bytes) that is available to store tuples. This is calculated by blockSizeInBytes*maxNumBlocks
    int getMaximumMemoryInBytes();
    //convenience function that returns getMaximumMemoryInBytes()- getMemoryUsedByTuples()
    int getFreeMemoryInBytes();

private:
    int maxNumBlocks;
    int blockSizeInBytes;
    int numFreeBlocks;
    int memoryUsedByTuples;

    //creates a tuple from the string read from a file of a relation
    Tuple* createTuple(string basic_string);
    //Returns true if there is enough memory to create a tuple from the given data, false otherwise
    bool canCreatTuple(string data);
    //returns the tuple if it fits into the remaining memory (and updates used memory), otherwise throws an exception
    Tuple *accountTupleMemory(Tuple *tuple);

    void deleteTuple(Tuple *tup);

    void assertIntegrity();
};

#endif //JOIN_PROGRAM_MEMORYMANAGER_H
