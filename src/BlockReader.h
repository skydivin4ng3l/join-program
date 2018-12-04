//
// Created by leon on 11/13/18.
//

#ifndef JOIN_PROGRAM_BLOCKREADER_H
#define JOIN_PROGRAM_BLOCKREADER_H

#include "Block.h"
#include "MemoryManager.h"

class Relation;

/***
 * Implementation of the Block-wise sequential Scan of a relation.
 */
class BlockReader {
public:
    BlockReader(Relation *pRelation,MemoryManager* mngr);
    //Reads new Input and returns it as a new Block. The new Block is registered with the MemoryManager. If there are no more Blocks available, this throws an exception.
    Block* nextBlock();
    //Returns true if there is more input to be read, false otherwise
    bool hasNext();

private:
    ifstream tableStream;
    Relation* relation;
    string buffer;
    bool isDone;
    //reads the next tuple and saves its string representation to buffer
    void readNextLine();

    MemoryManager *mngr;
};

#endif //JOIN_PROGRAM_BLOCKREADER_H
