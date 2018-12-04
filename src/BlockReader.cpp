//
// Created by leon on 11/12/18.
//

#include "Relation.h"
#include "BlockReader.h"
#include "MemoryManager.h"

BlockReader::BlockReader(Relation* r,MemoryManager* mngr) {
    this->mngr = mngr;
    relation = r;
	tableStream.open(r->getFile());
    readNextLine();
}

void BlockReader::readNextLine() {
    buffer = "";
    basic_istream<char, char_traits<char>> &curStream = getline(tableStream, buffer);
    if(curStream){
        isDone = false;
    } else{
        isDone = true;
    }
}

Block* BlockReader::nextBlock() {
    //TODO: make the mnemory manager a singleton;
    if(isDone){
        throw std::domain_error("No more tuples to be read.");
    }
    Block* returnBlock = mngr->allocateEmptyBlock();
    Tuple* tup = mngr->createTuple(buffer);
    if(tup->getSizeInBytes() <=mngr->getBlockSizeInBytes()){
        returnBlock->addTuple(tup);
    } else{
        mngr->deleteTuple(tup);
        throw std::domain_error("Size of single is bigger than block size.");
    }
    while(true){
        readNextLine();
        if (!isDone && mngr->canCreatTuple(buffer)) {
            Tuple *tup = mngr->createTuple(buffer);
            bool success = returnBlock->addTuple(tup);
            if (!success) {
                mngr->deleteTuple(tup);
                break;
            }
        } else{
            break;
        }
    }
    return returnBlock;
}

bool BlockReader::hasNext() {
    return !isDone;
}
