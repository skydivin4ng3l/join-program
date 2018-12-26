//
// Created by leon on 11/14/18.
//

#include "MemoryManager.h"


MemoryManager::MemoryManager(int maxNumBlocks, int blockSizeInBytes) {
    this->maxNumBlocks = maxNumBlocks;
    this->blockSizeInBytes = blockSizeInBytes;
    this->numFreeBlocks=maxNumBlocks;
    this->memoryUsedByTuples = 0;
    assertIntegrity();
}

void MemoryManager::clearBlock(Block* block) {
    auto deleteTuple = [&](Tuple* tup) {
        memoryUsedByTuples -= tup->getSizeInBytes();
        delete tup; };
    for_each(block->tuples.begin(), block->tuples.end(), deleteTuple);
    block->tuples.clear();
    block->currentSizeInBytes = 0;
    assertIntegrity();
}

Block* MemoryManager::allocateEmptyBlock() {
    if (numFreeBlocks > 0) {
        numFreeBlocks--;
        assertIntegrity();
        return new Block(blockSizeInBytes);
    } else {
        throw std::domain_error("No more Blocks available");
    }
}

void MemoryManager::deleteBlock(Block* block) {
    assertIntegrity();
    clearBlock(block);
    delete block;
    numFreeBlocks +=1;
    assertIntegrity();
}

void MemoryManager::deleteBlockOnly(Block* block){
    assertIntegrity();
    delete block;
    numFreeBlocks +=1;
    assertIntegrity();
}

int MemoryManager::getNumFreeBlocks() {
    return numFreeBlocks;
}

int MemoryManager::getMaxNumBlocks() {
    return maxNumBlocks;
}

Tuple* MemoryManager::createTuple(string data) {
    Tuple *tuple = new Tuple(data);
    return accountTupleMemory(tuple);
}

Tuple *MemoryManager::accountTupleMemory(Tuple *tuple) {
    if (memoryUsedByTuples + tuple->getSizeInBytes() <= getMaximumMemoryInBytes()){
        memoryUsedByTuples += tuple->getSizeInBytes();
        assertIntegrity();
        return tuple;
    } else{
        delete tuple;
        throw domain_error("Not enough memory available to create tuple");
    }
}

int MemoryManager::getBlockSizeInBytes() {
	return blockSizeInBytes;
}

void MemoryManager::deleteTuple(Tuple *tuple) {
    memoryUsedByTuples -= tuple->getSizeInBytes();
    delete tuple;
    assertIntegrity();
}

Tuple *MemoryManager::createTuple(std::vector<string> strings) {
    Tuple *tuple = new Tuple(strings);
    accountTupleMemory(tuple);
    assertIntegrity();
    return tuple;
}

int MemoryManager::getMemoryUsedByTuples() {
    return memoryUsedByTuples;
}

int MemoryManager::getMaximumMemoryInBytes() {
    return maxNumBlocks * blockSizeInBytes;
}

bool MemoryManager::canCreateTuple(vector<string> vector) {
    Tuple *tuple = new Tuple(vector);
    int tupleSize = tuple->getSizeInBytes();
    delete tuple;
    assertIntegrity();
    if(memoryUsedByTuples + tupleSize <= getMaximumMemoryInBytes()){
        return true;
    } else{
        return false;
    }
}

bool MemoryManager::canCreatTuple(string data) {
    Tuple *tuple = new Tuple(data);
    int tupleSize = tuple->getSizeInBytes();
    delete tuple;
    assertIntegrity();
    if(memoryUsedByTuples + tupleSize <= getMaximumMemoryInBytes()){
        return true;
    } else{
        return false;
    }
}

int MemoryManager::getFreeMemoryInBytes() {
    return getMaximumMemoryInBytes() - getMemoryUsedByTuples();
}

void MemoryManager::assertIntegrity() {
    /*if(getMemoryUsedByTuples() > (maxNumBlocks-numFreeBlocks) * blockSizeInBytes){
        throw domain_error("Internal consistency error: Tuples demand more memory than blocks that are avaialble");
    }*/

}

void MemoryManager::printStatus() {
    cout << "---------------------------------------------------------------Memory consumption report---------------------------------------------------------------" << endl;
    cout << "Block capacity: " << blockSizeInBytes << " Bytes" << endl;
    cout << "allocated Blocks: " << (maxNumBlocks-numFreeBlocks) << "/" << maxNumBlocks << endl;
    cout << "allocated Memory (tuples):" << memoryUsedByTuples << "/" << getMaximumMemoryInBytes() << " Bytes" << endl;
    cout << "-------------------------------------------------------------------------------------------------------------------------------------------------------" << endl;
}

