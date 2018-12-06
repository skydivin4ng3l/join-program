//
// Created by leon on 11/13/18.
//

#include <unordered_map>
#include <stack>
#include "NestedLoopEquiJoinAlgorithm.h"

NestedLoopEquiJoinAlgorithm::NestedLoopEquiJoinAlgorithm(MemoryManager* memoryManager) {
    this->memoryManager = memoryManager;
}

joinStringTupleIndexBlockPointerStackPair NestedLoopEquiJoinAlgorithm::buildDatastructForOuterRelationChunk(BlockReader* outerReader, int joinAttributeIndex){
    std::unordered_multimap<std::string,Tuple*> indexJoinAttributeToTuple;
    std::stack<Block*> loadedOuterRelationChunk;

    while (memoryManager->getNumFreeBlocks() - 1 > 1 && outerReader->hasNext() ) {
        //TODO: actually load the memory full with outer blocks and put them into a pointer data structure
        Block* loadedBlock = outerReader->nextBlock();
        loadedOuterRelationChunk.push(loadedBlock);
        for(auto& currentOuterTuple : loadedBlock->getTuples() ) {
            indexJoinAttributeToTuple.insert( std::make_pair(currentOuterTuple->getData(joinAttributeIndex),currentOuterTuple));
        }
    }
    return make_pair(indexJoinAttributeToTuple,loadedOuterRelationChunk);
}

void NestedLoopEquiJoinAlgorithm::join(Relation* left, Relation* right, int leftJoinAttributeIndex, int rightJoinAttributeIndex,string outputFile) {
    //Task: implement
    // check which relation is the bigger one, smaller one should be outside LEF IS ALWAYS outer
    Relation* outer = right;
    Relation* inner = left;
    int outerJoinAttributeIndex = rightJoinAttributeIndex;
    int innerJoinAttributeIndex = leftJoinAttributeIndex;

    if ( left->getSize() <= right->getSize() ) {
        outer = left;
        inner = right;
        innerJoinAttributeIndex = rightJoinAttributeIndex;
        outerJoinAttributeIndex = leftJoinAttributeIndex;
    }
    
    auto outerReader = outer->getReader();
    auto innerReader = inner->getReader();
    std::unordered_multimap<std::string,Tuple*> outerJoinIndex;
    Block* outputBlock = memoryManager->allocateEmptyBlock();

    while ( outerReader->hasNext() ) {
        joinStringTupleIndexBlockPointerStackPair outerBlocksDataStructChunk = buildDatastructForOuterRelationChunk(outerReader, outerJoinAttributeIndex);

       while (memoryManager->getNumFreeBlocks() - 1 > 0 && innerReader->hasNext() ) {
           Block* currentInnerBlock = innerReader->nextBlock();
           for(auto& currentInnerTuple: currentInnerBlock->getTuples() ){
               auto range = outerJoinIndex.equal_range(currentInnerTuple->getData(innerJoinAttributeIndex));
               for (auto it = range.first; it != range.second; ++it) {
                   //TODO: Optimize combination of tuple
                   std::vector<std::string> joinedTupleStrings;
                   std::vector<std::string> outerTupleStrings = it->second->tupleToStringVector();
                   std::vector<std::string> innerTupleStrings = currentInnerTuple->tupleToStringVector();
                   joinedTupleStrings.reserve(outerTupleStrings.size()+innerTupleStrings.size());
                   joinedTupleStrings.insert(joinedTupleStrings.end(),outerTupleStrings.begin(),outerTupleStrings.end());
                   joinedTupleStrings.insert(joinedTupleStrings.end(),innerTupleStrings.begin(),innerTupleStrings.end());
                   Tuple *joinedTuple;
                   if (!memoryManager->canCreateTuple(joinedTupleStrings)) {
                       outputBlock->writeBlockToDisk(outputFile);
                       memoryManager->clearBlock(outputBlock);
                   }
                   joinedTuple = memoryManager->createTuple(joinedTupleStrings);

                   if (outputBlock->addTuple(joinedTuple)) {} else {
                       outputBlock->writeBlockToDisk(outputFile);
                       memoryManager->clearBlock(outputBlock);
                       outputBlock->addTuple(joinedTuple);
                   }
               }
           }
           memoryManager->deleteBlock(currentInnerBlock);
       }

    }
    memoryManager->deleteBlock(outputBlock);

    for(auto& keyWithTuple: outerJoinIndex) {
        std::cout << "Key: " << keyWithTuple.first << " associated with Tuple: ";
        keyWithTuple.second->printData();
    }
}



