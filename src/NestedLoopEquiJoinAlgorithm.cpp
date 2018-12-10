//
// Created by leon on 11/13/18.
//


#include <cmath>
#include "NestedLoopEquiJoinAlgorithm.h"

NestedLoopEquiJoinAlgorithm::NestedLoopEquiJoinAlgorithm(MemoryManager* memoryManager) {
    this->memoryManager = memoryManager;
}

joinStringTupleIndexAndBlockPointerVectorPair NestedLoopEquiJoinAlgorithm::buildDatastructureForOuterRelationChunk(
        BlockReader *outerReader, int joinAttributeIndex){
    std::unordered_multimap<std::string,Tuple*> indexJoinAttributeToTuple;
    std::vector<Block*> loadedOuterRelationChunk;

    while (memoryManager->getNumFreeBlocks() > 1 && outerReader->hasNext() ) {
        //load the memory full with outer blocks and put them into a pointer data structure
        Block* loadedBlock = outerReader->nextBlock();
        loadedOuterRelationChunk.push_back(loadedBlock);
        for(auto& currentOuterTuple : loadedBlock->getTuples() ) {
            indexJoinAttributeToTuple.insert( std::make_pair(currentOuterTuple->getData(joinAttributeIndex),currentOuterTuple));
        }
    }
    return make_pair(indexJoinAttributeToTuple,loadedOuterRelationChunk);
}

void NestedLoopEquiJoinAlgorithm::join(Relation* left, Relation* right, int leftJoinAttributeIndex, int rightJoinAttributeIndex,string outputFile) {
    // check which relation is the bigger one, one with less blocks should be outside
    Relation* outer = right;
    Relation* inner = left;
    int outerJoinAttributeIndex = rightJoinAttributeIndex;
    int innerJoinAttributeIndex = leftJoinAttributeIndex;

    if ( left->getSize() < right->getSize() ) {
        outer = left;
        inner = right;
        innerJoinAttributeIndex = rightJoinAttributeIndex;
        outerJoinAttributeIndex = leftJoinAttributeIndex;
        cout << "switching Relation order " << endl;
    }

    //actual joining
    auto outerReader = outer->getReader();
    auto innerReader = inner->getReader();
    Block* outputBlock = memoryManager->allocateEmptyBlock();

    while ( outerReader->hasNext() ) {
        joinStringTupleIndexAndBlockPointerVectorPair outerBlocksChunkDataStructure = buildDatastructureForOuterRelationChunk(
                outerReader, outerJoinAttributeIndex);

       while (memoryManager->getNumFreeBlocks() > 0 && innerReader->hasNext() ) {
           Block* currentInnerBlock = innerReader->nextBlock();
           for(auto& currentInnerTuple: currentInnerBlock->getTuples() ){
               auto range = outerBlocksChunkDataStructure.first.equal_range(currentInnerTuple->getData(innerJoinAttributeIndex));
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
        for(auto blockToUnload : outerBlocksChunkDataStructure.second) {
            memoryManager->deleteBlock(blockToUnload);
        }
    }
    outputBlock->writeBlockToDisk(outputFile);
    memoryManager->deleteBlock(outputBlock);

}



