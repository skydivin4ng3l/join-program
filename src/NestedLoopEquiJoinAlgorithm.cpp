//
// Created by leon on 11/13/18.
//

#include <unordered_map>
#include "NestedLoopEquiJoinAlgorithm.h"

NestedLoopEquiJoinAlgorithm::NestedLoopEquiJoinAlgorithm(MemoryManager* memoryManager) {
    this->memoryManager = memoryManager;
}

void NestedLoopEquiJoinAlgorithm::join(Relation* left, Relation* right, int leftJoinAttributeIndex, int rightJoinAttributeIndex,string outputFile) {
    //Task: implement
    // check which relation is the bigger one, smaller one should be outside LEF IS ALWAYS OUTTER
    Relation* outter = right;
    Relation* inner = left;
    int outterJoinAttributeIndex = rightJoinAttributeIndex;
    int innerJoinAttributeIndex = leftJoinAttributeIndex;

    if ( left->getSize() <= right->getSize() ) {
        outter = left;
        inner = right;
        innerJoinAttributeIndex = rightJoinAttributeIndex;
        outterJoinAttributeIndex = leftJoinAttributeIndex;
    }

    auto outterReader = outter->getReader();
    auto innerReader = inner->getReader();
    std::unordered_multimap<std::string,Tuple*> outterJoinIndex;
    Block* outputBlock = this->memoryManager->allocateEmptyBlock();

    while ( (this->memoryManager->getNumFreeBlocks()-1 > 1)  && outterReader->hasNext() ) {
       for(auto& currentOuterTupel : outterReader->nextBlock()->getTuples() ) {
           outterJoinIndex.insert( std::make_pair(currentOuterTupel->getData(outterJoinAttributeIndex),currentOuterTupel));
       }
       while ( (this->memoryManager->getNumFreeBlocks()-1 > 0) && innerReader->hasNext() ) {
           Block* currentInnerBlock = innerReader->nextBlock();
           for(auto& currentInnerTupel: currentInnerBlock->getTuples() ){
               auto range = outterJoinIndex.equal_range(currentInnerTupel->getData(innerJoinAttributeIndex));
               for (auto it = range.first; it != range.second; ++it) {
                   Tuple joinedTupel;
                   joinedTupel = *currentInnerTupel;
                   //TODO: it->second. HOWTO combine tupel
                   if (outputBlock->addTuple(joinedTupel)) {} else {
                       outputBlock->writeBlockToDisk(outputFile);
                       memoryManager->clearBlock(outputBlock);
                       outputBlock->addTuple(joinedTupel);
                   }
                   }
               }
           }
       }
       
    }

    for(auto& keyWithTupel: outterJoinIndex) {
        std::cout << "Key: " << keyWithTupel.first << " associated with Tupel: ";
        keyWithTupel.second->printData();
    }
}



