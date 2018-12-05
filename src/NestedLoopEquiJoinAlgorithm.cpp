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

    while ( (this->memoryManager->getNumFreeBlocks()-1 > 1)  && outterReader->hasNext() ) {
       for(auto& t : outterReader->nextBlock()->getTuples() ) {
           outterJoinIndex.insert( std::make_pair(t->getData(outterJoinAttributeIndex),t));
       }
    }

    for(auto& keyWithTupel: outterJoinIndex) {
        std::cout << "Key: " << keyWithTupel.first << " associated with Tupel: ";
        keyWithTupel.second->printData();
    }
}



