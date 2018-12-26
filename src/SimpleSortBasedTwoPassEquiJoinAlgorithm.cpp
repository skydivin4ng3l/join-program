//
// Created by leon on 11/13/18.
//


//#include <cmath>
#include "SimpleSortBasedTwoPassEquiJoinAlgorithm.h"

SimpleSortBasedTwoPassEquiJoinAlgorithm::SimpleSortBasedTwoPassEquiJoinAlgorithm(MemoryManager* memoryManager) {
    this->memoryManager = memoryManager;
}

std::string SimpleSortBasedTwoPassEquiJoinAlgorithm::twoPassMultiwayMergeSort(Relation *relation, int joinAttributeIndex){
    std::multimap<std::string,Tuple*> indexJoinAttributeToTuple;
    std::vector<Block*> loadedRelationChunk;
    std::string sortedRelation = "Relation_"+ static_cast<std::string>(relation)+"_SortedBy_"+ static_cast<std::string>(joinAttributeIndex)+"_JoinAttribute.txt";
    //deletes output from previous runs:
    remove(sortedRelation.c_str());
    std::vector<BlockReader*> partialFilesReaders;
    //std::vector<std::string> partialSortedFiles;
    int partialSortedFileNumber = 0;

    auto relationReader = relation->getReader();
    //TODO
    // create new partial sort file
    // load the memory full with blocks and put them into a "self-sorting" multimap pointer data structure
    // put them back into blocks in sorted order
    // print them to this file,
    // release Memory, add filename to returning string vector
    // repeat until relation complete

    while(relationReader->hasNext()){
        std::string partialSortedFileName = "partialSorted_"+ static_cast<std::string>(partialSortedFileNumber)+".txt";
        //deletes output from previous runs:
        remove(partialSortedFileName.c_str());
        //Fill Ram with blocks
        while (memoryManager->getNumFreeBlocks() > 1 && relationReader->hasNext() ) {

            Block* loadedBlock = relationReader->nextBlock();
            loadedRelationChunk.push_back(loadedBlock);
            for(auto& currentOuterTuple : loadedBlock->getTuples() ) {
                // order tuples by join attribute
                indexJoinAttributeToTuple.insert( std::make_pair(currentOuterTuple->getData(joinAttributeIndex),currentOuterTuple));
            }
        }
        Block* outputBlock = memoryManager->allocateEmptyBlock();
        //print the ordered chuck to disk
        for(auto it=indexJoinAttributeToTuple.begin(); it !=indexJoinAttributeToTuple.end(); it++){
            if (outputBlock->addTuple(it->second)) {} else {
                outputBlock->writeBlockToDisk(partialSortedFileName);
                memoryManager->clearBlock(outputBlock);
                outputBlock->addTuple(it->second);
            }
        }
        outputBlock->writeBlockToDisk(partialSortedFileName);
        //free memory
        memoryManager->deleteBlock(outputBlock);
        for(auto blockToUnload : loadedRelationChunk) {
            memoryManager->deleteBlock(blockToUnload);
        }
        indexJoinAttributeToTuple.clear();
        partialSortedFileNumber++;
        //insert the sorted partial file into a data structure for further processing
        Relation partialRelation = Relation(partialSortedFileName,memoryManager);
        partialFilesReaders.push_back(partialRelation.getReader());
    }

    //read and merge all sorted partial files
    while (any_of(partialFilesReaders.begin(), partialFilesReaders.end(), [](BlockReader* reader){reader->hasNext();})){
        std::vector<Block*> blocksToMerge;
        std::multimap<std::string,Tuple*> sortedTuples;
        //read one block of each file and merge them
        for(auto reader : partialFilesReaders){
            if (reader->hasNext()) {
                Block* loadedBlock = reader->nextBlock();
                blocksToMerge.push_back(loadedBlock);
                for(auto& currentTuple : loadedBlock->getTuples() ) {
                    sortedTuples.insert( std::make_pair(currentTuple->getData(joinAttributeIndex),currentTuple));
                }
            }
        }
        //write the merged blocks
        Block* outputBlock = memoryManager->allocateEmptyBlock();
        for(auto it=sortedTuples.begin(); it !=sortedTuples.end(); it++){
            if (outputBlock->addTuple(it->second)) {} else {
                outputBlock->writeBlockToDisk(sortedRelation);
                memoryManager->clearBlock(outputBlock);
                outputBlock->addTuple(it->second);
            }
        }
        outputBlock->writeBlockToDisk(sortedRelation);
        //free memory
        memoryManager->deleteBlock(outputBlock);
        for(auto blockToUnload : blocksToMerge) {
            memoryManager->deleteBlock(blockToUnload);
        }
        sortedTuples.clear();

    }


    return sortedRelation;
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::join(Relation* left, Relation* right, int leftJoinAttributeIndex, int rightJoinAttributeIndex,string outputFile) {

    //actual joining
    std::string sortedLeft = twoPassMultiwayMergeSort(left,leftJoinAttributeIndex);
    std::string sortedRight = twoPassMultiwayMergeSort(right, rightJoinAttributeIndex);
    //TODO Phase 2: MergeR und S
    //1.
    //Jeweils ein Block
    //2.
    //Suche insgesamt kleinstes Y in beiden Blocks
    //3.
    //Falls nicht in anderem Block vorhanden: Entferne alle Tupel mit diesem Y
    //4.
    //Falls vorhanden: Identifiziere alle Tupel mit diesem Y
    //–
    //Gegebenenfalls nachladen
    //5.
    //Gebe alle Kombinationen aus
    Block* outputBlock = memoryManager->allocateEmptyBlock();


}



