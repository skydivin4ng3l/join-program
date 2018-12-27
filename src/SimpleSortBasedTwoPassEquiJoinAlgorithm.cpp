//
// Created by leon on 11/13/18.
//


//#include <cmath>
#include "SimpleSortBasedTwoPassEquiJoinAlgorithm.h"

SimpleSortBasedTwoPassEquiJoinAlgorithm::SimpleSortBasedTwoPassEquiJoinAlgorithm(MemoryManager* memoryManager) {
    this->memoryManager = memoryManager;
}

std::string SimpleSortBasedTwoPassEquiJoinAlgorithm::twoPassMultiwayMergeSort(Relation *relation, int joinAttributeIndex){
    joinStringTupleIndex indexJoinAttributeToTuple;
    std::vector<Block*> loadedRelationChunk;
    std::hash<std::string> hash_fn;
    std::size_t hashedFileRelationFileName = hash_fn(relation->getFile());
    std::stringstream ss;
    ss << "Relation_" << hashedFileRelationFileName << "_SortedBy_"<< joinAttributeIndex << "_JoinAttribute.txt";
    std::string sortedRelationFile = ss.str();
    //deletes output from previous runs:
    remove(sortedRelationFile.c_str());
    std::vector<BlockReader*> partialFilesReaders;
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
        std::string partialSortedFileName = "partialSorted_"+ std::to_string(partialSortedFileNumber)+".txt";
        //deletes output from previous runs:
        remove(partialSortedFileName.c_str());
        //Fill Ram with blocks
        while (memoryManager->getNumFreeBlocks() > 1 && relationReader->hasNext() ) {

            Block* loadedBlock = relationReader->nextBlock();
            loadedRelationChunk.push_back(loadedBlock);
            for(auto& currentTuple : loadedBlock->getTuples() ) {
                // order tuples by join attribute
                indexJoinAttributeToTuple.insert( std::make_pair(currentTuple->getData(joinAttributeIndex),currentTuple));
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
                outputBlock->writeBlockToDisk(sortedRelationFile);
                memoryManager->clearBlock(outputBlock);
                outputBlock->addTuple(it->second);
            }
        }
        outputBlock->writeBlockToDisk(sortedRelationFile);
        //free memory
        memoryManager->deleteBlock(outputBlock);
        for(auto blockToUnload : blocksToMerge) {
            memoryManager->deleteBlock(blockToUnload);
        }
        sortedTuples.clear();

    }


    return sortedRelationFile;
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::join(Relation* left, Relation* right, int leftJoinAttributeIndex, int rightJoinAttributeIndex,string outputFile) {

    //actual joining
    std::string sortedLeftFile = twoPassMultiwayMergeSort(left,leftJoinAttributeIndex);
    std::string sortedRightFile = twoPassMultiwayMergeSort(right, rightJoinAttributeIndex);
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
    Relation sortedLeftRelation = Relation(sortedLeftFile,memoryManager);
    Relation sortedRightRelation = Relation(sortedRightFile,memoryManager);
    auto leftReader = sortedLeftRelation.getReader();
    auto rightReader = sortedRightRelation.getReader();

    Block* outputBlock = memoryManager->allocateEmptyBlock();
    while(all_of(leftReader,rightReader,[](BlockReader* reader){reader->hasNext();})) {
        Block *loadedLeftBlock = leftReader->nextBlock();
        vector<Block *> loadedLeftBlocks;
        loadedLeftBlocks.push_back(loadedLeftBlock);
        Block *loadedRightBlock = rightReader->nextBlock();
        vector<Block *> loadedRightBlocks;
        loadedRightBlocks.push_back(loadedRightBlock);
        vector<Tuple *> leftTuples = loadedLeftBlock->getTuples();
        vector<Tuple *> rightTuples = loadedRightBlock->getTuples();

        //auto leftTupleIterator = leftTuples.begin();
        //auto rightTupleIterator = rightTuples.begin();

        //if(leftTupleIterator->getData(leftJoinAttributeIndex)<rightTupleIterator->getData(rightJoinAttributeIndex))

        while(!leftTuples.empty() && !rightTuples.empty()) {
            std::string smallestJoinAttributeLeft = leftTuples.front()->getData(rightJoinAttributeIndex);
            std::string smallestJoinAttributeRight = rightTuples.front()->getData(rightJoinAttributeIndex);

            int compareValue = smallestJoinAttributeLeft.compare(smallestJoinAttributeRight);
            // equal 0; first char smaller or string shorter <0; first char greater or string longer >0
            if ( compareValue == 0){
                auto rightTupleIterator = rightTuples.begin();
                bool joinPartnerAvailable = true;
                while(joinPartnerAvailable) {
                    joinTuples(outputFile, outputBlock, leftTuples.front(), *rightTupleIterator);
                    if (rightTupleIterator == rightTuples.end()){

                    }
                    if(*rightTupleIterator != *(rightTupleIterator+1)) {
                        break;
                    }


                }
            } else if (compareValue <0 ) {
                leftTuples.erase(leftTuples.begin());

            }
        }

        /*joinStringTupleIndex leftIndex;
        joinStringTupleIndex rightIndex;
        for (auto &currentTuple : loadedLeftBlock->getTuples()) {
            leftIndex.insert(std::make_pair(currentTuple->getData(leftJoinAttributeIndex), currentTuple));
        }
        for (auto &currentTuple : loadedRightBlock->getTuples()) {
            rightIndex.insert(std::make_pair(currentTuple->getData(rightJoinAttributeIndex), currentTuple));
        }
*/

    }
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::joinTuples(const string &outputFile, Block *outputBlock,
                                                         Tuple* &leftTuple, Tuple* &rightTuple) const {
    vector<string> joinedTupleStrings;
    vector<string> leftTupleStrings = leftTuple->tupleToStringVector();
    vector<string> rightTupleStrings = rightTuple->tupleToStringVector();
    joinedTupleStrings.reserve(leftTupleStrings.size()+rightTupleStrings.size());
    joinedTupleStrings.insert(joinedTupleStrings.end(),leftTupleStrings.begin(),leftTupleStrings.end());
    joinedTupleStrings.insert(joinedTupleStrings.end(),rightTupleStrings.begin(),rightTupleStrings.end());
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