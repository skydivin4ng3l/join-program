//
// Created by leon on 11/13/18.
//


#include "SimpleSortBasedTwoPassEquiJoinAlgorithm.h"

SimpleSortBasedTwoPassEquiJoinAlgorithm::SimpleSortBasedTwoPassEquiJoinAlgorithm(MemoryManager* memoryManager) {
    this->memoryManager = memoryManager;
}

std::string SimpleSortBasedTwoPassEquiJoinAlgorithm::twoPassMultiwayMergeSort(Relation *relation, int joinAttributeIndex){
    joinStringTupleIndex indexJoinAttributeToTuple;
    std::vector<Block*> loadedRelationChunk;
    loadedRelationChunk.resize(0);
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
        for(Block* blockToUnload : loadedRelationChunk) {
            memoryManager->deleteBlockOnly(blockToUnload);
        }
        indexJoinAttributeToTuple.clear();
        partialSortedFileNumber++;
        //insert the sorted partial file into a data structure for further processing
        Relation partialRelation = Relation(partialSortedFileName,memoryManager);
        partialFilesReaders.push_back(partialRelation.getReader());
    }

    //read and merge all sorted partial files
    while (any_of(partialFilesReaders.begin(), partialFilesReaders.end(), [](BlockReader* reader){ return reader->hasNext();})){
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
            memoryManager->deleteBlockOnly(blockToUnload);
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

    vector<Block *> loadedLeftBlocks;
    vector<Block *> loadedRightBlocks;
    joinStringTupleIndex leftIndex;
    joinStringTupleIndex rightIndex;

    Block* outputBlock = memoryManager->allocateEmptyBlock();
    while(leftReader->hasNext() && rightReader->hasNext()) {
        Block *loadedLeftBlock = leftReader->nextBlock();
        loadedLeftBlocks.push_back(loadedLeftBlock);
        Block *loadedRightBlock = rightReader->nextBlock();
        loadedRightBlocks.push_back(loadedRightBlock);

        fillBufferIndex(leftJoinAttributeIndex, loadedLeftBlock, leftIndex);
        fillBufferIndex(rightJoinAttributeIndex, loadedRightBlock, rightIndex);
        
        while (!rightIndex.empty() || !leftIndex.empty()) {
            auto smallestLeft = leftIndex.begin();
            auto smallestRight = rightIndex.begin();
            int compareValue = smallestLeft->first.compare(smallestRight->first);
            if (compareValue == 0) { // equal 0; first char smaller or string shorter <0; first char greater or string longer >0
                //preload all left/right tuples with same join attribute so we can join them all together and then delete them on both sides
                while (smallestRight->first == (rightIndex.end()--)->first && rightReader->hasNext()) { //TODO What if the Memory is not enough?
                    //fill buffer
                    Block *postLoadedRightBlock = rightReader->nextBlock();
                    loadedRightBlocks.push_back(postLoadedRightBlock);
                    fillBufferIndex(rightJoinAttributeIndex, postLoadedRightBlock, rightIndex);
                }
                while (smallestLeft->first == (leftIndex.end()--)->first && leftReader->hasNext()) { //TODO What if the Memory is not enough?
                    //fill buffer
                    Block *postLoadedLeftBlock = leftReader->nextBlock();
                    loadedLeftBlocks.push_back(postLoadedLeftBlock);
                    fillBufferIndex(leftJoinAttributeIndex, postLoadedLeftBlock, leftIndex);
                }
                auto rightRange = rightIndex.equal_range(smallestRight->first);
                auto leftRange = leftIndex.equal_range(smallestLeft->first);
                for (auto leftIt = leftRange.first; leftIt != leftRange.second; ++leftIt) {
                    for (auto rightIt = rightRange.first; rightIt != rightRange.second; ++rightIt) {
                        joinTuples(outputFile,outputBlock,leftIt->second, rightIt->second );
                    }
                }
                removeTuplesWithSameJoinAttribute(loadedLeftBlocks, leftIndex, smallestLeft);
                removeTuplesWithSameJoinAttribute(loadedRightBlocks, rightIndex, smallestRight);
            } else if (compareValue < 0) { // first char smaller or string shorter <0;
                //there is no join partner for the smallest left therefore delete these tuples and free memory if block is empty
                removeTuplesWithSameJoinAttribute(loadedLeftBlocks, leftIndex, smallestLeft);
            } else if (compareValue > 0) { // first char greater or string longer >0
                //there is no join partner for the smallest right therefore delete these tuples and free memory if block is empty
                removeTuplesWithSameJoinAttribute(loadedRightBlocks, rightIndex, smallestRight);
            }
        }
    }
    outputBlock->writeBlockToDisk(outputFile);
    memoryManager->deleteBlock(outputBlock);
    for (auto currentBlock : loadedLeftBlocks){
        memoryManager->deleteBlock(currentBlock);
    }
    for (auto currentBlock : loadedRightBlocks){
        memoryManager->deleteBlock(currentBlock);
    }
    leftIndex.clear();
    rightIndex.clear();
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::removeTuplesWithSameJoinAttribute(vector<Block *> &loadedBlocks,
                                                                                joinStringTupleIndex &indexStructure,
                                                                                const multimap<std::basic_string<char, std::char_traits<char>, std::allocator<char>>, Tuple *, std::less<std::basic_string<char, std::char_traits<char>, std::allocator<char>>>, std::allocator<std::pair<std::basic_string<char, std::char_traits<char>, std::allocator<char>>, Tuple *>>>::iterator &smallestIterator) {
    auto range = indexStructure.equal_range(smallestIterator->first);
    for (auto it = range.first; it != range.second; ++it){
                    memoryManager->deleteTuple(it->second);
                    indexStructure.erase(it);
                }
    for (auto currentBlock : loadedBlocks){
                    if (currentBlock->getCurrentSizeBytes() == 0) {
                        memoryManager->deleteBlock(currentBlock);
                    }
                }
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::fillBufferIndex(int JoinAttributeIndex, Block *loadedBlock,
                                                              joinStringTupleIndex &indexStructure) const {
    for (auto &currentTuple : loadedBlock->getTuples()) {
            indexStructure.insert(make_pair(currentTuple->getData(JoinAttributeIndex), currentTuple));
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