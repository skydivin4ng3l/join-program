//
// Created by leon on 11/13/18.
//


#include "SimpleSortBasedTwoPassEquiJoinAlgorithm.h"
#include <assert.h>

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
    ss << "Relation_" << hashedFileRelationFileName << "_SortedBy_"<< joinAttributeIndex << "_JoinAttribute.csv";
    std::string sortedRelationFile = ss.str();
    //deletes output from previous runs:
    remove(sortedRelationFile.c_str());
    std::vector<BlockReader*> partialFilesReaders;
    std::vector<std::string> partialFiles;
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
        std::string partialSortedFileName = "partialSorted_"+ std::to_string(partialSortedFileNumber)+".csv";
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
            memoryManager->deleteBlockOnly(blockToUnload);//why do we here just delete the block the tuples should have been written to the disk
        }
        loadedRelationChunk.clear();
        indexJoinAttributeToTuple.clear();
        partialSortedFileNumber++;
        //insert the sorted partial file into a data structure for further processing
        insertPartialSortedFileIntoDataStructure(partialFilesReaders, partialFiles, partialSortedFileName);
    }

    //read and merge all sorted partial files
    Block* outputBlock = memoryManager->allocateEmptyBlock();
    memoryManager->printStatus();
    vector<BlockReader*> processableChunkOfFileReaders;
    int availableFreeBlocks = memoryManager->getNumFreeBlocks();
    while (partialFilesReaders.size() > availableFreeBlocks) {
        static int filecount = 0;
        //move possible amount of readers into the process chain
        processableChunkOfFileReaders.insert(processableChunkOfFileReaders.begin(),partialFilesReaders.begin(),partialFilesReaders.begin()+availableFreeBlocks);
        partialFilesReaders.erase(partialFilesReaders.begin(),partialFilesReaders.begin()+availableFreeBlocks);
        //create a new file for partial merged content
        std::stringstream ss;
        ss << "Partially_Merged_Relation_" << hashedFileRelationFileName << "_SortedBy_"<< joinAttributeIndex << "_JoinAttribute_part_"<< filecount  <<".csv";
        string partiallyMergedSortedRelationFile = ss.str();
        //deletes output from previous runs:
        remove(partiallyMergedSortedRelationFile.c_str());
        //merge the files of processableChunk vector into partiallyMergedSortedRelationFile
        mergeSortedFilesIntoFile(processableChunkOfFileReaders, partiallyMergedSortedRelationFile, outputBlock,
                                 joinAttributeIndex);
        insertPartialSortedFileIntoDataStructure(partialFilesReaders, partialFiles, partiallyMergedSortedRelationFile);
        filecount++;
        processableChunkOfFileReaders.clear();
    }
    mergeSortedFilesIntoFile(partialFilesReaders, sortedRelationFile, outputBlock, joinAttributeIndex);
    //free memory
    memoryManager->deleteBlock(outputBlock);
    partialFilesReaders.clear();

    //comment this for partial progress files------
    for(auto fileName : partialFiles) {
        remove(fileName.c_str());
    }
    partialFiles.clear();
    //---------------------------------------------

    return sortedRelationFile;
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::insertPartialSortedFileIntoDataStructure(
        vector<BlockReader *> &partialFilesReaders, vector<string> &partialFiles, const string &partialSortedFileName) const {
    Relation partialRelation = Relation(partialSortedFileName, memoryManager);
    partialFilesReaders.push_back(partialRelation.getReader());
    partialFiles.push_back(partialSortedFileName);
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::mergeSortedFilesIntoFile(vector<BlockReader *> &processableChunkOfFileReaders,
                                                                       const string &sortedRelationFile, Block *outputBlock,
                                                                       int joinAttributeIndex) const {
    assert(memoryManager->getNumFreeBlocks() >= processableChunkOfFileReaders.size());
    while (any_of(processableChunkOfFileReaders.begin(), processableChunkOfFileReaders.end(), [](BlockReader* reader){ return reader->hasNext();}) ) {
            memoryManager->printStatus();
            vector<Block*> blocksToMerge;
            multimap<string,Tuple*> sortedTuples;
            //read one block of each file and merge them
            for(auto reader : processableChunkOfFileReaders){
                if (reader->hasNext()) {
                    Block* loadedBlock = reader->nextBlock();
                    blocksToMerge.push_back(loadedBlock);
                    for(auto& currentTuple : loadedBlock->getTuples() ) {
                        sortedTuples.insert( make_pair(currentTuple->getData(joinAttributeIndex),currentTuple));
                    }
                }
            }
            //write the merged blocks
            for(auto it=sortedTuples.begin(); it !=sortedTuples.end(); it++){
                if (outputBlock->addTuple(it->second)) {} else {
                    outputBlock->writeBlockToDisk(sortedRelationFile);
                    memoryManager->clearBlock(outputBlock);
                    outputBlock->addTuple(it->second);
                }
            }
            //delete the origin blocks and structure
            for(auto blockToUnload : blocksToMerge) {
                memoryManager->deleteBlockOnly(blockToUnload);
            }
            blocksToMerge.clear();
            sortedTuples.clear();
        }
    outputBlock->writeBlockToDisk(sortedRelationFile);
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
        //TODO Handle one empty index, is there a problem?
        while (!rightIndex.empty() && !leftIndex.empty()) {
            auto smallestLeft = leftIndex.begin();
            auto smallestRight = rightIndex.begin();
            int compareValue = smallestLeft->first.compare(smallestRight->first);
            if (compareValue == 0) { // equal 0; first char smaller or string shorter <0; first char greater or string longer >0
                //preload all left/right tuples with same join attribute so we can join them all together and then delete them on both sides
                while (smallestRight->first == rightIndex.rbegin()->first && rightReader->hasNext() && memoryManager->getNumFreeBlocks() >= 1) { //TODO What if the Memory is not enough?
                    //fill buffer
                    Block *postLoadedRightBlock = rightReader->nextBlock();
                    loadedRightBlocks.push_back(postLoadedRightBlock);
                    fillBufferIndex(rightJoinAttributeIndex, postLoadedRightBlock, rightIndex);
                }
                while (smallestLeft->first == leftIndex.rbegin()->first && leftReader->hasNext() && memoryManager->getNumFreeBlocks() >= 1) { //TODO What if the Memory is not enough?
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
                //TODO how to delete tuples / blocks that are not yet printed to the outputfile but we need to get rid of the blocks and tuples
                //may be we here should have a custom only block deletion Funktion instead of reusing the removeTuplesWithSameJoinAttribute funktion
                //cause we know we will need the tuples of the output block, but how to delete the tuples after we printed them, will this be done by the deltion/clearance of the outputblock?
                /*auto range = leftIndex.equal_range(smallestLeft->first);
                for (auto it = range.first; it != range.second; ++it){
                    leftIndex.erase(it);
                }*/
                removeTuplesWithSameJoinAttribute(loadedLeftBlocks, leftIndex, smallestLeft, leftJoinAttributeIndex);
                removeTuplesWithSameJoinAttribute(loadedRightBlocks, rightIndex, smallestRight, rightJoinAttributeIndex);
            } else if (compareValue < 0) { // first char smaller or string shorter <0;
                //there is no join partner for the smallest left therefore delete these tuples and free memory if block is empty
                removeTuplesWithSameJoinAttribute(loadedLeftBlocks, leftIndex, smallestLeft, leftJoinAttributeIndex);
            } else if (compareValue > 0) { // first char greater or string longer >0
                //there is no join partner for the smallest right therefore delete these tuples and free memory if block is empty
                removeTuplesWithSameJoinAttribute(loadedRightBlocks, rightIndex, smallestRight, rightJoinAttributeIndex);
            }
        }
    }
    //print the block never the less
    outputBlock->writeBlockToDisk(outputFile);

    //cleanup
    memoryManager->deleteBlock(outputBlock);

    for (auto currentBlock : loadedLeftBlocks){
        memoryManager->deleteBlock(currentBlock);
    }
    loadedLeftBlocks.clear();

    for (auto currentBlock : loadedRightBlocks){
        memoryManager->deleteBlock(currentBlock);
    }
    loadedRightBlocks.clear();

    leftIndex.clear();
    rightIndex.clear();

    //comment those for partial progess files---
    remove(sortedLeftFile.c_str());
    remove(sortedRightFile.c_str());
    //------------------------------------------
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::removeTuplesWithSameJoinAttribute(vector<Block *> &loadedBlocks,
                                                                                joinStringTupleIndex &indexStructure,
                                                                                const multimap<std::basic_string<char, std::char_traits<char>, std::allocator<char>>, Tuple *, std::less<std::basic_string<char, std::char_traits<char>, std::allocator<char>>>, std::allocator<std::pair<std::basic_string<char, std::char_traits<char>, std::allocator<char>>, Tuple *>>>::iterator &smallestIterator,
                                                                                int joinAttributeIndex) {
    //delete Tuples from loaded Blocks
    /*auto range = indexStructure.equal_range(smallestIterator->first);
    for (auto it = range.first; it != range.second; ++it){
                    memoryManager->deleteTuple(it->second);
                    //indexStructure.erase(it);
    }*/
    //delete all tuple entries in index structure with key
    indexStructure.erase(smallestIterator->first);
    //delete loaded blocks if they contain only already processed Tuples
    //vector<Block *> blocksToDelete;
    for (auto block_It = loadedBlocks.begin() ; block_It != loadedBlocks.end(); ){
        vector<Tuple *> currentTuples = (*block_It)->getTuples();
        auto alreadyProcessedTuple = [&](Tuple * tuple) -> bool {
            std::string currentTupleJoinAttribute = tuple->getData(joinAttributeIndex);
            int compareValue = currentTupleJoinAttribute.compare(smallestIterator->first);
            //equal: 0 ; string shorter or first char smaller <0
            if (compareValue <= 0 ) {
                return true;
            }
            return false;
        };
        //delete currentBlock if it only contains tuples which join attribute is equal or smaller than the currently processed joinAttribute
        if (all_of(currentTuples.begin(),currentTuples.end(), alreadyProcessedTuple ) ){
            //blocksToDelete.push_back(currentBlock);
            memoryManager->deleteBlock(*block_It);
            block_It = loadedBlocks.erase(block_It);
        } else {
            block_It++;
        }
    }
    // clean up dirty cache
    //loadedBlocks.erase(std::remove_if(loadedBlocks.begin(),loadedBlocks.end(), [&](Block* block) -> bool {return block->getCurrentSizeBytes() == 0; } ), loadedBlocks.end());
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