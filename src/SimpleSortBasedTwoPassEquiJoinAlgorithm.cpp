//
// Created by leon on 11/13/18.
//


#include "SimpleSortBasedTwoPassEquiJoinAlgorithm.h"
#include <assert.h>

SimpleSortBasedTwoPassEquiJoinAlgorithm::SimpleSortBasedTwoPassEquiJoinAlgorithm(MemoryManager* memoryManager) {
    this->memoryManager = memoryManager;
}

std::string SimpleSortBasedTwoPassEquiJoinAlgorithm::twoPassMultiwayMergeSort(Relation *relation, int joinAttributeIndex){
    // create new partial sort file
    // load the memory full with blocks and put them into a "self-sorting" multimap pointer data structure
    // put them back into blocks in sorted order
    // print them to this file,
    // release Memory, add filename to string vector
    // repeat until relation complete

    std::hash<std::string> hash_fn;
    std::size_t hashedFileRelationFileName = hash_fn(relation->getFile());
    std::stringstream ss;
    ss << "Relation_" << hashedFileRelationFileName << "_SortedBy_"<< joinAttributeIndex << "_JoinAttribute.csv";
    std::string sortedRelationFile = ss.str();
    //deletes output from previous runs:
    remove(sortedRelationFile.c_str());

    joinStringTupleIndex indexJoinAttributeToTuple;
    std::vector<Block*> loadedRelationChunk;
    loadedRelationChunk.resize(0);
    std::vector<BlockReader*> partialFilesReaders;
    std::vector<std::string> partialFiles;
    int partialSortedFileNumber = 0;

    BlockReader *relationReader = relation->getReader();

    while(relationReader->hasNext()){
        std::string partialSortedFileName = "partialSorted_"+std::to_string(hashedFileRelationFileName)+"_part_"+ std::to_string(partialSortedFileNumber)+".csv";
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
        //write the ordered chuck to disk
        for(auto it=indexJoinAttributeToTuple.begin(); it !=indexJoinAttributeToTuple.end(); it++){
            if (outputBlock->addTuple(it->second)) {/*Tuple will be added*/} else {
                //full Block will be written to disc then wiped then tuple added
                outputBlock->writeBlockToDisk(partialSortedFileName);
                memoryManager->clearBlock(outputBlock);
                outputBlock->addTuple(it->second);
            }
        }
        //write the last block containing the last added tuples to disc
        outputBlock->writeBlockToDisk(partialSortedFileName);
        //free memory
        memoryManager->deleteBlock(outputBlock);
        indexJoinAttributeToTuple.clear();
        for(Block* blockToUnload : loadedRelationChunk) {
            memoryManager->deleteBlockOnly(blockToUnload);//why can't we delete the block with the tuples, they should have been written to the disk
        }
        loadedRelationChunk.clear();
        partialSortedFileNumber++;
        //insert the sorted partial file into a data structure for further processing
        insertPartialSortedFileIntoDataStructure(partialFilesReaders, partialFiles, partialSortedFileName);
    }

    //read and merge all sorted partial files
    Block* outputBlock = memoryManager->allocateEmptyBlock();
    //memoryManager->printStatus();
    vector<BlockReader*> processableChunkOfFileReaders;
    int availableFreeBlocks = memoryManager->getNumFreeBlocks();
    //split amount of files so for each file one block can be created for merging process
    while (partialFilesReaders.size() > availableFreeBlocks) {
        //create a new file for partial merged content
        static int filecount = 0;
        std::stringstream ss;
        ss << "Partially_Merged_Relation_" << hashedFileRelationFileName << "_SortedBy_"<< joinAttributeIndex << "_JoinAttribute_part_"<< filecount  <<".csv";
        string partiallyMergedSortedRelationFile = ss.str();
        //deletes output from previous runs:
        remove(partiallyMergedSortedRelationFile.c_str());

        //move processable amount of readers into the process chain
        processableChunkOfFileReaders.insert(processableChunkOfFileReaders.begin(),partialFilesReaders.begin(),partialFilesReaders.begin()+availableFreeBlocks);
        partialFilesReaders.erase(partialFilesReaders.begin(),partialFilesReaders.begin()+availableFreeBlocks);

        //merge the files of processableChunk vector into partiallyMergedSortedRelationFile
        mergeSortedFilesIntoFile(processableChunkOfFileReaders, partiallyMergedSortedRelationFile, outputBlock,
                                 joinAttributeIndex);
        insertPartialSortedFileIntoDataStructure(partialFilesReaders, partialFiles, partiallyMergedSortedRelationFile);
        filecount++;
        processableChunkOfFileReaders.clear();
    }
    memoryManager->clearBlock(outputBlock);
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
        joinStringTupleBlockStatIndex sortedTuples;
        //read one block of each file and fill the tuple into a merge DataStructure
        for(auto reader : processableChunkOfFileReaders){
            if (reader->hasNext()) {
                Block *loadedBlock = reader->nextBlock();
                relationStatistics *thisRelationStatPointer = new relationStatistics(reader,loadedBlock);
                loadTuplesFromBlockIntoDataStructure(loadedBlock, joinAttributeIndex, sortedTuples,
                                                     thisRelationStatPointer);
            }
        }
        //memoryManager->printStatus();
        //write the merged blocks
        //if one block is empty the corresponding relation reader will refill with the next block of this relation
        while (!sortedTuples.empty()) {
            Tuple *tupleWithSmallestJoinIndex = sortedTuples.begin()->second.first;
            if (outputBlock->addTuple(tupleWithSmallestJoinIndex)) {
                cleanUpAndBufferTupleBlock(sortedTuples, joinAttributeIndex);
            } else {
                outputBlock->writeBlockToDisk(sortedRelationFile);
                memoryManager->clearBlock(outputBlock);
                outputBlock->addTuple(tupleWithSmallestJoinIndex);
                cleanUpAndBufferTupleBlock(sortedTuples, joinAttributeIndex);
            }
        }
        sortedTuples.clear();
        }
    outputBlock->writeBlockToDisk(sortedRelationFile);
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::cleanUpAndBufferTupleBlock(
        joinStringTupleBlockStatIndex &sortedTuples, int joinAttributeIndex) const {
    relationStatistics *thisRelationStatPointer = sortedTuples.begin()->second.second;
    thisRelationStatPointer->loadedTupleCount--;
    if (thisRelationStatPointer->loadedTupleCount == 0) {
        memoryManager->deleteBlockOnly(thisRelationStatPointer->loadedBlocks.front() );
        thisRelationStatPointer->loadedBlocks.pop();
        if (thisRelationStatPointer->thisBlocksReader->hasNext()) {
            Block *loadedBlock = thisRelationStatPointer->thisBlocksReader->nextBlock();
            loadTuplesFromBlockIntoDataStructure(loadedBlock, joinAttributeIndex, sortedTuples, thisRelationStatPointer);
        }
    }
    sortedTuples.erase(sortedTuples.begin());
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::loadTuplesFromBlockIntoDataStructure(Block *loadedBlock,
                                                                                   int joinAttributeIndex,
                                                                                   multimap<string, pair<Tuple *, relationStatistics *>> &sortedTuplesIndex,
                                                                                   relationStatistics *thisRelationStatPointer) const {
    if (thisRelationStatPointer->loadedBlocks.empty()) {
        thisRelationStatPointer->loadedBlocks.push(loadedBlock);
    }
    for(auto& currentTuple : loadedBlock->getTuples() ) {
                    thisRelationStatPointer->loadedTupleCount++;
                    sortedTuplesIndex.insert( make_pair(currentTuple->getData(joinAttributeIndex), make_pair(currentTuple,thisRelationStatPointer)) );
                }
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::join(Relation* left, Relation* right, int leftJoinAttributeIndex, int rightJoinAttributeIndex,string outputFile) {

    //actual joining
    std::string sortedLeftFile = twoPassMultiwayMergeSort(left,leftJoinAttributeIndex);
    //memoryManager->printStatus();
    std::string sortedRightFile = twoPassMultiwayMergeSort(right, rightJoinAttributeIndex);
    //memoryManager->printStatus();
    //TODO Phase 2: Merge R and S
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

    std::queue<Block *> *loadedLeftBlocks = new queue<Block *>();
    std::queue<Block *> *loadedRightBlocks = new queue<Block *>();
    joinStringTupleBlockStatIndex leftIndex;
    joinStringTupleBlockStatIndex rightIndex;

    Block* outputBlock = memoryManager->allocateEmptyBlock();
    while( (leftReader->hasNext() || !leftIndex.empty()) && (rightReader->hasNext() || !rightIndex.empty()) && memoryManager->getNumFreeBlocks() >= 2) {

        if (leftReader->hasNext()) {
            loadBlockIntoIndex(leftReader, leftIndex, leftJoinAttributeIndex, loadedLeftBlocks);
        }
        if (rightReader->hasNext()) {
            loadBlockIntoIndex(rightReader, rightIndex, rightJoinAttributeIndex, loadedRightBlocks);
        }

        //TODO Handle one empty index, is there a problem?
        while (!rightIndex.empty() && !leftIndex.empty()) {
            auto smallestLeft = leftIndex.begin()->first;
            auto smallestRight = rightIndex.begin()->first;
            int compareValue = smallestLeft.compare(smallestRight);
            if (compareValue == 0) { // equal 0; first char smaller or string shorter <0; first char greater or string longer >0
                //preload all left/right tuples with same join attribute so we can join them all together and then delete them on both sides
                while (smallestRight == rightIndex.rbegin()->first && rightReader->hasNext() ) { //TODO What if the Memory is not enough?
                    //fill buffer
                    if (memoryManager->getNumFreeBlocks() >= 1) {
                        loadBlockIntoIndex(rightReader, rightIndex, rightJoinAttributeIndex, loadedLeftBlocks);
                    } else {
                        std::cout << "ERROR: Not enough memory!" << endl;
                    }
                }
                while (smallestLeft == leftIndex.rbegin()->first && leftReader->hasNext() && memoryManager->getNumFreeBlocks() >= 1) { //TODO What if the Memory is not enough?
                    //fill buffer
                    if (memoryManager->getNumFreeBlocks() >= 1) {
                        loadBlockIntoIndex(leftReader, leftIndex, leftJoinAttributeIndex, loadedRightBlocks);
                    } else {
                        std::cout << "ERROR: Not enough memory!" << endl;
                    }
                }
                auto rightRange = rightIndex.equal_range(smallestRight);
                auto leftRange = leftIndex.equal_range(smallestLeft);
                for (auto leftIt = leftRange.first; leftIt != leftRange.second; ++leftIt) {
                    for (auto rightIt = rightRange.first; rightIt != rightRange.second; ++rightIt) {
                        joinTuples(outputFile,outputBlock,leftIt->second.first, rightIt->second.first );
                        //memoryManager->printStatus();
                    }
                }
                removeSmallestTuplesWithSameJoinAttribute2(leftIndex);
                removeSmallestTuplesWithSameJoinAttribute2(rightIndex);
            } else if (compareValue < 0) { // first char smaller or string shorter <0;
                //there is no join partner for the smallest left therefore delete these tuples and free memory if block is empty
                removeSmallestTuplesWithSameJoinAttribute2(leftIndex);
            } else if (compareValue > 0) { // first char greater or string longer >0
                //there is no join partner for the smallest right therefore delete these tuples and free memory if block is empty
                removeSmallestTuplesWithSameJoinAttribute2(rightIndex);
            }
        }
    }
    //print the block never the less
    outputBlock->writeBlockToDisk(outputFile);

    //cleanup
    memoryManager->deleteBlock(outputBlock);

    while (!loadedLeftBlocks->empty()) {
        memoryManager->deleteBlock(loadedLeftBlocks->front());
        loadedLeftBlocks->pop();
    }
    delete loadedLeftBlocks;
    while (!loadedRightBlocks->empty()) {
        memoryManager->deleteBlock(loadedRightBlocks->front());
        loadedRightBlocks->pop();
    }
    delete loadedRightBlocks;

    leftIndex.clear();
    rightIndex.clear();

    //comment those for partial progess files---
    remove(sortedLeftFile.c_str());
    remove(sortedRightFile.c_str());
    //------------------------------------------
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::removeSmallestTuplesWithSameJoinAttribute2(joinStringTupleBlockStatIndex &indexStructure) const {
    auto range =indexStructure.equal_range(indexStructure.begin()->first);
    for (auto it = range.first; it != range.second; ) {
        relationStatistics *thisRelationStatPointer = it->second.second ;
        thisRelationStatPointer->loadedTupleCount--;
        if (thisRelationStatPointer->loadedTupleCount == 0) {
            memoryManager->deleteBlock(thisRelationStatPointer->loadedBlocks.front() );
            thisRelationStatPointer->loadedBlocks.pop();
            delete thisRelationStatPointer;
        }
        it = indexStructure.erase(it);
    }
}

void SimpleSortBasedTwoPassEquiJoinAlgorithm::loadBlockIntoIndex(BlockReader *reader,
                                                                 joinStringTupleBlockStatIndex &indexStructure,
                                                                 int joinAttributeIndex, std::queue<Block *> *loadedBlocks) const {
    Block *loadedBlock = reader->nextBlock();
    loadedBlocks->push(loadedBlock); //additional reference for fast cleanup
    relationStatistics *thisRelationStatPointer = new relationStatistics(reader,loadedBlock);
    loadTuplesFromBlockIntoDataStructure(loadedBlock, joinAttributeIndex, indexStructure, thisRelationStatPointer);
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