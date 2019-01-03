//
// Created by leon on 11/13/18.
//

#ifndef JOIN_PROGRAM_SIMPLESORTBASEDTWOPASSEQUIJOINALGORITHM_H
#define JOIN_PROGRAM_SIMPLESORTBASEDTWOPASSEQUIJOINALGORITHM_H


#include "Relation.h"
#include "MemoryManager.h"
#include <map>
#include <queue>


typedef std::multimap<std::string,Tuple*> joinStringTupleIndex;
typedef struct relationStatistics_t {
    BlockReader *thisBlocksReader;
    std::queue<Block *> loadedBlocks;
    double loadedTupleCount = 0;

    relationStatistics_t(BlockReader* reader,Block *block, double count = 0): thisBlocksReader(reader),loadedTupleCount(count){
        loadedBlocks.push(block);
    }
} relationStatistics;

class SimpleSortBasedTwoPassEquiJoinAlgorithm {

public:
    //constructor that passes the reference to the Memory Manager
    SimpleSortBasedTwoPassEquiJoinAlgorithm(MemoryManager* memoryManager);
    //reference to the memory manager that keeps track of the used memory
    MemoryManager* memoryManager;
    /*joins the two Relations 'left' and 'right' via an equi-join.
     * The third and fourth parameter are the indices of the attributes that are required to be equal.
     * In other words: Two tuples t1,t2 join if t1[leftJoinAttributeIndex] == t2[rightJoinAttributeIndex] is true, where t1 is from Relation left and t2 is from Relation right.
     * The join-Result includes ALL original columns, including BOTH columns that are equal.
     * The resulting tuples should be written to the outputFile.
     * */
    void join(Relation* left, Relation* right, int leftJoinAttributeIndex, int rightJoinAttributeIndex,string outputFile);

private:
    std::string twoPassMultiwayMergeSort(Relation *relation, int joinAttributeIndex);

    void
    joinTuples(const string &outputFile, Block *outputBlock, Tuple *& leftTuples, Tuple *& rightTuples) const;

    void fillBufferIndex(int JoinAttributeIndex, Block *loadedBlock, joinStringTupleIndex &indexStructure) const;

    void removeTuplesWithSameJoinAttribute(vector<Block *> &loadedBlocks,
                                               joinStringTupleIndex &indexStructure,
                                               const multimap<std::basic_string<char, std::char_traits<char>, std::allocator<char>>, Tuple *, std::less<std::basic_string<char, std::char_traits<char>, std::allocator<char>>>, std::allocator<std::pair<std::basic_string<char, std::char_traits<char>, std::allocator<char>>, Tuple *>>>::iterator &smallestIterator,
                                               int joinAttributeIndex);

    void mergeSortedFilesIntoFile(vector<BlockReader *> &processableChunkOfFileReaders,
                                      const string &sortedRelationFile, Block *outputBlock,
                                      int joinAttributeIndex) const;

    void insertPartialSortedFileIntoDataStructure(vector<BlockReader *> &partialFilesReaders, vector<string> &partialFiles,
                                                  const string &partialSortedFileName) const;

    void loadTuplesFromBlockIntoMergeDataStructure(Block *loadedBlock, int joinAttributeIndex,
                                                   multimap<string, pair<Tuple *, relationStatistics_t *>> &sortedTuples,
                                                   relationStatistics_t *thisRelationStatPointer) const;
    //decrease loaded Tuple Count, remove first element from DataStructure; if TupleCount is 0: read next Block and add to DataStructure
    void cleanUpAndBufferTupleBlock(multimap<string, pair<Tuple *, relationStatistics *>> &sortedTuples, int joinAttributeIndex) const;
};


#endif //JOIN_PROGRAM_SIMPLESORTBASEDTWOPASSEQUIJOINALGORITHM_H
