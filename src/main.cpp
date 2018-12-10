#include <iostream>
#include <stdlib.h>
#include <algorithm>
#include "Relation.h"
#include "utils.h"
#include "NestedLoopEquiJoinAlgorithm.h"
#include <stdio.h>

int main() {

	//console print optimization
	std::ios::sync_with_stdio(false);

	//feel free to change the parameters to see how your program reacts
    MemoryManager manager = MemoryManager(10,50000);

    //set these paths to the downloaded data files and pass the relations to the join-method to test with larger amounts of data
	//Relation r1 = Relation("../resources/movieSample.csv",&manager);
	//Relation r2 = Relation("../resources/plotSample.csv",&manager);

	//very small test-files
	Relation r1 = Relation("../resources/testRelation1.csv",&manager);
    Relation r2 = Relation("../resources/testRelation2.csv",&manager);

	string outputFile = "output.txt";
	//deletes output from previous runs:
	remove(outputFile.c_str());

	std::cout << "Joining Relation with size " << r1.getSize() << " and " << r2.getSize()  << " (In Bytes)" << endl;
	//calculate actor/movie join:
	NestedLoopEquiJoinAlgorithm* alg = new NestedLoopEquiJoinAlgorithm(&manager);
	alg->join(&r1,&r2,0,0,outputFile);

	return 0;
}