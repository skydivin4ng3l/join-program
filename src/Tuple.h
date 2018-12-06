#ifndef TUPLE
#define TUPLE

#include <string>
#include <iostream>
#include <vector>
#include <array>
#include <sstream>
#include <map>
#include <cstring>
#include <algorithm>
#include "utils.h"

template<typename T>
	struct wrapper {
		typedef map<string, T> stringTupleMap;
	};

using namespace std;


class Tuple {
    friend class MemoryManager;
    friend class Block;
	public:
		//prints the content of this tuple
		void printData();
		//returns the number of Attributes in this tuple
		int getAttributeCount();
		//returns the data at the specified index
		string getData(int);
		//returns the size of this tuple in Bytes
		int getSizeInBytes();
		//returns the tuple data as a vector of strings
		vector<string> tupleToStringVector();

private:
		Tuple(vector<string>);
		//creates a tuple object from the specified raw input string, where fields are separated by the delimiter character
		Tuple(string);
		vector<string> tupleData;
		//returns string representation of own content that can be written to file
		string toWritableForm();
		char delimiter = ',';
};

#endif