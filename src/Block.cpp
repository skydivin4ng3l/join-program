#include "Block.h"

using namespace std;

Block::Block(int maxsize) {
	blocksize = maxsize;
	currentSizeInBytes = 0;
}

//
//Block::Block(int maxsize, string fpath, vector<tuple<string, char, int16_t>> scheme) {
//	blocksize = maxsize;
//	currentSizeInBytes = 0;
//	ifstream inputStream;
//	inputStream.open(fpath);
//	string buffer = "";
//	vector<string> allData;
//	int diff = 0;
//	int tmp = 0;
//	while (getline(inputStream, buffer)) {
//		allData.push_back(buffer);
//	}
//	inputStream.close();
//	ofstream outputStream;
//	outputStream.open(fpath, ios::out | ios::trunc);
//	auto extractTuples = [&](const string& row) {
//		if ((currentSizeInBytes + diff) <= blocksize) {
//			tmp = currentSizeInBytes;
//			this->addTuple(new Tuple(row, scheme));
//		}
//		else {
//			outputStream << row << '\n';
//		}
//		diff = currentSizeInBytes - tmp;
//	};
//	for_each(allData.begin(), allData.end(), extractTuples);
//	outputStream.close();
//
//	/*
//	while ((currentSizeInBytes + diff) <= blocksize) {
//		tmp = currentSizeInBytes;
//		if (getline(inputStream, buffer)) {
//			this->addTuple(Tuple(buffer, scheme));
//		}
//		else {
//			break;
//		}
//		diff = currentSizeInBytes - tmp;
//	} */
//}

bool Block::addTuple(Tuple* tuple) {
	if ((currentSizeInBytes + tuple->getSizeInBytes()) <= blocksize) {
		tuples.push_back(tuple);
		currentSizeInBytes += tuple->getSizeInBytes();
		return true;
	}
	return false;
}

vector<Tuple*> Block::getTuples() {
	return tuples;
}

int Block::getCurrentSizeBytes() {
	return currentSizeInBytes;
}

void Block::writeBlockToDisk(string file) {
	ofstream blockWriteStream;
	blockWriteStream.open(file, ios::out | ios::app);
	auto writeTuple = [&](Tuple* tup) {
		string buffer = tup->toWritableForm();
		blockWriteStream.write(buffer.c_str(), buffer.length());
	};
	for_each(tuples.begin(), tuples.end(), writeTuple);
	blockWriteStream.close();
}
