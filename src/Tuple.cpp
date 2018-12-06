#include "Tuple.h"

using namespace std;

Tuple::Tuple(string data) {
	tupleData = cutStringAtDelimiter(data,delimiter);
}

Tuple::Tuple(vector<string> data) {
	tupleData = data;
}

void Tuple::printData() {
	for (int i = 0; i < tupleData.size(); i++) {
		cout << tupleData.at(i) << " ";
	}
	cout << endl;
}

string Tuple::getData(int idx) {
	return tupleData.at(idx);
}


int Tuple::getAttributeCount() {
	return tupleData.size();
}

int Tuple::getSizeInBytes() {
	int mySize = 0;
	for (int i = 0; i < tupleData.size(); i++) {
		mySize += tupleData.at(i).capacity();
	}

	return mySize;
}

string Tuple::toWritableForm() {
	stringstream output;
	bool firstrun = true;
	for(int i=0; i<this->getAttributeCount(); i++) {
		if (firstrun) {
			output << this->getData(i);
			firstrun = false;
		}
		else {
			output << delimiter << this->getData(i);
		}
	}
	output << '\n';
	return output.str();
}

vector<string> Tuple::tupleToStringVector() {
	vector<string> outputVector;
	for (int i = 0; i < this->getAttributeCount(); ++i) {
		outputVector.push_back(this->getData(i));
	}
	return outputVector;
}
