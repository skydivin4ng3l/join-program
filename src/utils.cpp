#include "utils.h"

vector<string> cutStringAtDelimiter(string origin, char delimiter) {
	stringstream dataStream;
	dataStream.str(origin);
	string buffer = "";
	vector<string> cutData;
	while (getline(dataStream, buffer, delimiter)) {
		cutData.push_back(buffer);
	}
	return cutData;
}
