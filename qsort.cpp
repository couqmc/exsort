/* Our program has two phases. In the first phase, we divide the input file into
 * "runs" and sort those runs in memory. In the second phase, we merge those runs
 * into a single sorted output file */
#include "qsort.h"
int phase1(char * inputFileName)
{
	std::ofstream tempFile;	/* fstream for a temporary file */
	std::string name;
	std::ifstream inputFile; /* fstream for the input file */
	Buffer * inBuf;          /* input buffer */
	Buffer * outBuf;         /* output buffer */
	int index = 0;           /* indicate which run we are dealing with*/
	char * bufferEnd;        /* end position of the buffer */
	char * recordEnd;        /* end position of the record */
	char * recordStart;      /* start position of the record */
	recType rec;
	std::vector<recType> recordList; /* the list that stores all records in one run to sort*/
	int inBufferSize  = 200000040; /* the size of the input buffer which determine 
	                                * the size of each run */
	int outBufferSize = 20000000; /* the size of the output buffer */
	partialRecord currentParRec;  /* partial record for the current buffer */
	partialRecord nextParRec;     /* partial record for the next buffer */
	currentParRec.partial = false;
	nextParRec.partial = false;
	/* initialize buffers */
	outBuf = new Buffer;
	inBuf = new Buffer;
	inBuf->buffer = new char[inBufferSize];
	outBuf->buffer = new char[outBufferSize];
	
	inputFile.open(inputFileName,std::ios::binary);
	if (!inputFile.is_open())
	{
		std::cout<<"error opening input file"<<std::endl;
		exit(-1);
	}

	while(1)
	{
		/* we read next chunk of records to the input buffer */
		inputFile.read(inBuf->buffer,inBufferSize);
		/* if we have run out of records then we exit */
		if ((inputFile.eof() == true) && (inputFile.gcount() == 0))
		{
			break;
		}
		//std::cout<<"index "<<index<<std::endl;
		inBuf->bufferSize = inputFile.gcount();
		/*now we get records from the input buffer
		 * to the RecordList to sort */
		bufferEnd = inBuf->buffer+inBuf->bufferSize-1;
		if (nextParRec.partial == true ) /* if we hava a partial record */
		{
			/* copy the rest of the record to currentParRec*/
			currentParRec = nextParRec;
			recordEnd = (char *)std::memchr(inBuf->buffer,'\n',inBuf->bufferSize);		
			std::memcpy(currentParRec.data + currentParRec.size,inBuf->buffer, recordEnd - inBuf->buffer + 1);
			currentParRec.size += recordEnd - inBuf->buffer + 1;
			/* put the record to the RecordList*/
			rec.size = currentParRec.size  ;
			rec.data = currentParRec.data;
			recordList.push_back(rec);
			nextParRec.partial = false;
			/* if we have reached to the end of the buffer
			 * contiunue to the next iteration */
			if (recordEnd == bufferEnd  )
			{
				continue;
			}
			else /* determine the position of the next record */
			{
				recordStart = recordEnd+1;		
			}
		}
		/*we have a full record at the beginning of the buffer*/
		else 
		{
			recordStart = inBuf->buffer;
		}
		/*this loop gets all the remaining records of the buffer*/
		while(1)
		{
			recordEnd = (char*)std::memchr(recordStart,'\n',bufferEnd-recordStart+1);
			if (recordEnd == NULL) /* we have a partial record */
			{
				nextParRec.size = bufferEnd-recordStart+1;
				std::memcpy(nextParRec.data,recordStart,nextParRec.size);
				nextParRec.partial = true;
				break;
			}
			else /* we don't have a partial record */
			{
				/* we have reached to the end of the buffer */
				if (recordEnd == bufferEnd)
				{
					/* put the record to the RecordList */
					rec.size = recordEnd-recordStart+1;
					rec.data = recordStart;
					recordList.push_back(rec);
					break;
				}
				else /* we have not reached to the end of the buffer */
				{
					/* put the record to the RecordList */
					rec.size = recordEnd-recordStart+1;
					rec.data = recordStart;
					recordList.push_back(rec);
					/* determine the position of the next record */
					recordStart = recordEnd+1;
				}
			}
		}
		/* now we hava all the records in the RecordList
		 * we sort them and write to a temporary file */
		outBuf->bufferLength = 0;
		/*sort the records*/
		std::sort(recordList.begin(),recordList.end());
		/*open a temporary file to write*/
		name = "temp" + std::to_string(index);
		tempFile.open(name,std::ios::binary);
		if (!tempFile.is_open())
		{
			std::cout<<"error opening temporary file"<<std::endl;
			exit(-1);
		}
		for (int i = 0; i < recordList.size(); i++)
		{
			/*write records to buffer*/
			std::memcpy(outBuf->buffer+outBuf->bufferLength,recordList[i].data,recordList[i].size);
			outBuf->bufferLength += recordList[i].size;
			/*write buffer to file*/
			if (outBuf->bufferLength > (outBufferSize - MAX_LENGTH))
			{
				tempFile.write(outBuf->buffer,outBuf->bufferLength);
				outBuf->bufferLength = 0;
			}
		}
		/* write the last records of the buffer (if any) to the output file */
		if (outBuf->bufferLength > 0)
		{
			tempFile.write(outBuf->buffer,outBuf->bufferLength);
		}
		recordList.clear(); /* clear the recordList*/
		tempFile.close();
		index++;
	}
	delete[] inBuf->buffer;
	delete[] outBuf->buffer;
	inputFile.close();
	return index; /* return number of runs */
}

void phase2(char* outputFileName, int numberOfFiles)
{	
	PhaseTwoInfo * info;
	int bufferSize = 30000000; /* size of the input buffers*/
	std::string tempName = "temp";
	char * recordStart; /* start position of the record */
	char * recordEnd;   /* end position of the record */
	recType rec;
	int index; /* indicate which run we are dealing with */
	
	/* initialize struct info */
	info = new PhaseTwoInfo;
	info->numberOfFiles = numberOfFiles;
	info->inputFile = new std::ifstream *[numberOfFiles];
	info->inputBufferArray = new Buffer*[numberOfFiles];
	info->parRec = new partialRecord*[numberOfFiles];
	info->currentRecord = new int[numberOfFiles];
	info->endOfBuffer = new bool[numberOfFiles];
	info->eof = new bool[numberOfFiles];
	info->outputFile = new std::ofstream;
	info->outputFile->open(outputFileName,std::ios::binary);
	if (!info->outputFile->is_open())
		{
			std::cout<<"error opening output file"<<std::endl;
			exit(-1);
		}
	info->outputBuffer = new Buffer;
	info->outputBuffer->buffer = new char[bufferSize+MAX_LENGTH];
	info->outputBuffer->bufferSize = bufferSize+MAX_LENGTH;
	info->outputBuffer->bufferLength = 0;
	info->bufferSize = bufferSize;
	for (int i = 0; i < numberOfFiles; i++)
	{
		info->parRec[i] = new partialRecord;
		info->parRec[i]->partial = false;
		info->inputBufferArray[i] = new Buffer;
		info->inputBufferArray[i]->buffer = new char[bufferSize];
		info->inputBufferArray[i]->bufferSize = bufferSize;
		info->inputFile[i] = new std::ifstream;
		info->inputFile[i]->open(tempName+std::to_string(i),std::ios::binary);
		if (!info->inputFile[i]->is_open())
		{
			std::cout<<"error opening temporary file"<<std::endl;
			exit(-1);
		}
		info->inputFile[i]->read(info->inputBufferArray[i]->buffer, bufferSize);
		info->inputBufferArray[i]->bufferSize = info->inputFile[i]->gcount();
		info->eof[i] = false;
		info->endOfBuffer[i] = false;
		/* initialize the RecordList*/
		recordStart = info->inputBufferArray[i]->buffer;
		recordEnd = (char*) std::memchr(recordStart,'\n', info->inputBufferArray[i]->bufferSize);
		rec.size = recordEnd-recordStart+1;
		rec.data = recordStart;
		info->currentRecord[i] = rec.size;
		info->recordList.push_back(rec);
	}
	/* our main loop */
	while ((index = getLeast(info)) != -1)
	{
		putOut(info, index);
		getNext(info,index);
	}
	/* write the last records of the buffer (if any) to the output file */
	if (info->outputBuffer->bufferLength != 0)
	{
		info->outputFile->write(info->outputBuffer->buffer,info->outputBuffer->bufferLength);
	}
}
inline void getNext(PhaseTwoInfo * info,int index  )
{
	char * recordStart; /* start position of the record */
	char * recordEnd;   /* end position of the record */
	char * bufferEnd;   /* end position of the buffer */
	Buffer *inputBuffer;
	inputBuffer = info->inputBufferArray[index];
	partialRecord *parRec = info->parRec[index];
	std::ifstream *inputFile = info->inputFile[index];
	
	static int total = 0;
	
	if (info->eof[index] == true) return; /* if this run is out of records we do nothing*/
	if (info->endOfBuffer[index] == true) /*we have reached to the end of the buffer 
	                                       * so now we refill it */
	{
		inputFile->read(inputBuffer->buffer,inputBuffer->bufferSize);
		/* if this run is out of records we indicate it and exit */
		if (inputFile->eof() == true && inputFile->gcount() == 0) 
		{
			info->eof[index] = true;
			info->FilesDone++;
			return;
		}	
		inputBuffer->bufferSize = inputFile->gcount();
		bufferEnd = inputBuffer->buffer+inputBuffer->bufferSize-1;
		if (parRec->partial == true ) /* we have a partial record */
		{
			/* copy the rest of the record to parRec */
			recordEnd = (char *)std::memchr(inputBuffer->buffer,'\n',inputBuffer->bufferSize);
			std::memcpy(parRec->data + parRec->size,inputBuffer->buffer,
				recordEnd-inputBuffer->buffer+1);
			info->recordList[index].size = parRec->size+recordEnd-inputBuffer->buffer+1;
			info->recordList[index].data = parRec->data;
			total++;
			if (recordEnd == bufferEnd  ) /* we have reached to the end of the buffer */
			{
				info->endOfBuffer[index] == true;
				parRec->partial = false;
			}
			else /* we have not reached to the end of the buffer */
			{
				info->currentRecord[index] = recordEnd + 1-inputBuffer->buffer;
				parRec->partial = false;
				info->endOfBuffer[index] = false;
			}
		}
		else /*we get a record at the beginning of the buffer*/
		{
			recordStart = inputBuffer->buffer;
			recordEnd = (char*) std::memchr(recordStart,'\n', bufferEnd-recordStart+1);
			info->recordList[index].size = recordEnd-recordStart+1;
			info->recordList[index].data = recordStart;
			total++;
			info->currentRecord[index] = recordEnd + 1-inputBuffer->buffer;
			info->endOfBuffer[index] = false;
		}

	}
	else /*we are in the middle of the buffer*/
	{
		bufferEnd = inputBuffer->buffer+inputBuffer->bufferSize-1;
		recordStart = inputBuffer->buffer+info->currentRecord[index];
		recordEnd = (char*) std::memchr(recordStart,'\n', bufferEnd-recordStart+1);
		if (recordEnd == NULL) /* we have a partial record at the end of the buffer */
		{
			/* copy part of the record to the partial record */
			parRec->size = bufferEnd-recordStart+1;
			std::memcpy(parRec->data,recordStart,parRec->size);
			parRec->partial = true;
			info->endOfBuffer[index] = true;
		}
		else /* we don't have a partial record*/
		{
			if (recordEnd == bufferEnd) /* we have reached to the end of the buffer */
			{
				/* put the record to the RecordList */
				info->recordList[index].size = recordEnd-recordStart+1;
				info->recordList[index].data = recordStart;
				total++;
				info->endOfBuffer[index] = true;
			}
			else /* we have not reached to the end of the buffer */
			{
				/* put the record to the RecordList */
				info->recordList[index].size = recordEnd-recordStart+1;
				info->recordList[index].data = recordStart;
				total++;
				info->currentRecord[index] = recordEnd + 1-inputBuffer->buffer;
			}
		}
	}
}
inline void putOut(PhaseTwoInfo *info, int index)
{
	Buffer *outputBuffer = info->outputBuffer;
	partialRecord *parRec = info->parRec[index];
	/* we only deal with runs that still have records
	 * and full records (not partial ones)*/
	if (info->eof[index] == false && parRec->partial == false)
	{
		std::memcpy(outputBuffer->buffer+outputBuffer->bufferLength,
			    info->recordList[index].data,info->recordList[index].size);
		outputBuffer->bufferLength += info->recordList[index].size;
	}
	/* write the last records of the buffer (if any) to the output file */
	if (outputBuffer->bufferLength > info->bufferSize)
	{
		info->outputFile->write(outputBuffer->buffer,outputBuffer->bufferLength);
		outputBuffer->bufferLength = 0;
	}
}
inline int getLeast(PhaseTwoInfo *info)
{
	int index = -1; /* corresponding to a run */
	char min[MAX_LENGTH];
	int numberOfFiles = info->numberOfFiles; /* number of runs */
	/* find the first record in the RecordList, 
	 * choose it as the record with the least value*/
	for (int i = 0; i<numberOfFiles;i++)
	{
		if (info->eof[i] == false)
		{
			std::memcpy(min, info->recordList[i].data,info->recordList[i].size);
			index = i;
			break;
		}
	}
	/* determine the record with the least value int the RecordList*/
	for (int i = 0; i<numberOfFiles;i++)
	{
		if (info->eof[i] == false)
		{
			if((std::memcmp(info->recordList[i].data,min,info->recordList[i].size) < 0))
			{
				std::memcpy(min, info->recordList[i].data,info->recordList[i].size);
				index = i;
			}
		}
	}
	/* return the index of the run */
	return index;
}