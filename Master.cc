#include <iostream>
#include <assert.h>
#include <mpi.h>
#include <iomanip>
#include <algorithm>
#include <string.h>

#include "Master.h"
#include "Common.h"
#include "Configuration.h"
#include "PartitionSampling.h"
#include "Trie.h"

using namespace std;

void Master::run()
{
  if ( totalNode != 1 + conf.getNumReducer() ) {
    cout << "The number of workers mismatches the number of processes.\n";
    assert( false );
  }

  // GENERATE LIST OF PARTITIONS.
  PartitionSampling partitioner;
  partitioner.setConfiguration( &conf );
	//PartitionList rangeList;
	PartitionList maxList, minList;	
	PartitionList partitionList;
	
  // BROADCAST CONFIGURATION TO WORKERS
  MPI::COMM_WORLD.Bcast( &conf, sizeof( Configuration ), MPI::CHAR, 0 );
  // Note: this works because the number of partitions can be derived from the number of workers in the configuration.


/*  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    unsigned char* partition = *it;
    MPI::COMM_WORLD.Bcast( partition, conf.getKeySize() + 1, MPI::UNSIGNED_CHAR, 0 );
  }*/

  
  // TIME BUFFER
  int numWorker = conf.getNumReducer();
  double rcvTime[ numWorker + 1 ];  
  double rTime = 0;
  double avgTime;
  double maxTime;
	double totTime = 0;


	// RECIEVING TOPK DATASET RANGE
 /* for ( unsigned int i = 1; i <= conf.getNumReducer(); i++ ) {
		for(int j = 0; j < 2; j++){
			unsigned char* buff = new unsigned char[ conf.getKeySize()];
			MPI::COMM_WORLD.Recv( buff, conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0);
			rangeList.push_back( buff );
		}
  }	

	sort( rangeList.begin(), rangeList.end(), Sorter(conf.getKeySize()) ) ;
	unsigned char* minKey = rangeList.front();
	unsigned char* maxKey = rangeList.back();*/

  for ( unsigned int i = 1; i <= conf.getNumReducer(); i++ ) {
		for(int j = 0; j < 2; j++){//receive min and max value for each node
			unsigned char* buff = new unsigned char[ conf.getKeySize()];
			MPI::COMM_WORLD.Recv( buff, conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0);
			if(j == 1) maxList.push_back( buff );
			else minList.push_back( buff );
		}
  }	

	sort( maxList.begin(), maxList.end(), Sorter(conf.getKeySize()) ) ;
	sort( minList.begin(), minList.end(), Sorter(conf.getKeySize()) ) ;
	unsigned char* minKey = minList.front();
	unsigned char* maxKey = maxList.front();

	unsigned long long dig1 = 0; unsigned long long dig2 = 0;
	//converting the stings to integer values
	for(unsigned int i = 0; i < conf.getKeySize(); i++){
			unsigned long long temp = pow(10,conf.getKeySize()-i-1);
			dig1 += (minKey[i]-48)*temp;
			dig2 += (maxKey[i]-48)*temp;
	}

	unsigned long long part[conf.getNumReducer()-1];
	unsigned long long range = dig2 - dig1;
	unsigned long long partitionSize = round(range/conf.getNumReducer());
	for( unsigned int i = 1; i < conf.getNumReducer(); i++){
			part[i-1] = (i*partitionSize) + dig1;
	}

	unsigned char tempArr[conf.getKeySize()];
	for( unsigned int i = 0; i < conf.getNumReducer()-1; i++){
		unsigned long long num = part[i];
		unsigned char* keyBuff = new unsigned char[ conf.getKeySize()];
		for(unsigned int j = 0; j < conf.getKeySize(); j++){
				unsigned long long temp = pow(10,conf.getKeySize()-j-1);
				unsigned long long quotient = num / temp;
				unsigned long long rem = num % temp;
				num = rem;
				tempArr[j] = (unsigned char) (quotient + 48);
		}
		memcpy(keyBuff, tempArr,conf.getKeySize());
		partitionList.push_back(keyBuff);
	}

  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = partitionList.begin(); it != partitionList.end(); it++ ) {
    unsigned char* partition = *it;
    MPI::COMM_WORLD.Bcast( partition, conf.getKeySize(), MPI::UNSIGNED_CHAR, 0 );
  }
	MPI::COMM_WORLD.Bcast( maxKey, conf.getKeySize(), MPI::UNSIGNED_CHAR, 0 );

  // COMPUTE MAP TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": MAP     | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;
	totTime += avgTime/numWorker;

  // COMPUTE PACKING TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": PACK    | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  

  
  // COMPUTE SHUFFLE TIME
  double txRate = 0;
  double avgRate = 0;
  for( unsigned int i = 1; i <= conf.getNumReducer(); i++ ) {
    MPI::COMM_WORLD.Barrier();
    MPI::COMM_WORLD.Barrier();
    MPI::COMM_WORLD.Recv( &rTime, 1, MPI::DOUBLE, i, 0 );
    avgTime += rTime;
    MPI::COMM_WORLD.Recv( &txRate, 1, MPI::DOUBLE, i, 0 );
    avgRate += txRate;
  }
  cout << rank << ": SHUFFLE | Sum = " << setw(10) << avgTime
       << "   Rate = " << setw(10) << avgRate/numWorker << " Mbps" << endl; 
	totTime += avgTime/numWorker;

  // COMPUTE UNPACK TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": UNPACK  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;
  
  // COMPUTE REDUCE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
		cout<<i<<":"<<rcvTime[i]<<"\n";
  }
  cout << rank << ": REDUCE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;      
	totTime += avgTime/numWorker;
	cout<< rank << ": TOTAL = " << totTime<< endl;

  // CLEAN UP MEMORY
  for ( auto it = partitionList.begin(); it != partitionList.end(); it++ ) {
    delete [] *it;
  }
}

/*bool Master::Sorter( const unsigned char* keyl, const unsigned char* keyr, unsigned int size )
{
  for ( unsigned long i = 0; i < size; i++ ) {
    if ( keyl[i] < keyr[i] ) {
      return true;
    }
    else if ( keyl[ i ] > keyr[ i ] ) {
      return false;
    }
  }
  return true;
}*/
