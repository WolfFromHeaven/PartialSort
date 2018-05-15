#include <iostream>
#include <assert.h>
#include <mpi.h>
#include <iomanip>
#include <algorithm>
#include <string.h>
#include <vector>
#include <cmath>

#include "CodedMaster.h"
#include "Common.h"
#include "CodedConfiguration.h"
#include "PartitionSampling.h"
#include "Trie.h"

using namespace std;

void CodedMaster::run()
{
  if ( totalNode != 1 + conf.getNumReducer() ) {
    cout << "The number of workers mismatches the number of processes.\n";
    assert( false );
  }

  // GENERATE LIST OF PARTITIONS.
  PartitionSampling partitioner;
  partitioner.setConfiguration( &conf );
  //PartitionList* partitionList = partitioner.createPartitions();
	PartitionList rangeList;
	PartitionList partList;


  // BROADCAST CONFIGURATION TO WORKERS
  MPI::COMM_WORLD.Bcast( &conf, sizeof( CodedConfiguration ), MPI::CHAR, 0 );
  // Note: this works because the number of partitions can be derived from the number of workers in the configuration.


  // BROADCAST PARTITIONS TO WORKERS
/*  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    unsigned char* partition = *it;
    MPI::COMM_WORLD.Bcast( partition, conf.getKeySize() + 1, MPI::UNSIGNED_CHAR, 0 );
  }*/


  // TIME BUFFER
  int numWorker = conf.getNumReducer();
  double rcvTime[ numWorker + 1 ];  
  double rTime = 0;
  double avgTime;
  double maxTime;


  // COMPUTE CODE GENERATION TIME  
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": CODEGEN | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;


	// RECIEVING TOPK DATASET RANGE
	//unsigned char partition[ conf.getKeySize()] = {" "};
  for ( unsigned int i = 1; i <= conf.getNumReducer(); i++ ) {
		for(int j = 0; j < 2; j++){//receive min and max value for each node
			unsigned char* buff = new unsigned char[ conf.getKeySize()];
			MPI::COMM_WORLD.Recv( buff, conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0);
			rangeList.push_back( buff );
		}
  }	
	//cout<<rangeList.size()<<"\n";
	sort( rangeList.begin(), rangeList.end(), Sorter(conf.getKeySize()) ) ;
	unsigned char* minKey = rangeList.front();
	unsigned char* maxKey = rangeList.back();
	//cout<<minKey[0]<<" "<<maxKey[0]<<"\n";
	unsigned long long dig1 = 0; unsigned long long dig2 = 0;

	//CONVERTING THE STRINGS TO INTEGER VALUES
	for(unsigned int i = 0; i < conf.getKeySize(); i++){
			unsigned long long temp = pow(95,conf.getKeySize()-i-1);
			//cout<<minKey[i]<<" "<<maxKey[i]<<"\n";
			dig1 += (minKey[i]-32)*temp;
			dig2 += (maxKey[i]-32)*temp;
			//cout<<dig1<<" "<<dig2<<"\n";
	}
	//cout<<dig1<<" "<<dig2;

	//FINDING THE PARTITIONS
	unsigned long long part[conf.getNumReducer()-1];
	unsigned long long range = dig2 - dig1;
	unsigned long long partitionSize = round(range/conf.getNumReducer());
	for( unsigned int i = 1; i < conf.getNumReducer(); i++){
			part[i-1] = (i*partitionSize) + dig1;
	}
	//cout<<part[0]<<" "<<part[1];
	unsigned char tempArr1[conf.getKeySize()];
	for( unsigned int i = 0; i < conf.getNumReducer()-1; i++){
		//cout<<"111\n";
		unsigned long long num = part[i];cout<<i<<" : ";
		unsigned char* keyBuff = new unsigned char[ conf.getKeySize()];
		for(unsigned int j = 0; j < conf.getKeySize(); j++){
				unsigned long long temp = pow(95,conf.getKeySize()-j-1);
				unsigned long long quotient = num / temp;
				unsigned long long rem = num % temp;
				num = rem;
				tempArr1[j] = (unsigned char) (quotient + 32); cout<<tempArr1[j];
		}
		cout<<endl;
		memcpy(keyBuff, tempArr1, conf.getKeySize());
		partList.push_back(keyBuff);
	}
	cout<<partList.size()<<endl;
	

  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = partList.begin(); it != partList.end(); it++ ) {
    unsigned char* partition = *it;
    MPI::COMM_WORLD.Bcast( partition, conf.getKeySize(), MPI::UNSIGNED_CHAR, 0 );
  }

      

  // COMPUTE MAP TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": MAP     | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  

  
  // COMPUTE ENCODE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": ENCODE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  

  // COMPUTE SHUFFLE TIME
  avgTime = 0;
  double txRate = 0;
  double avgRate = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    MPI::COMM_WORLD.Recv( &rTime, 1, MPI::DOUBLE, i, 0 );
    avgTime += rTime;    
    MPI::COMM_WORLD.Recv( &txRate, 1, MPI::DOUBLE, i, 0 );
    avgRate += txRate;
  }
  cout << rank
       << ": SHUFFLE | Sum = " << setw(10) << avgTime
       << "   Rate = " << setw(10) << avgRate/numWorker << " Mbps" << endl;


  // COMPUTE DECODE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": DECODE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  

  // COMPUTE REDUCE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": REDUCE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;      
  

  // CLEAN UP MEMORY
  for ( auto it = partList.begin(); it != partList.end(); it++ ) {
    delete [] *it;
  }
}
