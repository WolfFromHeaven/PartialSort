#include <iostream>
#include <vector>
#include <iomanip>
#include <list>
#include <algorithm>
#include <map>

#include "utility.h"


using namespace std;
typedef vector< unsigned char* > NewList;

bool Sorter( const unsigned char* keyl, const unsigned char* keyr )
{
  for ( unsigned long i = 0; keyl[i] != '\0'; i++ ) {
    if ( keyl[ i ] < keyr[ i ] ) {
      return true;
    }
    else if ( keyl[ i ] > keyr[ i ] ) {
      return false;
    }
  }
  return true;
}

bool cmpKey( const unsigned char* keyA, const unsigned char* keyB, unsigned int size )
{
  for ( unsigned long i = 0; i < size; i++ ) {
    if ( keyA[ i ] < keyB[ i ] ) {
      return true;
    }
    else if ( keyA[ i ] > keyB[ i ] ) {
      return false;
    }
  }
  return false;
}

NewList SortSorted(NewList array[]){
    NewList finalSort;
    int arrSize = 3; //number of reducers
    list<unsigned char*> mylist;
    map<unsigned char*,int> Id;
    
    for(int i =0;i < arrSize; i++){
      mylist.push_back((array[i])[0]);
      Id[(array[i])[0]] = i;
    }
    int flag = 0;
    while(flag == 0){
      //cout<<"T\n";
      unsigned char* min = *mylist.begin(); 
      for(auto it = mylist.begin(); it != mylist.end(); ++it){
        if(!cmpKey(min,*it,4)) min = *it; 
      }

      finalSort.push_back(min);
      
      int index = Id[min];

      array[index].erase(array[index].begin());
      mylist.remove(min);
      if(!array[index].empty()){
        flag = 0;
        mylist.push_back((array[index])[0]);
        Id[(array[index])[0]] = index;
      } 
      else{
        if(!mylist.empty()) flag =0;
        else flag =1;
      }
      for(auto itr = mylist.begin(); itr != mylist.end(); ++itr) cout<<*itr<<" ";
      cout<<"\n";
  }
  return finalSort;

}


int main() {
  
  NewList charVector1;
  NewList charVectorarray[3];
  NewList out;
  vector<int> vec;
  int n;
  cin>>n;
  for(int i=0; i<n; i++){
    unsigned char* arr = new unsigned char[4];
    for(int j=0; j<4; j++){
      cin>>arr[j];
    }
    charVector1.push_back(arr);
  }
 
  for(int k=0; k <4 ; k++){
    charVectorarray[0].push_back(charVector1[k]);
  }
  sort((charVectorarray[0]).begin(),(charVectorarray[0]).end(),Sorter);
  
  for(int k=4; k <7 ; k++){
    charVectorarray[1].push_back(charVector1[k]);
  }
  sort((charVectorarray[1]).begin(),(charVectorarray[1]).end(),Sorter);
  
  for(int k=7; k <13 ; k++){
    charVectorarray[2].push_back(charVector1[k]);
  }
  sort((charVectorarray[2]).begin(),(charVectorarray[2]).end(),Sorter);
  
/*  for(auto it = charVectorarray[2].begin(); it != charVectorarray[2].end(); it++){
      cout<<*it<<" ";
  }*/

  out = SortSorted(charVectorarray);
  for(int i=0; i<out.size(); i++){
    cout<<out[i]<<"\n";
  }
}
