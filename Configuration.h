#ifndef _MR_CONFIGURATION
#define _MR_CONFIGURATION

class Configuration {

 protected:
  unsigned int numReducer;
  unsigned int numInput;  
  
  const char *inputPath;
  const char *outputPath;
  const char *partitionPath;
  unsigned long numSamples;
	unsigned int K;
  
 public:
  Configuration() {
    numReducer = 3;
    numInput = numReducer;    
    K = 20;
    //inputPath = "/home/prarthana/Coded-TeraSort/Textgen/fileNum.txt";
		inputPath = "./Input/fileNum.txt";
		outputPath = "./Output/Output_1GB";
    partitionPath = "./Partition/Partition_1GB";
    
	//	outputPath = "./Output/Input125GB";
    //partitionPath = "./Partition/Input125GB";
    numSamples = 100;
  }
  ~Configuration() {}
  const static unsigned int KEY_SIZE = 10;
  const static unsigned int VALUE_SIZE = 90;  
  
  unsigned int getNumReducer() const { return numReducer; }
  unsigned int getNumInput() const { return numInput; } 
	unsigned int getK() const { return K; } 
  const char *getInputPath() const { return inputPath; }
  const char *getOutputPath() const { return outputPath; }
  const char *getPartitionPath() const { return partitionPath; }
  unsigned int getKeySize() const { return KEY_SIZE; }
  unsigned int getValueSize() const { return VALUE_SIZE; }
  unsigned int getLineSize() const { return KEY_SIZE + VALUE_SIZE; }
  unsigned long getNumSamples() const { return numSamples; }  
};

#endif
