import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

 public class MyCustomDatePartitioner extends Partitioner<Text, IntWritable> {
 
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
 
        	/*
        	 * we have at the most 2 reducers and the dates vary from March to May 2006
        	 */
            String date = key.toString();

            //this is done to avoid performing mod with 0
            if(numReduceTasks == 0)
                return 0;
            
            if (date.compareTo("2006-04-15") == -1 )
            	return 0;
            else
            	return 1;
 

        }
    }