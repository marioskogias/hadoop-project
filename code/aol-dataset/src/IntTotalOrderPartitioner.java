import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

 public class IntTotalOrderPartitioner extends Partitioner<IntWritable, Writable> {
 
        @Override
        public int getPartition(IntWritable key, Writable value, int numReduceTasks) {
 
        	/*
        	 * we have at the most 2 reducers and the dates vary from March to May 2006
        	 */

            //this is done to avoid performing mod with 0
            if(numReduceTasks == 0)
                return 0;
            
            /*
             * http://www.philippeadjiman.com/blog/2009/12/20/hadoop-tutorial-series-issue-2-getting-started-with-customized-partitioning/
             * check zipf's law
             */
            if (key.get() > 3) // this is a test
            	return 0;
            else
            	return 1;
        }

    }