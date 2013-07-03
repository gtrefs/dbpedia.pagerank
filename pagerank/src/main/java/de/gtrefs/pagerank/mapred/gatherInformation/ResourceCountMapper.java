package de.gtrefs.pagerank.mapred.gatherInformation;

import de.gtrefs.pagerank.configuration.Conf;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author Gregor Trefs
 */
public class ResourceCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text key;
    private IntWritable one;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        key = new Text(Conf.RESOURCE_COUNT);
        one = new IntWritable(1);
    }

    
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(this.key, one); 
    }
    
    
}
