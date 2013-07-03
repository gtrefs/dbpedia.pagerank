package de.gtrefs.pagerank.mapred.sort;

import de.gtrefs.pagerank.mapred.kv.ResourceValue;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author Gregor Trefs
 */
public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, ResourceValue> {

    private DoubleWritable outputKey;
    private ResourceValue outputValue;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        outputKey = new DoubleWritable();
        outputValue = new ResourceValue();
    }
    
    
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get pagerank
        String split[] = value.toString().split("\\s");
        double pageRank = Double.parseDouble(split[1]);
        // Hadoops sort mechanism (QickSort or HeapSort) is used
        // For an descending order, we have to multiply pagerank with -1
        pageRank *= -1;
        // Set outputkey
        outputKey.set(pageRank);
        // Set output value
        outputValue.setResourceDescription(value.toString());
        // Emit
        context.write(outputKey, outputValue);
    }

    
}
