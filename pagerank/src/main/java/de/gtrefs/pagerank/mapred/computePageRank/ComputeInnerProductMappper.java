package de.gtrefs.pagerank.mapred.computePageRank;

import de.gtrefs.pagerank.configuration.Conf;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Emits PageRank of every DanglingNode.
 * @author Gregor Trefs
 */
public class ComputeInnerProductMappper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private DoubleWritable outputValue;
    private Text outputKey;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        outputValue = new DoubleWritable();
        outputKey = new Text(Conf.INNER_PRODUCT);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String split[] = value.toString().split("\\s");
        int outNodeCount = Integer.valueOf(split[2]);
        // If it is a dangling node
        if(outNodeCount==0){
            // Emit pageRank
            outputValue.set(Double.valueOf(split[1]));
            context.write(outputKey, outputValue);
        }
    }
    
}
