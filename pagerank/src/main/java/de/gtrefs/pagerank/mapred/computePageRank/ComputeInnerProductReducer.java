package de.gtrefs.pagerank.mapred.computePageRank;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Sums PageRank of dangling links up.
 * @author Gregor Trefs
 */
public class ComputeInnerProductReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable outputValue;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        outputValue = new DoubleWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum=0;
        for(DoubleWritable val:values){
            sum += val.get();
        }
        outputValue.set(sum);
        context.write(key, outputValue);
    }
    
}
