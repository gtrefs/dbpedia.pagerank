package de.gtrefs.pagerank.mapred.sort;

import de.gtrefs.pagerank.mapred.kv.ResourceValue;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author Gregor Trefs
 */
public class SortReducer extends Reducer<DoubleWritable, ResourceValue, Text, Text> {

    private Text outputValue;
    private Text outputKey;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        outputKey = new Text();
        outputValue = new Text();
    }

    @Override
    protected void reduce(DoubleWritable key, Iterable<ResourceValue> values, Context context) throws IOException, InterruptedException {
        // Get description
        for (ResourceValue val : values) {
            if (val.isDescription()) {
                // Split
                String split[] = val.getResourceDescription().split("\\s");
                // Set output
                outputKey.set(split[0]);
                outputValue.set(split[1] + " " + split[2] + " " + split[3]);
                // Emit
                context.write(outputKey, outputValue);
            }
        }

    }
}
