package de.gtrefs.pagerank.mapred.computePageRank;

import de.gtrefs.pagerank.configuration.Conf;
import de.gtrefs.pagerank.mapred.kv.ResourceValue;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author Gregor Trefs
 */
public class ComputePagerankReducer extends Reducer<Text, ResourceValue, Text, Text> {

    private double s, d, e;
    private int numberOfResources;
    private Text outputKey;
    private Text outputValue;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // init
        outputKey = new Text();
        outputValue = new Text();
        // Get s (Conf.S)
        s = Conf.S;
        double t = 1-s;
        // Get d
        d = Double.valueOf(context.getConfiguration().get(Conf.DANGLING_PROPABILITY_FRACTION));
        // Get number of Resources
        numberOfResources = context.getConfiguration().getInt(Conf.RESOURCE_COUNT, -1);
        // Compute teleportation fraction
        e = (1d / numberOfResources) * t;
    }

    @Override
    protected void reduce(Text key, Iterable<ResourceValue> values, Context context) throws IOException, InterruptedException {
        double pageRank = 0;
        String description = "";
        // Iterate over Resource Values
        for (ResourceValue val : values) {
            if (val.isDescription()) {
                description = val.getResourceDescription();
                continue;
            }
            pageRank += val.getPageRankFraction();
        }
        // Multiply a with s
        pageRank *= s;
        // Split description up
        String split[] = description.split("\\s");
        // Add d and e to pagerank
        pageRank += d;
        pageRank += e;
        // Set output
        if (split.length > 1) {
            outputValue.set(pageRank + " " + split[2] + " " + split[3]);
        } else {
            // No description was found, at least actual pagerank could be written out
            // Will crash in next iteration
            outputValue.set(String.valueOf(pageRank));
        }
        outputKey.set(key);
        // Emit new description
        context.write(outputKey, outputValue);
    }
}
