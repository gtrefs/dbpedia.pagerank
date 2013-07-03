package de.gtrefs.pagerank.mapred.computePageRank;

import de.gtrefs.pagerank.configuration.Conf;
import de.gtrefs.pagerank.mapred.kv.ResourceValue;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author Gregor Trefs
 */
public class ComputePagerankMapper extends Mapper<LongWritable, Text, Text, ResourceValue> {

    private Text outputKey;
    private ResourceValue outputValue;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        outputKey = new Text();
        outputValue = new ResourceValue();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // For every outgoing link, emit ResourceValue with pageRankFraction
        // For key emit ResourceValue with resourceDescription

        // Split value by space
        String split[] = value.toString().split("\\s");
        int numberOfOutlinks = Integer.valueOf(split[2]);
        double pageRank = Double.parseDouble(split[1]);
        // If it is not a dangling node, emit for every outlink ResourceValue
        if (numberOfOutlinks != 0) {
            // Split outgoing links by Conf.OUT_NODE_LIST_SEPERATOR
            String outnodes[] = split[3].split(Conf.OUT_NODE_LIST_SEPERATOR_MATCHER);
            // Compute fraction
            double pageRankFraction = pageRank / numberOfOutlinks;
            // For every outlinks emit ResourceValue
            for (String outnode : outnodes) {
                outputKey.set(outnode);
                outputValue.setPageRankFraction(pageRankFraction);
                context.write(outputKey, outputValue);
            }
        }
        // Emit description for input value
        outputKey.set(split[0]);
        outputValue.setResourceDescription(value.toString());
        // Emit
        context.write(outputKey, outputValue);
    }
}
