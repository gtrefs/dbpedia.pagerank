package de.gtrefs.pagerank.mapred.gatherInformation;

import de.gtrefs.pagerank.configuration.Conf;
import de.gtrefs.pagerank.mapred.strategy.Strategy;
import de.gtrefs.pagerank.mapred.strategy.StrategyFactory;
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
public class ResourceCountSplitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text resource;
    private IntWritable one;
    private Strategy filter; 

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        resource = new Text();
        one = new IntWritable(1);
        // Determine filter
        String filterType = context.getConfiguration().get(Conf.FILTER_TYPE);
        filter = StrategyFactory.getInstance().determineStrategy(filterType);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] stringResources = filter.filterResourceCountSplitMapper(value.toString());
        for(String stringResource:stringResources){
            resource.set(stringResource);
            context.write(resource, one);
        }
    }
}
