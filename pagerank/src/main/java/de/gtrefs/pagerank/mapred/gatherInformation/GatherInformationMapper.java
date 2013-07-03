package de.gtrefs.pagerank.mapred.gatherInformation;

import de.gtrefs.pagerank.configuration.Conf;
import de.gtrefs.pagerank.mapred.strategy.Strategy;
import de.gtrefs.pagerank.mapred.strategy.StrategyFactory;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author Gregor Trefs
 */
public class GatherInformationMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text key;
    private Text value;
    private Strategy filter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        key = new Text();
        value = new Text();
        // Determine filter
        String filterType = context.getConfiguration().get(Conf.FILTER_TYPE);
        filter = StrategyFactory.getInstance().determineStrategy(filterType);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        filter.filterGatherInformationMapper(key, value, context,this.key,this.value); 
    }
}
