package de.gtrefs.pagerank.mapred.gatherInformation;

import de.gtrefs.pagerank.configuration.Conf;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author Gregor Trefs
 */
public class ResourceCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text key;
    private IntWritable sum;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        key = new Text(Conf.RESOURCE_COUNT);
        sum = new IntWritable(0);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum+=i.get();
        }
        // Write it out
        this.sum.set(sum);
        context.write(this.key, this.sum);
    }
}
