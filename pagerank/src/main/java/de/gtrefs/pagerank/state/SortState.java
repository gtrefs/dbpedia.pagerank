package de.gtrefs.pagerank.state;

import de.gtrefs.pagerank.PageRank;
import de.gtrefs.pagerank.mapred.kv.ResourceValue;
import de.gtrefs.pagerank.mapred.sort.SortMapper;
import de.gtrefs.pagerank.mapred.sort.SortReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Gregor Trefs
 */
public class SortState extends AbstractState {

    @Override
    public void executeJob(PageRank rank) throws IOException, InterruptedException, ClassNotFoundException {
        // log where we are right now
        logLocation();
        sortResultJob(configuration.getUnsortedResultPath(), configuration.getSortedResultPath(), rank.getConf());
        // Set state
        rank.setState(new FinishedState());
    }

    private boolean sortResultJob(String inputPath, String outputPath, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        // Vars
        // Input path
        Path inputPathFS = new Path(inputPath);
        Path outputPathFS = new Path(outputPath);
        // Set up job to find out outdegrees
        Configuration sortResultConf = new Configuration(conf);
        Job sortResultJob = new Job(sortResultConf, "SortResultJob");
        sortResultJob.setJarByClass(PageRank.class);
        sortResultJob.setMapperClass(SortMapper.class);
        sortResultJob.setReducerClass(SortReducer.class);

        sortResultJob.setOutputKeyClass(Text.class);
        sortResultJob.setOutputValueClass(Text.class);

        // Mapper has different output
        sortResultJob.setMapOutputKeyClass(DoubleWritable.class);
        sortResultJob.setMapOutputValueClass(ResourceValue.class);

        TextInputFormat.addInputPath(sortResultJob, inputPathFS);
        TextOutputFormat.setOutputPath(sortResultJob, outputPathFS);
        // Delete output file if it exists
        removeIfExists(outputPath, sortResultConf);
        // Run job
        sortResultJob.waitForCompletion(true);
        // if the file is present, delete unsorted result
        if (FileSystem.get(sortResultConf).exists(outputPathFS)) {
            removeIfExists(inputPath, sortResultConf);
            return true;
        }
        // Else return false
        return false;
    }
}
