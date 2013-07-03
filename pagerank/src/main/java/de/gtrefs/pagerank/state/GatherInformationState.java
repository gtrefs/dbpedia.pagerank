package de.gtrefs.pagerank.state;

import de.gtrefs.pagerank.configuration.Conf;
import de.gtrefs.pagerank.PageRank;
import de.gtrefs.pagerank.mapred.gatherInformation.GatherInformationMapper;
import de.gtrefs.pagerank.mapred.gatherInformation.GatherInformationReducer;
import de.gtrefs.pagerank.mapred.gatherInformation.ResourceCountMapper;
import de.gtrefs.pagerank.mapred.gatherInformation.ResourceCountReducer;
import de.gtrefs.pagerank.mapred.gatherInformation.ResourceCountSplitMapper;
import de.gtrefs.pagerank.mapred.gatherInformation.ResourceCountSplitReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Gregor Trefs
 */
public class GatherInformationState extends AbstractState {

    private int resourceCount = 0;

    public GatherInformationState() {
        //log = LogFactory.getLog(GatherInformationState.class);
    }

    @Override
    public void executeJob(PageRank rank) throws IOException, InterruptedException, ClassNotFoundException {
        // Log where we are right now
        logLocation();
        // 1. Job: Count number of resources
        // Create new Job with ResourceCountMapper and -Reducer
        // If the count fails
        if (countResourcesJob(configuration.getGraphFilePath(), configuration.getCountFilePath(), rank.getConf()) && gatherInformationJob(configuration.getGraphFilePath(), configuration.getInformationGatheringPath(), rank.getConf())) {
            // Resources have been counted and information have been gathered
            // Set resource count
            rank.setResourceCount(resourceCount);
            // Advance to ComputePageRankState
            rank.setState(new ComputePageRankState());
            // Return
            return;
        }
        // Something went wrong
        rank.setState(new FinishedState());
    }

    private boolean gatherInformationJob(String inputPath, String outputPath, Configuration pageRankConfiguration) throws IOException, InterruptedException, ClassNotFoundException {
        // Vars
        // Input path
        Path inputPathFS = new Path(inputPath);
        Path outputPathFS = new Path(outputPath);
        // Set up job to find out outdegrees
        // Clone settings from pageRankConfiguration
        Configuration gatherInformationConf = new Configuration(pageRankConfiguration);
        // Set count
        gatherInformationConf.setInt(Conf.RESOURCE_COUNT, resourceCount);
        // Set filter type
        gatherInformationConf.set(Conf.FILTER_TYPE, configuration.getStrategy().getType().toString());
        Job gatherInformationJob = new Job(gatherInformationConf, "GatherInformationJob");
        gatherInformationJob.setJarByClass(PageRank.class);
        gatherInformationJob.setMapperClass(GatherInformationMapper.class);
        gatherInformationJob.setReducerClass(GatherInformationReducer.class);

        gatherInformationJob.setOutputKeyClass(Text.class);
        gatherInformationJob.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(gatherInformationJob, inputPathFS);
        TextOutputFormat.setOutputPath(gatherInformationJob, outputPathFS);

        if (!configuration.isKeepGatherInformationFiles() || !FileSystem.get(gatherInformationConf).exists(outputPathFS)) {
            // Delete output file if it exists
            removeIfExists(outputPath, gatherInformationConf);
            // Run job
            gatherInformationJob.waitForCompletion(true);
        }
        // Is the file present ?
        return FileSystem.get(gatherInformationConf).exists(outputPathFS);
    }

    private boolean countResourcesJob(String inputPath, String outputPath, Configuration pageRankConfiguration) throws IOException, InterruptedException, ClassNotFoundException {
        String intermediatePath = outputPath + Conf.INTERMEDIATE_RESULT_PATH_SUFFIX;
        boolean redoJob = !configuration.isKeepGatherInformationFiles() || !FileSystem.get(new Configuration()).exists(new Path(outputPath));
        return redoJob ? resourcesCountSplitJob(inputPath, intermediatePath, pageRankConfiguration) && resourcesCountJob(intermediatePath, outputPath, pageRankConfiguration) : extractResourceCount(new Configuration(), outputPath);

    }

    private boolean extractResourceCount(Configuration conf, String outputPath) throws IOException {
        // Retrieve content of file
        Integer temp = getFileContent(Integer.class, conf, outputPath, Conf.RESOURCE_COUNT);
        // If it is null return false
        if (temp == null) {
            return false;
        }
        // Else set new resource count
        resourceCount = temp;
        // return true
        return true;
    }

    private boolean resourcesCountJob(String inputPath, String outputPath, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        // Vars
        Path inputPathFS = new Path(inputPath);
        Path outputPathFS = new Path(outputPath);
        // Set up job to find out outdegrees
        Configuration countResourceConf = new Configuration(conf);
        Job countResourcesJob = new Job(countResourceConf, "ResourcesCountJob");
        countResourcesJob.setJarByClass(PageRank.class);
        countResourcesJob.setMapperClass(ResourceCountMapper.class);
        countResourcesJob.setReducerClass(ResourceCountReducer.class);
        countResourcesJob.setCombinerClass(ResourceCountReducer.class);

        countResourcesJob.setOutputKeyClass(Text.class);
        countResourcesJob.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(countResourcesJob, inputPathFS);
        TextOutputFormat.setOutputPath(countResourcesJob, outputPathFS);
        // Delete outputfile if it exists
        removeIfExists(outputPath, countResourceConf);
        // Run job
        countResourcesJob.waitForCompletion(true);

        // Is the file present ?
        if (FileSystem.get(countResourceConf).exists(outputPathFS)) {
            // Retrieve content of file
            boolean successfullExtracted = extractResourceCount(countResourceConf, outputPath);
            // Delete intermediate results
            removeIfExists(inputPath, countResourceConf);
            return successfullExtracted;
        }
        return false;
    }

    private boolean resourcesCountSplitJob(String inputPath, String outputPath, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        // Vars
        // Input path
        Path inputPathFS = new Path(inputPath);
        Path outputPathFS = new Path(outputPath);
        // Set up job to find out outdegrees
        Configuration countResourceConf = new Configuration(conf);
        // Deliver the filter type
        countResourceConf.set(Conf.FILTER_TYPE, configuration.getStrategy().getType().toString());
        Job countResourcesJob = new Job(countResourceConf, "ResourcesCountSplitJob");
        countResourcesJob.setJarByClass(PageRank.class);
        countResourcesJob.setMapperClass(ResourceCountSplitMapper.class);
        countResourcesJob.setReducerClass(ResourceCountSplitReducer.class);
        countResourcesJob.setCombinerClass(ResourceCountSplitReducer.class);

        countResourcesJob.setOutputKeyClass(Text.class);
        countResourcesJob.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(countResourcesJob, inputPathFS);
        TextOutputFormat.setOutputPath(countResourcesJob, outputPathFS);
        // Run job if the file is not present or it should be overwritten
        // Delete outputfile if it exists
        removeIfExists(outputPath, countResourceConf);
        // Run job
        countResourcesJob.waitForCompletion(true);
        // Is the file present ?
        return FileSystem.get(countResourceConf).exists(outputPathFS);
    }
}
