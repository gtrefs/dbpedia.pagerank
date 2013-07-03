package de.gtrefs.pagerank.state;

import de.gtrefs.pagerank.PageRank;
import de.gtrefs.pagerank.configuration.Conf;
import de.gtrefs.pagerank.mapred.computePageRank.ComputeInnerProductMappper;
import de.gtrefs.pagerank.mapred.computePageRank.ComputeInnerProductReducer;
import de.gtrefs.pagerank.mapred.computePageRank.ComputePagerankReducer;
import de.gtrefs.pagerank.mapred.computePageRank.ComputePagerankMapper;
import de.gtrefs.pagerank.mapred.kv.ResourceValue;
import java.io.IOException;
import org.apache.commons.logging.LogFactory;
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
public class ComputePageRankState extends AbstractState {

    private double danglingPropability = 0;

    @Override
    public void executeJob(PageRank rank) throws IOException, InterruptedException, ClassNotFoundException {
        // log where we are right now
        logLocation();
        // 0. Do Conf.PAGERANK_LOOPS times
        // 0.1. Compute Dangling contribution
        // 0.2. Compute walk and teleport
        // Determine start and end
        int start = configuration.getComputationResultStart() < 0 ? 0 : configuration.getComputationResultStart();
        int end = configuration.getComputationResultEnd() < start ? start : configuration.getComputationResultEnd();
        String pathToDelete = "";
        // Determine input path
        String inputPath = start == 0 ? configuration.getInformationGatheringPath() : configuration.getUnsortedResultPath() + Conf.INTERMEDIATE_RESULT_PATH_SUFFIX + (start - 1);
        // Set outputpath
        String outputPath = configuration.getUnsortedResultPath() + Conf.INTERMEDIATE_RESULT_PATH_SUFFIX + start;
        for (int i = start; i < end; i++) {
            LogFactory.getLog(ComputePageRankState.class).info("Round " + (i + 1) + " of " + end);
            if (!(computeDanglingPart(inputPath, configuration.getComputeDanglingPath(), rank.getResourceCount(), Conf.S, rank.getConf()) && computePageRank(inputPath, outputPath, rank.getResourceCount(), rank.getConf()))) {
                // If one of both jobs is not functioning properly
                // exit
                rank.setState(new FinishedState());
                return;
            }
            // Create new paths
            pathToDelete = inputPath;
            inputPath = outputPath;
            outputPath = (i + 1 == end - 1 ? configuration.getUnsortedResultPath() : configuration.getUnsortedResultPath() + Conf.INTERMEDIATE_RESULT_PATH_SUFFIX + (i + 1));
            // If path to delete is not empty, delete it
            // Only delete informationGathering information if requested
            if (!pathToDelete.isEmpty() && (i != 0 || configuration.isDeleteInformationGatheringInformation())) {
                removeIfExists(pathToDelete, rank.getConf());
            }
        }
        rank.setState(new SortState());
    }

    private boolean computePageRank(String inputPath, String outputPath, int numberOfResources, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        // Vars
        // Input path
        LogFactory.getLog(ComputePageRankState.class).info("Before pageRank computation");
        Path inputPathFS = new Path(inputPath);
        Path outputPathFS = new Path(outputPath);
        // Set up job to find out outdegrees
        Configuration computePageRankConf = new Configuration(conf);
        // Set d and number of resource
        computePageRankConf.setInt(Conf.RESOURCE_COUNT, numberOfResources);
        computePageRankConf.set(Conf.DANGLING_PROPABILITY_FRACTION, Double.toString(danglingPropability));
        Job computePageRankJob = new Job(computePageRankConf, "ComputePageRankJob");
        computePageRankJob.setJarByClass(PageRank.class);
        computePageRankJob.setMapperClass(ComputePagerankMapper.class);
        computePageRankJob.setReducerClass(ComputePagerankReducer.class);

        computePageRankJob.setOutputKeyClass(Text.class);
        computePageRankJob.setOutputValueClass(Text.class);

        // Mapper has different output
        computePageRankJob.setMapOutputKeyClass(Text.class);
        computePageRankJob.setMapOutputValueClass(ResourceValue.class);

        TextInputFormat.addInputPath(computePageRankJob, inputPathFS);
        TextOutputFormat.setOutputPath(computePageRankJob, outputPathFS);
        // Delete output file if it exists
        removeIfExists(outputPath, computePageRankConf);
        // Run job
        computePageRankJob.waitForCompletion(true);
        // Is the file present ?
        return FileSystem.get(computePageRankConf).exists(outputPathFS);
    }

    private boolean computeDanglingPart(String inputPath, String outputPath, int resourceCount, double s, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        // Compute inner product of actual pageRank vector p 
        // and the transformed dangling vector d.
        // The jth entry of d is 1, iff the jth resource is a dangling resource,
        // otherwise 0.
        // That means, according to the inner product definition, we only have
        // sum the dangling node page ranks
        // The resulting inner product must be divided by the number of resources
        // and multiplied with the daming factor s.
        // The division distributes the proboability of getting from a dangling node
        // to another node over all nodes.
        // The damping factor represents how often this instead of teleporting appears

        LogFactory.getLog(ComputePageRankState.class).info("Before danglingPart computation");
        // Input path
        Path inputPathFS = new Path(inputPath);
        Path outputPathFS = new Path(outputPath);
        // Set up job to find out outdegrees
        Configuration computeInnerProductConf = new Configuration(conf);
        Job computeInnerProductJob = new Job(computeInnerProductConf, "ComputeInnerProduct");
        computeInnerProductJob.setJarByClass(PageRank.class);
        computeInnerProductJob.setMapperClass(ComputeInnerProductMappper.class);
        computeInnerProductJob.setReducerClass(ComputeInnerProductReducer.class);
        computeInnerProductJob.setCombinerClass(ComputeInnerProductReducer.class);

        computeInnerProductJob.setOutputKeyClass(Text.class);
        computeInnerProductJob.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(computeInnerProductJob, inputPathFS);
        TextOutputFormat.setOutputPath(computeInnerProductJob, outputPathFS);
        // Delete output file if it exists
        removeIfExists(outputPath, computeInnerProductConf);
        // Run job
        computeInnerProductJob.waitForCompletion(true);
        // Is the file present ?
        if (FileSystem.get(computeInnerProductConf).exists(outputPathFS)) {
            // Retrieve content of file
            danglingPropability = getFileContent(Double.class, computeInnerProductConf, outputPath, Conf.INNER_PRODUCT);
            LogFactory.getLog(this.getClass()).info("Danglingpropability after retrieval: "+danglingPropability);
            LogFactory.getLog(this.getClass()).info("Resource count: "+resourceCount);
            // Divide by resourceCount
            danglingPropability /= resourceCount;
            LogFactory.getLog(this.getClass()).info("Danglingpropability after division through resourcecount: "+danglingPropability);
            // Multiply with s
            danglingPropability *= s;
            LogFactory.getLog(this.getClass()).info("Danglingpropability after multiplication with s: "+danglingPropability);
            return true;
        }
        return false;

    }
}
