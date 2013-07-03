package de.gtrefs.pagerank.state;

import de.gtrefs.pagerank.PageRank;
import de.gtrefs.pagerank.configuration.Conf;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 * Recovers the computation process, if it was aborted.
 * Scans for {@link GatherInformationState} and {@link ComputePageRankState} results.
 * If an according result was found <code>RecoverState</code> directly goes to the {@link ComputePageRankState}. 
 * @author Gregor Trefs
 */
public class RecoverState extends AbstractState {

    private int retrieveIntermediateResultNumber(Configuration conf) throws IOException {
        int lastFoundIntermediate = -1;
        for (int i = 0; i < configuration.getComputationResultEnd(); i++) {
            if (exists(configuration.getUnsortedResultPath() + Conf.INTERMEDIATE_RESULT_PATH_SUFFIX + i, conf)) {
                lastFoundIntermediate = i;
            }
        }
        return lastFoundIntermediate;
    }

    private Integer extractResourceCount(Configuration conf) throws IOException {
        // Retrieve content of file
        return getFileContent(Integer.class, conf, configuration.getCountFilePath(), Conf.RESOURCE_COUNT);
    }

    @Override
    public void executeJob(PageRank rank) throws IOException, InterruptedException, ClassNotFoundException {
        // Does the informationGathering result exist ?
        boolean gatherInformationResultExist = exists(configuration.getInformationGatheringPath(), rank.getConf());
        // The number of the found intermediate result
        int numberOfFoundIntermediateResult = retrieveIntermediateResultNumber(rank.getConf());
        // Extracted resource count
        Integer resourceCount = extractResourceCount(rank.getConf()); 
        // If any intermediate result has been foud and resource count could be retrieved
        // Set computation result start
        // Set resource count
        // Set ComputePageRankState
        // and return
        if ((gatherInformationResultExist || numberOfFoundIntermediateResult > -1) && resourceCount != null) {
            configuration.setComputationResultStart(numberOfFoundIntermediateResult > -1 ? (numberOfFoundIntermediateResult + 1) : 0);
            rank.setResourceCount(resourceCount);
            rank.setState(new ComputePageRankState());
            return;
        }
        // Else start process from the beginning
        configuration.setComputationResultStart(0);
        rank.setState(new GatherInformationState());
    }
}
