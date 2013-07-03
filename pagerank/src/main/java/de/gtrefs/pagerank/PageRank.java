package de.gtrefs.pagerank;

import de.gtrefs.pagerank.configuration.Conf;
import de.gtrefs.pagerank.state.ConfigurableState;
import de.gtrefs.pagerank.state.FinishedState;
import de.gtrefs.pagerank.state.GatherInformationState;
import de.gtrefs.pagerank.state.RecoverState;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * PageRank Computation for RDF-Graphs. <p> <h1>Definition</h1> The PageRank of
 * a graph g with n nodes is a vector with n elements, where an element is a
 * real number from 0 to 1. The nth number denotes the propability of being at
 * node n in time t. Therefore, the PageRank is a propability distribution over
 * g.
 *
 * <h1>Idea</h1> The PageRank algorithm was invented by Brin and Page to solve
 * the fundamental question of any search engine: How relevant is the retrieved
 * information for the questioner ? PageRank, thereby, relies on the information
 * which is encoded in the linking structure between documents. A document with
 * many incoming links is more important than one with only a few. And a
 * document which is linked by an important document is also important. That is,
 * the importance of document is defined by the number of incoming links and the
 * importance of the linking documents.
 *
 * <h1>Computaional Model</h1> PageRank employs the so-called random surfer
 * model: A user which traverses the (web) graph by choosing outgoing links on a
 * random basis and sometimes restarts his/hers traversal at a random node.
 * Mathematically, this can be expressed in Markov chain terms. The current
 * state of a graph has the propability distribution of p (= PageRank Vector).
 * Transition from one graph state to another is moving from one document to
 * another or going to a random node. The propability of a transition from
 * document d1 to another linked document d2 is depicted as:
 * p(d1)/#outlinks(d1). Where #outlinks(d1) is the number of links to other
 * documents. The propabilty of a transition from a document to a random
 * document is defined as 1/numberOfDocuments.
 * <code>p<sub>t+1</sub>=p<sub>t</sub>*M</code> where M is the transition
 * propability Matrix.
 * <code>M = s * A + t * E </code> where s is 0.85 and t = 1 - s.
 *
 * @author Gregor Trefs
 */
public class PageRank extends Configured implements Tool {

    // State of the PageRank
    private ConfigurableState state;
    // The total count of resources in the graph
    int resourceCount = 0;
    // current configuration
    private Conf configuration;

    /**
     * @param args the command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
    }

    /**
     * 
     * @param state
     */
    public void setState(ConfigurableState state) {
        // Init state with current Configuration
        state.init(configuration);
        // Set state
        this.state = state;
    }

    /**
     *
     * @return
     */
    public int getResourceCount() {
        return resourceCount;
    }

    /**
     *
     * @param resourceCount
     */
    public void setResourceCount(int resourceCount) {
        this.resourceCount = resourceCount;
    }

    private boolean createConfiguration() {
        configuration = Conf.getInstance();
        // Set the configuration up
        // Input graph
        configuration.setGraphFilePath(getConf().get(Conf.GRAPH_FILE_PATH));//("/user/gtrefs/input/dbpedia/page_links_en.nt");
        // Set count output location
        configuration.setCountFilePath(getConf().get(Conf.COUNT_FILE_PATH));//("/user/gtrefs/output/countResource");
        // Set information gathering output location
        configuration.setInformationGatheringPath(getConf().get(Conf.INFORMATION_GATHERING_PATH));//("/user/gtrefs/output/informationGathering");
        // Set output path for dangling computation
        configuration.setComputeDanglingPath(getConf().get(Conf.COMPUTE_DANGLING_PATH));//("/user/gtrefs/output/dangling");
        // Set the unsorted result path
        configuration.setUnsortedResultPath(getConf().get(Conf.UNSORTED_RESULT_PATH));//("/user/gtrefs/output/unsortedResult");
        // Set the sorted result path
        configuration.setSortedResultPath(getConf().get(Conf.SORTED_RESULT_PATH));//("/user/gtrefs/output/sortedResult");
        // If there are intermediate results from previous runs, should they be used during the information gathering (default = false) ?
        configuration.setKeepGatherInformationFiles(getConf().getBoolean(Conf.KEEP_GATHER_INFORMATION_FILES, false));
        // Determiner filter
        configuration.determineStrategy(getConf().get(Conf.FILTER_TYPE));
        // Determine computation start and end
        configuration.setComputationResultStart(getConf().getInt(Conf.COMPUTATION_RESULT_START, 0));
        configuration.setComputationResultEnd(getConf().getInt(Conf.COMPUTATION_RESULT_END, Conf.DEFAULT_PAGERANK_LOOPS));
        // Determine whether to delete GatheredInformation or not
        configuration.setDeleteInformationGatheringInformation(getConf().getBoolean(Conf.DELETE_INFORMATION_GATHERING, false));
        // Recover from previous computation ?
        configuration.setRecover(getConf().getBoolean(Conf.RECOVER, true));
        // Result prefix ? Defaults to new "part-r-"
        configuration.setResultPrefixName(getConf().get(Conf.RESULT_PREFIX_NAME, "part-r-"));

        return configuration.validate();
    }

    /**
     *
     * @param strings
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] strings) throws Exception {
        // 1. Gather information about given graph
        // 2. Compute PageRank 20 times
        // 3. Save result in HDFS
        // Create configuration
        // If the configuration is not valid, exit
        if (!createConfiguration()) {
            LogFactory.getLog(PageRank.class).error("Some paths are null or filter could not be determined. Configuration is invalid.");
            return 1;
        }
        if (configuration.isRecover()) {
            // Set to new RecoverState
            setState(new RecoverState());
        } else {
            // Set to new GatherInformationState
            setState(new GatherInformationState());
        }
        // Execute state as long it is not the FinishedState
        while (!state.getClass().equals(FinishedState.class)) {
            try {
                // Execute
                state.executeJob(this);
            } catch (IOException ex) {
                Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }
}
