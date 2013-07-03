package de.gtrefs.pagerank.configuration;

import de.gtrefs.pagerank.mapred.strategy.Strategy;
import de.gtrefs.pagerank.mapred.strategy.StrategyFactory;

/**
 * POJO for some additional information for running the jobs.
 * @author Gregor Trefs
 */
public class Conf {

    // resource count key
    public static String RESOURCE_COUNT = "graph_resourcecount";
    // Intermediate result of some states
    public static String INTERMEDIATE_RESULT_PATH_SUFFIX = "Intermediate";
    // Map & Reduce dummy resource to count dangling links
    public static String DUMMY_OUTLINK = "dummy";
    // OutNodeListSeperator
    public static String OUT_NODE_LIST_SEPERATOR = "|";
    // OutNodeListSeperator matcher expr. for regex
    public static String OUT_NODE_LIST_SEPERATOR_MATCHER = "\\|";
    // Empty Node list
    public static String EMPTY_NODE_LIST = "empty";
    // dumping factor s
    public static double S = 0.85;
    // How often should the PageRank be computed ?
    public static int DEFAULT_PAGERANK_LOOPS = 20;
    // Inner product key
    public static String INNER_PRODUCT = "inner_product";
    // Dangling propability fraction key
    public static String DANGLING_PROPABILITY_FRACTION = "dangling_propability_fraction";
    // Intialization
    // Property names for parameters on command line
    public static String COUNT_FILE_PATH = "pagerank.path.file.count";
    public static String INFORMATION_GATHERING_PATH = "pagerank.path.gathering.information";
    public static String GRAPH_FILE_PATH = "pagerank.path.file.graph";
    public static String COMPUTE_DANGLING_PATH = "pagerank.path.dangling.compute";
    public static String UNSORTED_RESULT_PATH = "pagerank.path.result.unsorted";
    public static String SORTED_RESULT_PATH = "pagerank.path.result.sorted";
    public static String KEEP_GATHER_INFORMATION_FILES = "pagerank.files.information.gathering.keep";
    public static String FILTER_TYPE = "pagerank.type.filter";
    public static String COMPUTATION_RESULT_START = "pagerank.computation.result.start";
    public static String COMPUTATION_RESULT_END = "pagerank.computation.result.end";
    public static String DELETE_INFORMATION_GATHERING = "pagerank.gathering.information.delete";
    public static String RECOVER = "pagerank.recover";
    public static String RESULT_PREFIX_NAME = "pagerank.name.prefix.result";
    private String countFilePath;
    private String informationGatheringPath;
    private String graphFilePath;
    private String computeDanglingPath;
    private String unsortedResultPath;
    private String sortedResultPath;
    private Strategy strategy;
    private int computationResultStart;
    private int computationResultEnd;
    private boolean deleteInformationGatheringInformation;
    private boolean recover;
    private String resultPrefixName;
    
    // IO
    // Delete or keep existing files ?
    private boolean keepGatherInformationFiles = false;
    // Singleton
    private static Conf INSTANCE = new Conf();

    private Conf() {
    }

    public boolean validate() {
        // Look whether everything has been initialized, before continue
        boolean ret = countFilePath != null && informationGatheringPath != null && graphFilePath != null
                && computeDanglingPath != null && unsortedResultPath != null && sortedResultPath != null
                && strategy != null;
        return ret;
    }

    public boolean determineStrategy(String strategyType) {
        strategy = StrategyFactory.getInstance().determineStrategy(strategyType);
        return strategy != null;
    }

    public static Conf getInstance() {
        return INSTANCE;
    }

    public String getCountFilePath() {
        return countFilePath;
    }

    public void setCountFilePath(String countFilePath) {
        this.countFilePath = countFilePath;
    }

    public String getInformationGatheringPath() {
        return informationGatheringPath;
    }

    public void setInformationGatheringPath(String informationGatheringPath) {
        this.informationGatheringPath = informationGatheringPath;
    }

    public String getGraphFilePath() {
        return graphFilePath;
    }

    public void setGraphFilePath(String graphFilePath) {
        this.graphFilePath = graphFilePath;
    }

    public String getComputeDanglingPath() {
        return computeDanglingPath;
    }

    public void setComputeDanglingPath(String computeDanglingPath) {
        this.computeDanglingPath = computeDanglingPath;
    }

    public String getUnsortedResultPath() {
        return unsortedResultPath;
    }

    public void setUnsortedResultPath(String unsortedResultPath) {
        this.unsortedResultPath = unsortedResultPath;
    }

    public String getSortedResultPath() {
        return sortedResultPath;
    }

    public void setSortedResultPath(String sortedResultPath) {
        this.sortedResultPath = sortedResultPath;
    }

    public boolean isKeepGatherInformationFiles() {
        return keepGatherInformationFiles;
    }

    public void setKeepGatherInformationFiles(boolean keepGatherInformationFiles) {
        this.keepGatherInformationFiles = keepGatherInformationFiles;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setFilter(Strategy filter) {
        this.strategy = filter;
    }

    public int getComputationResultEnd() {
        return computationResultEnd;
    }

    public void setComputationResultEnd(int computationResultEnd) {
        this.computationResultEnd = computationResultEnd;
    }

    public int getComputationResultStart() {
        return computationResultStart;
    }

    public void setComputationResultStart(int computationResultStart) {
        this.computationResultStart = computationResultStart;
    }

    public boolean isDeleteInformationGatheringInformation() {
        return deleteInformationGatheringInformation;
    }

    public void setDeleteInformationGatheringInformation(boolean deleteInformationGatheringInformation) {
        this.deleteInformationGatheringInformation = deleteInformationGatheringInformation;
    }

    public boolean isRecover() {
        return recover;
    }

    public void setRecover(boolean recover) {
        this.recover = recover;
    }

    public String getResultPrefixName() {
        return resultPrefixName;
    }

    public void setResultPrefixName(String resultPrefixName) {
        this.resultPrefixName = resultPrefixName;
    }
}
