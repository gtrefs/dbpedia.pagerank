package de.gtrefs.pagerank.mapred.gatherInformation;

import de.gtrefs.pagerank.configuration.Conf;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author Gregor Trefs
 */
public class GatherInformationReducer extends Reducer<Text, Text, Text, Text> {

    private int resourceCount;
    private double initialPageRank;
    private Text value;
    // private Log log;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // log = LogFactory.getLog(GatherInformationReducer.class);
        resourceCount = context.getConfiguration().getInt(Conf.RESOURCE_COUNT, -1);
        initialPageRank = 1d / resourceCount;
        // log.info("Resource count received: "+resourceCount);
        // log.info("Initial page rank: "+initialPageRank);
        value = new Text();
    }

    private String removeOutNodeListSeperatorAtTheEnd(StringBuilder outNodeList) {
        // Remove possible added OUT_NODE_LIST_SEPERARATOR from outNodeList
        if (outNodeList.length() > 0 && outNodeList.charAt(outNodeList.length() - 1) == Conf.OUT_NODE_LIST_SEPERATOR.charAt(0)) {
            outNodeList.deleteCharAt(outNodeList.length() - 1);
        }
        return outNodeList.toString();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Current outnodelist
        StringBuilder outNodeList = new StringBuilder();
        int outNodeCount = 0;
        Iterator<Text> it = values.iterator();
        while (it.hasNext()) {
            Text next = it.next();
            // Don't do anything with dummys
            if (next.toString().equals(Conf.DUMMY_OUTLINK)) {
                continue;
            }
            // Append it to the outNodeList
            outNodeList.append(next.toString());
            // Append seperator
            if (it.hasNext()) {
                outNodeList.append(Conf.OUT_NODE_LIST_SEPERATOR);
            }
            // Increase node count
            outNodeCount++;
        }

        // Create output
        // [key] [pageRank] [OutNodeCount] {[listOfOutnodes]|Conf.EMPTY_NODE_LIST}
        value.set(initialPageRank + " " + outNodeCount + " " + (outNodeCount == 0 ? Conf.EMPTY_NODE_LIST : removeOutNodeListSeperatorAtTheEnd(outNodeList)));
        context.write(key, value);
    }
}
