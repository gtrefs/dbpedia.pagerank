/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gtrefs.pagerank.mapred.strategy;

import de.gtrefs.pagerank.configuration.Conf;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Factory for {@link Strategy}s.
 * <ul>
 *   <li>{@link WikipediaStrategy}: Used for the Wikipedia link structure and cloudera hadoop 0.20-cdh3</li>
 *   <li>{@link DBPediaStrategy}: Used for DBPedia link structure and hadoop 0.20.203.0</li>
 * </ul>
 * @author Gregor Trefs
 */
public class StrategyFactory {

    private static StrategyFactory INSTANCE = new StrategyFactory();

    public static StrategyFactory getInstance() {
        return INSTANCE;
    }

    public Strategy getStrategy(Strategy.Type type) {
        if (type.compareTo(Strategy.Type.DBPEDIA) == 0) {
            return new DBPediaStrategy();
        }
        if (type.compareTo(Strategy.Type.WIKIPEDIA) == 0) {
            return new WikipediaStrategy();
        }
        return null;
    }

    public Strategy determineStrategy(String filterType) {
        // Determine enum type
        for (Strategy.Type type : Strategy.Type.values()) {
            if (type.toString().toLowerCase().equals(filterType.toLowerCase())) {
                // Return filter
                return getStrategy(type);
            }
        }
        return null;
    }

    public class WikipediaStrategy implements Strategy {

        // Dangling cache keeps emitted dangling key-value pairs in mind.
        // Thus, a corresponding dummy outlink is emitted only once per
        // InputSplit.
        private Map<String, String> danglingCache;

        private WikipediaStrategy() {
            danglingCache = new HashMap<String, String>();
        }

        @Override
        public String[] filterResourceCountSplitMapper(String triple) {
            // Split triple into tokens
            String split[] = triple.split("\\s");
            String subjectAndObject[] = new String[]{split[0], split[2]};
            // Filter for articels (URLs without namespace prefic)
            
            // First token is a resource and third token is a resource
            return new String[]{split[0], split[2]};
        }

        @Override
        public void filterGatherInformationMapper(LongWritable key, Text value, Context context, Text outKey, Text outValue) throws IOException, InterruptedException {
            // Split line up
            String split[] = value.toString().split("\\s");
            // key = subject, value = opject
            outKey.set(split[0]);
            outValue.set(split[2]);
            context.write(outKey, outValue);
            // resource in split[0] is no dangling node
            // Therefore no dummy outlink has to be emited
            // --> Write it into the cache
            danglingCache.put(split[0], split[0]);
            // Emmit another key/value pair for object with dummy as output link
            // This is to gather all dangling links
            if (danglingCache.put(split[2], split[2]) == null) {
                outKey.set(split[2]);
                outValue.set(Conf.DUMMY_OUTLINK);
                context.write(outKey, outValue);
            }
        }

        @Override
        public Type getType() {
            return Strategy.Type.WIKIPEDIA;
        }
    }

    public class DBPediaStrategy implements Strategy {

        // Dangling cache keeps emitted dangling key-value pairs in mind.
        // Thus, a corresponding dummy outlink is emitted only once per
        // InputSplit.
        private Map<String, String> danglingCache;

        private DBPediaStrategy() {
            danglingCache = new HashMap<String, String>();
        }

        @Override
        public String[] filterResourceCountSplitMapper(String triple) {
            // Objects may be literals. Look deeper into split[2]
            // Split triple into tokens
            String split[] = triple.split("\\s");
            String[] ret = split[2].startsWith("<http://dbpedia.org/resource/") ? new String[]{split[0], split[2]} : new String[]{split[0]};
            return ret;
        }

        @Override
        public void filterGatherInformationMapper(LongWritable key, Text value, Context context, Text outKey, Text outValue) throws IOException, InterruptedException {
            // If the split[2] is a resource, emit
            // Otherwise continue
            // Split line up
            String split[] = value.toString().split("\\s");
            if (split[2].startsWith("<http://dbpedia.org/resource/")) {
                // key = subject, value = opject
                outKey.set(split[0]);
                outValue.set(split[2]);
                context.write(outKey, outValue);
                // resource in split[0] is no dangling node
                // Therefore no dummy outlink has to be emited
                // --> Write it into the cache
                danglingCache.put(split[0], split[0]);
                // Emmit another key/value pair for object with dummy as output link
                // This is to gather all dangling links
                if (danglingCache.put(split[2], split[2]) == null) {
                    outKey.set(split[2]);
                    outValue.set(Conf.DUMMY_OUTLINK);
                    context.write(outKey, outValue);
                }
            } else {
                // Emit dummy link for split[0]
                if (danglingCache.put(split[0], split[0]) == null) {
                    outKey.set(split[0]);
                    outValue.set(Conf.DUMMY_OUTLINK);
                    context.write(outKey, outValue);
                }
            }
        }

        @Override
        public Type getType() {
            return Strategy.Type.DBPEDIA;
        }
    }
}
