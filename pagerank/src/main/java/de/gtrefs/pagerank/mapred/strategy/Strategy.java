/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gtrefs.pagerank.mapred.strategy;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * <code>Strategy</code>s encapsulate source specific information and methods.
 * {@link StrategyFactory} generates the <code>Strategy</code>s, determined by the {@link Type} enum.
 * Two <code>Strategy</code>s exist:
 * <ul>
 *  <li>{@link StrategyFactory.WikipediaStrategy}</li>
 *  <li>{@link StrategyFactory.DBPediaStrategy}</li>
 * </ul>
 * @author Gregor Trefs
 * @see FilterFactory
 * @see http://en.wikipedia.org/wiki/Strategy_pattern
 */
public interface Strategy {
    
    /**
     * Enum 
     */
    public enum Type{
        WIKIPEDIA,DBPEDIA
    }
    
    /**
     * Filters the given triple for resources which should be counted.
     * @param triple &lt;subject&gt; &lt;predicate&gt; &lt;object&gt;
     * @return filtered resources
     */
    public String[] filterResourceCountSplitMapper(String triple);
    
    /**
     * Filters the given value for resources which should be gathered.
     * @param key
     * @param value
     * @param context
     * @param outKey
     * @param outValue
     * @throws IOException
     * @throws InterruptedException 
     */
    public void filterGatherInformationMapper(LongWritable key, Text value, Context context, Text outKey, Text outValue) throws IOException, InterruptedException;

    /**
     * Returns the filter type.
     * @return 
     */
    public Type getType();
}
