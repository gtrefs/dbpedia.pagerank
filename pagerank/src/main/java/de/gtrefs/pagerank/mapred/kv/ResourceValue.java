package de.gtrefs.pagerank.mapred.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Gregor Trefs
 */
public class ResourceValue implements Writable{
    
    private Text resourceDescription;
    private DoubleWritable pageRankFraction;
    
    public ResourceValue(){
        resourceDescription = new Text("");
        pageRankFraction = new DoubleWritable(0);
    }
    
    public boolean isDescription(){
        return resourceDescription.getLength()!=0;
    }

    public double getPageRankFraction() {
        return pageRankFraction.get();
    }

    public void setPageRankFraction(double pageRankFraction) {
        resourceDescription.set("");
        this.pageRankFraction.set(pageRankFraction);
    }

    public String getResourceDescription() {
        return resourceDescription.toString();
    }

    public void setResourceDescription(String resourceDescription) {
        pageRankFraction.set(0);
        this.resourceDescription.set(resourceDescription);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write out
        resourceDescription.write(out);
        pageRankFraction.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        resourceDescription.readFields(in);
        pageRankFraction.readFields(in);
    }
    
    public static ResourceValue read(DataInput in) throws IOException{
        ResourceValue rv = new ResourceValue();
        rv.readFields(in);
        return rv;
    }
    
}