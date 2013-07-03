package de.gtrefs.pagerank.state;

import de.gtrefs.pagerank.PageRank;
import java.io.IOException;

/**
 *
 * @author Gregor Trefs
 */
public interface State {
    public void executeJob(PageRank rank) throws IOException, InterruptedException, ClassNotFoundException;  
}
