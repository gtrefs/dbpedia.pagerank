package de.gtrefs.pagerank.state;

import de.gtrefs.pagerank.configuration.Conf;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author Gregor Trefs
 */
public abstract class AbstractState implements ConfigurableState {

    protected Conf configuration;

    @Override
    public void init(Conf initData) {
        this.configuration = initData;
    }
    
    protected void logLocation(){
        LogFactory.getLog(this.getClass()).info("In state: "+this.getClass().getName());
    }

    protected boolean removeIfExists(String stringPath, Configuration conf) throws IOException {
        // Create the path
        Path path = new Path(stringPath);
        // Get the FileSystem
        FileSystem fs = FileSystem.get(conf);
        // Remove file
        return fs.delete(path, true);
    }
    
    protected boolean exists(String stringPath, Configuration conf) throws IOException {
        // Create the path
        Path path = new Path(stringPath);
        // Get the FileSystem
        FileSystem fs = FileSystem.get(conf);
        // Exists ?
        return fs.exists(path);
    }

    protected <T extends Number> T getFileContent(Class<T> returnType, Configuration conf, String filePath, String key) throws IOException {
        // Get file system
        FileSystem fs = FileSystem.get(conf);
        // While there are success files, search for the right one
        String baseName = configuration.getResultPrefixName();
        T ret = null;
        int i = 0;
        Path path = new Path(filePath + "/" + baseName + String.format("%05d", i));
        while (fs.exists(path)) {
            if (fs.isFile(path)) {
                FSDataInputStream dataStream = fs.open(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(dataStream));
                // read line
                String line = reader.readLine();
                // close streams
                reader.close();
                dataStream.close();
                // Split
                String[] split = line.split("\\s");
                // is it resource count
                if (split[0].equals(key)) {
                    // Convert second to T and return
                    if (returnType.equals(Integer.class)) {
                        ret = (T) Integer.valueOf(split[1]);
                    }
                    if (returnType.equals(Double.class)) {
                        ret = (T) Double.valueOf(split[1]);
                    }
                    return ret;
                }
            }
            path = new Path(filePath + "/" + baseName + String.format("%05d", ++i));
        }
        return ret;
    }
}
