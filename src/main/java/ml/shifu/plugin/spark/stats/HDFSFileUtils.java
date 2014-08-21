/*
 * Contains utils for dealing with HDFS or local filesystems specific to the spark normalization code.
 */
package ml.shifu.plugin.spark.stats;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class HDFSFileUtils {

    private Configuration hdfsConf;

    // Reads core-site.xml and hdfs-site.xml from provided hadoopConfPath and
    // creates a Configuration object
    // that is used to create instance of HDFS FileSystem.
    // FileSystem instance is created whenever required and then closed to avoid
    // hoarding of resource.
    public HDFSFileUtils(String hadoopConfPath) throws IOException {
        this.hdfsConf = new Configuration();
        // TODO: use a path joiner(?)
        Path coreSitePath = new Path(hadoopConfPath + "/" + "core-site.xml");
        Path hdfsSitePath = new Path(hadoopConfPath + "/" + "hdfs-site.xml");
        this.hdfsConf.addResource(coreSitePath);
        this.hdfsConf.addResource(hdfsSitePath);
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(this.hdfsConf);
        } catch (IOException e) {
            System.out
                    .println("ERROR: Could not create instance of filesystem");
            e.printStackTrace();
        }

        if (hdfs instanceof LocalFileSystem) {
            System.out
                    .println("ERROR: Could not create instance of hdfs FileSystem. Please check hadoop configuration files, got path " + hadoopConfPath);
            throw new IOException();
        }

        if (hdfs != null)
            hdfs.close();
    }


    // deletes files from either local/ hdfs filesystems
    public boolean delete(String strPath) {
        Path p = new Path(strPath);
        System.out.println("Deleting file " + p.toString());
        FileSystem fs = null;
        try {
            fs = p.getFileSystem(this.hdfsConf);
        } catch (IOException e1) {
            System.out.println("Cannot obtain FileSystem for " + p.toString());
            e1.printStackTrace();
        }

        try {
            return fs.delete(p, true);
        } catch (IOException e) {
            System.out.println("Cannot delete file " + p.toString());
            e.printStackTrace();
        }
        try {
            if (fs != null)
                fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    // gets full URI of file on HDFS
    public String getHDFSUri() throws IOException {
        FileSystem hdfs = FileSystem.get(this.hdfsConf);
        String Uri = hdfs.getUri().toString();
        hdfs.close();
        return Uri;
    }

    public String uploadToHDFS(String localPath, String HDFSDir)
            throws Exception {
        FileSystem fs = FileSystem.get(this.hdfsConf);
        String basename = new Path(localPath).getName().toString();
        Path HDFSPath = new Path(HDFSDir + "/" + basename);
        fs.copyFromLocalFile(new Path(localPath), HDFSPath);
        fs.close();
        return HDFSPath.toString();
    }
    
    /*
     *  uploads localPath to HDFSDir if localPath is on the local filesystem, and returns path of file on HDFS. Treats localPath as local if no scheme is specified.
     */
    public String uploadToHDFSIfLocal(String localPath, String HDFSDir)
            throws Exception {
        if (localPath.startsWith("hdfs:"))
            return localPath;
        String basename = new Path(localPath).getName().toString();
        System.out.println("Uploading " + basename + " to HDFS");
        Path HDFSPath = new Path(HDFSDir + "/" + basename);
        FileSystem hdfs = FileSystem.get(this.hdfsConf);
        hdfs.copyFromLocalFile(new Path(localPath), HDFSPath);
        hdfs.close();
        return relativeToFullHDFSPath(HDFSPath.toString());
    }

    /*
     *  gets home dir of HDFS filesystem
     */
    public String getHDFSHomeDir() throws IOException {
        FileSystem hdfs = FileSystem.get(this.hdfsConf);
        String homeDir = hdfs.getHomeDirectory().toString();
        hdfs.close();
        return homeDir;
    }

    public String relativeToFullHDFSPath(String relPath) throws IOException {
        if (relPath.startsWith("hdfs:") || relPath.startsWith("file:"))
            return relPath;
        if (relPath.startsWith("/")) {
            // relPath relative to root
            return this.getHDFSUri() + relPath;
        } else {
            // assume that path is relative to home
            return this.getHDFSHomeDir() + "/" + relPath;
        }
    }


    // concatenates all files in dirpath to target. Currently does not use
    // PathFilter.
    // However, all extraneous files created in output path by Spark are empty,
    // so filter not necessary.
    public void concat(String target, String dirpath, PathFilter filter)
            throws IllegalArgumentException, IOException {
        // now concatenate
        Path targetPath = new Path(target);
        FileSystem targetFS = targetPath.getFileSystem(this.hdfsConf);
        FileSystem hdfs = FileSystem.get(this.hdfsConf);
        targetFS.delete(targetPath, false);
        FileUtil.copyMerge(hdfs, new Path(dirpath), targetFS, new Path(target),
                true, this.hdfsConf, "");

        targetFS.close();
        hdfs.close();
    }

    public void createEmptyFile(String strPath) throws IOException {
        Path path = new Path(strPath);
        FileSystem fs = path.getFileSystem(this.hdfsConf);
        fs.create(path);
    }
    
    public String fullPath(String path) throws IOException {
        if(path.startsWith("hdfs") || path.startsWith("file"))
            return path;
        // path is local. 
        else if(path.startsWith("/"))
            return "file://" + path;
        else if(path.startsWith("."))
            return path;    // TODO: convert to full path by getting current working dir
        else {   // path is relative to home of user
            // if path starts with ~/ remote that portion
            if(path.startsWith("~"))
                if(path.length() > 2)
                    path= path.substring(2);
                else
                    path= "";
            FileSystem localFS= FileSystem.get(new Configuration());
            Path homePath= localFS.getHomeDirectory();
            localFS.close();
            return homePath.toString() + "/" + path;
        }
    }
    
    /*
     * Checks if a path is local.In absence of a schema, path is considered local by default.
     */
    public boolean isLocal(String path) {
        return !isHDFS(path);
    }
    
    public boolean isHDFS(String path) {
        return path.startsWith("hdfs:");
    }
    
    /*
     * Universal copy method which copies file from local/hdfs to local/hdfs
     */
    public void copy(String source, String dest) throws IOException {
        source= fullPath(source);
        dest= fullPath(dest);
        FileSystem hdfs= FileSystem.get(hdfsConf);
        FileSystem local= FileSystem.get(new Configuration());
        
        if(isLocal(source) && isHDFS(dest)) {
            hdfs.copyFromLocalFile(new Path(source), new Path(dest));
        }
        else if(isHDFS(source) && isLocal(dest)) {
            hdfs.copyToLocalFile(new Path(source), new Path(dest));
        }
        else if(isHDFS(source) && isHDFS(dest)) {
            FileUtil.copy(hdfs, new Path(source), hdfs, new Path(dest), false, hdfsConf);
        }
        else {  // both paths local
            FileUtil.copy(local, new Path(source), local, new Path(dest), false, new Configuration());
        }
            
    }
}
