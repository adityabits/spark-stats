package ml.shifu.plugin.spark.stats;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ml.shifu.core.request.Request;
import ml.shifu.core.util.JSONUtils;
import ml.shifu.core.util.Params;

public class HDFSUtilsTest {
    HDFSFileUtils hdfsUtils;
    
    @BeforeTest
    public void instantiate() throws IOException {
        Request req=  JSONUtils.readValue(new File("src/test/resources/spark_stats.json"), Request.class); 
        Params params= req.getProcessor().getParams();
        String hdfsConf= params.get("pathHDFSConf", "/usr/lib/hadoop/etc/hadoop").toString();
        hdfsUtils= new HDFSFileUtils(hdfsConf);
        
    }
    
    @Test
    public void testDelete() throws IOException, IllegalArgumentException, URISyntaxException {
        // create a file in local filesystem
        File testFile= new File("src/test/resources/HDFSTest/dummy");
        FileUtils.touch(testFile); 
        System.out.println("local testfile: " + testFile.toURI().toString());
        hdfsUtils.delete(testFile.toURI().toString());
        Assert.assertFalse(hdfsUtils.exists(testFile.getPath()));
        FileUtils.deleteQuietly(testFile);
        
        // test on HDFS
        Path testPath= new Path(hdfsUtils.relativeToFullHDFSPath("hdfs:///ml/shifu/test/dummy"));
        FileSystem hdfs= FileSystem.get(hdfsUtils.getHDFSConf());
        System.out.println("hdfs testfile: " + testPath.toString());
        hdfs.createNewFile(testPath);
        hdfs.close();
        hdfsUtils.delete(testPath.toString());
        Assert.assertFalse(hdfsUtils.exists(testPath.toString()));
        
        FileSystem hdfs1= FileSystem.get(hdfsUtils.getHDFSConf());
        hdfs1.delete(testPath, true);
        hdfs1.close();
    }
    
    
    @Test
    public void uploadToHDFSIfLocalTest() throws Exception {
        File testFile= new File("src/test/resources/HDFSTest/dummy");
        FileUtils.touch(testFile);
        hdfsUtils.uploadToHDFSIfLocal(testFile.getAbsolutePath(), "hdfs://ml/shifu/test");
        Assert.assertTrue(hdfsUtils.exists("hdfs://ml/shifu/test/dummy"));
        hdfsUtils.delete("hdfs://ml/shifu/test/dummy");
        
        // check with hdfs file
        hdfsUtils.createEmptyFile("hdfs://ml/shifu/test1/dummy");
        hdfsUtils.uploadToHDFSIfLocal("hdfs://ml/shifu/test1/dummy", "hdfs://ml/shifu/test");
        Assert.assertFalse(hdfsUtils.exists("hdfs://ml/shifu/test/dummy"));
        hdfsUtils.delete("hdfs://ml/shifu/test1");
    }
    
    @Test 
    public void fullDefaultLocalTest() throws IOException, URISyntaxException {
        Assert.assertTrue(hdfsUtils.fullDefaultLocal("/home/user").equals("file:///home/user"));        
    }
}
