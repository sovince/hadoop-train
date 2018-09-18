package com.sovince.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://hadoop000:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;




    /**
     * 创建文件夹
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/mbp"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream out = fileSystem.create(new Path("/mbp/vince.txt"));
        out.write("This is Vince's MacBookPro".getBytes());
        out.flush();
        out.close();
    }

    /**
     * 读取文件
     * @throws Exception
     */
    @Test
    public void cat() throws Exception{
//        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi3/uninstall.log"));
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi3/b.txt"));
        IOUtils.copyBytes(in,System.out,1024);
        in.close();
    }

    /**
     * 重命名文件
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/mbp/bob.txt");
        Path newPath = new Path("/mbp/anne.txt");
        fileSystem.rename(oldPath,newPath);
    }



    @Before
    public void setUp() throws Exception{
        System.out.println("Start hdfs-test");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
    }

    @After
    public void tearDown(){
        fileSystem = null;
        configuration = null;
        System.out.println();
        System.out.println("End hdfs-test");
    }

}
