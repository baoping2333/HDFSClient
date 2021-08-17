package com.elaina;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @program: HDFSClient
 * @ClassName HdfsClient
 * @description:
 * @author: wanbaoping
 * @create: 2021-08-17 00:35
 * @Version 1.0
 **/
public class HdfsClient {
    private FileSystem fs;

    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop102:9820"), configuration, "atguigu");
    }

    @After
    public void close() throws Exception {
        fs.close();
    }

    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        fs.mkdirs(new Path("/2021/linzhi"));

    }

}
