package com.elaina;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
        //副本数设置
//        configuration.set("dfs.replication", "2");
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

    /**
     * 上传
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 2 上传文件
        fs.copyFromLocalFile(false, false, new Path("H:\\imgs\\bb.jpg"), new Path("/2021/bb.jpg"));
        System.out.println("over");
    }

    /**
     * 文件下载
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/2021/bb.jpg"), new Path("C:\\Users\\12782\\Desktop\\bingbing.jpg"), true);
    }

    /**
     * 删除路径或者目录
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException {
        // 2 执行删除
        fs.delete(new Path("/2021/linzhi"), true);
    }

    /**
     * 文件更名或者移动
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException {
        // 2 修改文件名称
        fs.rename(new Path("/2021/bb.jpg"), new Path("/冰冰.jpg"));

    }

    /**
     * HDFS文件详情查看
     * 查看文件名称、权限、长度、块信息
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("---------------------");
        }

    }

    /**
     * HDFS文件和文件夹判断
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{


        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("文件:"+fileStatus.getPath().getName());
            }else {
                System.out.println("文件夹:"+fileStatus.getPath().getName());
            }
        }


    }

}
