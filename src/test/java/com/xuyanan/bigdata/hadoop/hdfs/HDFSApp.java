package com.xuyanan.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://192.168.43.113:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://192.168.43.113:8020"), configuration, "hadoop");
    }

    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void copyFromLocalBigFile() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("D:\\Downloads\\VMware-player-16.1.2-17966106.exe")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/VMware-player-16.1.2-17966106.exe"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.println(".");
                    }
                });
        IOUtils.copyBytes(in, out, 4096);
    }

    @Test
    public void copyToLocalFile() throws Exception {
        Path src = new Path("/hdfsapi/test/a.txt");
        Path dst = new Path("D:\\Users\\xuyanan");
        fileSystem.copyToLocalFile(false, src, dst, true);
    }

    @Test
    public void listFiles() throws Exception{
        FileStatus[] statuses= fileSystem.listStatus(new Path("/hdfsapi/test"));

        for(FileStatus fs : statuses) {
            String isDir = fs.isDirectory() ? "文件夹":"文件";
            String permission = fs.getPermission().toString();
            short replication = fs.getReplication();
            long length = fs.getLen();
            String path = fs.getPath().toString();

            System.out.println(isDir + "\t" +  permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    @Test
    public void listFilesRecursive() throws Exception{
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);

        while(files.hasNext()) {
            LocatedFileStatus fs = files.next();
            String isDir = fs.isDirectory() ? "文件夹":"文件";
            String permission = fs.getPermission().toString();
            short replication = fs.getReplication();
            long length = fs.getLen();
            String path = fs.getPath().toString();

            System.out.println(isDir + "\t" +  permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    @Test
    public void create() throws IOException {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        out.writeUTF("Hello pk");
        out.flush();
        out.close();
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
    }
}
