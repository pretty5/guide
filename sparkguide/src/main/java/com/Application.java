package com;

import com.common.Constants;
import jodd.datetime.JDateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/*
*@ClassName:Application
 @Description:TODO
 @Author:
 @Date:2018/11/26 10:41 
 @Version:v1.0
*/
public class Application {
    private static Logger logger = LoggerFactory.getLogger("Application");

    public static void main(String[] args) {
        try {
            initApp();
        } catch (URISyntaxException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }


        //String tmpDir= Constants.HOME+"\\"+UUID.randomUUID().toString();
        //String tmpDir= "C:\\Users\\root\\Desktop\\desktop\\zy课件\\大数据第二个项目\\智能导诊数据工厂\\";
        //step 1: 利用sqoop/sparksql增量导出 hdfs临时目录
        String tmpDir = null;
        try {
            tmpDir = step1();
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }
        //step 2: 利用sparksql计算
        try {
             step2(tmpDir);
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }
        //step 3:临时目录中的数据归档存储
        try {
            step3(tmpDir);
        } catch (URISyntaxException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }
        //step 4 :删除临时目录
        try {
            step4(tmpDir);
        } catch (URISyntaxException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }


    }

    //初始化应用
    private static void  initApp() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(Constants.HOME), configuration);
        if (!fileSystem.exists(new Path(Constants.HOME))) {
            fileSystem.mkdirs(new Path(Constants.HOME));
        }
    }
    //通过sparksql将数据导入hdfs
    private static String step1() throws IOException, InterruptedException {
       //线上环境
        SparkAppHandle sparkAppHandle = new SparkLauncher()
                .setAppResource("E:\\IDEA\\guide\\sparkguide\\target\\sparkguide-1.0-SNAPSHOT.jar")
                .setMainClass("LoadDataTask")
                .setMaster("yarn")
                .setDeployMode("client")
                .setAppName("s")
                .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
                .setSparkHome("E:\\spark-2.3.0-bin-hadoop2.7")
                .setVerbose(true)
                .startApplication();

        while (!sparkAppHandle.getState().isFinal()) {
            Thread.sleep(2000);
        }
        return "";
        /*
        本地环境
        return LoadDataTask.run();
         */
    }

    private static void step4(String tmpDir) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(tmpDir), configuration);
        fileSystem.delete(new Path(tmpDir), true);
        fileSystem.close();
    }

    private static void step3(String tmpDir) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(tmpDir), configuration);
        FSDataInputStream inputStream = fileSystem.open(new Path(tmpDir));
        JDateTime jdt = new JDateTime();            // 当前的日期以及时间
        //jdt.getFormat();//获取格式化字符串，默认是 YYYY-MM-DD hh:mm:ss.mss
        jdt.setFormat("YYYY-MM-DD");//设置格式化字符串

        String dir = Constants.HOME + "/history/" + jdt + "/";
        FSDataOutputStream outputStream = fileSystem.create(new Path(dir));
        IOUtils.copyBytes(inputStream, outputStream, 1024);
        fileSystem.close();
    }

    private static void step2(String tmpDir) throws IOException, InterruptedException {
        /*
        线上环境
         */
        //线上环境
        SparkAppHandle sparkAppHandle = new SparkLauncher()
                .setAppResource("E:\\IDEA\\guide\\sparkguide\\target\\sparkguide-1.0-SNAPSHOT.jar")
                .setMainClass("MergeRecordAndReimburseByHospital")
                .setMaster("yarn")
                .setDeployMode("client")
                .setAppName("cal")
                .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
                .setSparkHome("E:\\spark-2.3.0-bin-hadoop2.7")
                .setVerbose(true)
                .startApplication();

        while (!sparkAppHandle.getState().isFinal()) {
            Thread.sleep(2000);
        }


        /*本地环境
        MergeRecordAndReimburseByHospital.main(new String[]{tmpDir});
         */

    }
}
