package cm.mr.mapreduce.sort;

import cm.mr.mapreduce.flow.FlowAnalysisBean;
import cm.mr.mapreduce.flow.FlowAnalysisMapper;
import cm.mr.mapreduce.flow.FlowAnalysisReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @description
 * @date created on 2020/8/25
 */
public class FlowSumSortRunner {

    /**
     * MR程序入口
     * 业务逻辑相关信息描述成一个job mapper reducer
     * @param args
     */
    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);

            // 本地模式MR运行
//            configuration.set("mapreduce.framework.name", "local");

//            configuration.set("yarn.resourcemanager.hostname","node-2");

            // 指定Main Class
            job.setJarByClass(FlowSumSortRunner.class);

            // set Mapper class, set Reducer class
            job.setMapperClass(FlowSumSortMapper.class);
            job.setReducerClass(FlowAnalysisReducer.class);

            // set mapper阶段的 输出 k,v 类型
            job.setMapOutputKeyClass(FlowAnalysisBean.class);
            job.setMapOutputValueClass(Text.class);

            //指定 本次mr最终输出的 k,v类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowAnalysisBean.class);

            job.setNumReduceTasks(2);

            //指定本次输入的数据路径
            //FileInputFormat.setInputPaths(job, "/wordcount/input");
            FileInputFormat.setInputPaths(job, "D://wordcount//FlowAnalysis//input");

            //FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));
            FileOutputFormat.setOutputPath(job, new Path("D://wordcount//FlowAnalysis//output"));

            //将程序信息日志方式信息可以打印出来
            // job.submit()
            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
