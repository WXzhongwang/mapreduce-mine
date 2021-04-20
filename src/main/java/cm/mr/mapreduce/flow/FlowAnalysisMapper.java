package cm.mr.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @description  每个用户的流量统计（上行流量，下行流量，总流量）
 * @date created on 2020/8/31
 */
public class FlowAnalysisMapper extends Mapper<LongWritable, Text, Text, FlowAnalysisBean> {

    private FlowAnalysisBean v = new FlowAnalysisBean();

    private Text k = new Text();

    @Override protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split("\t");

        String phoneNumber = fields[1];

        Long upFlow = Long.parseLong(fields[fields.length - 3]);

        Long downFlow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNumber);

        v.set(upFlow, downFlow);

        context.write(new Text(phoneNumber), v);
    }
}
