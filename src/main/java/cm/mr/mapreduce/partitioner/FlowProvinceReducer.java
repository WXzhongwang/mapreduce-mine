package cm.mr.mapreduce.partitioner;

import cm.mr.mapreduce.flow.FlowAnalysisBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @description 流量统计reducer阶段（字典序排序）
 * @date created on 2020/8/31
 */
public class FlowProvinceReducer extends Reducer<Text, FlowAnalysisBean, Text, FlowAnalysisBean> {

    private FlowAnalysisBean v = new FlowAnalysisBean();

    @Override protected void reduce(Text key, Iterable<FlowAnalysisBean> values, Context context)
        throws IOException, InterruptedException {

        String phoneNumber = key.toString();

        Long upFlow = 0L;

        Long downFlow = 0L;

        for (FlowAnalysisBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        v.set(upFlow, downFlow);

        context.write(new Text(phoneNumber), v);
    }
}
