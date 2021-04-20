package cm.mr.mapreduce.sort;

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
public class FlowSumSortReducer extends Reducer<FlowAnalysisBean, Text, Text, FlowAnalysisBean> {

    @Override protected void reduce(FlowAnalysisBean key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        context.write(values.iterator().next(), key);

    }
}
