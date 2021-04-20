package cm.mr.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @description
 * @date created on 2020/8/26
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 只需要关注业务代码
     *
     * Reducer阶段的 KEYIN 和 VALIN 一定为map阶段对应的输出
     *
     * reduce阶段具体业务方法
     * 接收所有来自mapper阶段的数据输出，按照key的字典序进行排序
     * <hello, 1><spark, 1><hadoop, 1><world, 1>
     * 排序后：
     * <hadoop, 1><hello, 1><spark, 1><world, 1>
     *
     *     按key是否为一组去调用reduce,
     *     本方法的key就是这一组相同key的键值对的共同key，
     *     本方法的value就是这一组相同key的键值对的value，作为一个迭代器传入
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        //定义一个计数器
        int count = 0;

        //遍历一组迭代器，进行累计
        for (IntWritable value : values) {
            count += value.get();
        }

        //把最终的结果输出
        context.write(key, new IntWritable(count));
    }
}
