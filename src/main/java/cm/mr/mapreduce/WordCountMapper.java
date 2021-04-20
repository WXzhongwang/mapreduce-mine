package cm.mr.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @description
 * @date created on 2020/8/26
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 单词数据出现次数
     * 读取一行传入一个(K,V） 这里着重处理Value, key为偏移量，对我们没用，
     * value为行数据
     */

    /**
     * 该方法就是mapper阶段具体的业务逻辑实现方法，方法调用取决于读取数据的组件，有没有给MR传入数据
     * 如果有的话，每传入一个(K,V）对，就会被调用一次
     *
     *
     * hello hadoop world
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String line = value.toString();

        //内容切割为单词
        String[] words = line.split(" ");

        for (String word : words) {
            //使用MR上下文context
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
