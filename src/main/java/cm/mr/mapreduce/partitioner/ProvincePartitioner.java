package cm.mr.mapreduce.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @description
 * @date created on 2020/9/10
 */
public class ProvincePartitioner<Text, FlowAnalysisBean> extends Partitioner<Text, FlowAnalysisBean> {

    public static HashMap<String, Integer> provinceMap = new HashMap<>();

    static {
        provinceMap.put("134", 0);
        provinceMap.put("135", 0);
        provinceMap.put("136", 0);
        provinceMap.put("138", 0);
        provinceMap.put("185", 1);
        provinceMap.put("186", 1);
        provinceMap.put("187", 1);
        provinceMap.put("188", 1);
        provinceMap.put("130", 2);
        provinceMap.put("131", 2);
        provinceMap.put("132", 2);
    }

    /**
     * 实际分区方法， 返回分区编号    决定数据会落到哪个分区 part-r-00000?
     * @param text
     * @param flowAnalysisBean
     * @param numPartitions
     * @return
     */
    @Override public int getPartition(Text text, FlowAnalysisBean flowAnalysisBean, int numPartitions) {

        Integer code  = provinceMap.get(text.toString().substring(0, 3));

        if (code != null){
            return code;
        }

        return 5;
    }
}
