package cm.mr.mapreduce.flow;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @description 自定义传输对象  interface WritableComparable<T> extends Writable, Comparable<T>
 * @date created on 2020/8/31
 */
public class FlowAnalysisBean implements WritableComparable<FlowAnalysisBean> {

    private Long upFlow;

    private Long downFlow;

    private Long sum;

    public FlowAnalysisBean() {

    }

    public FlowAnalysisBean(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sum = this.upFlow + this.downFlow;
    }

    public FlowAnalysisBean(Long upFlow, Long downFlow, Long sum) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sum = sum;
    }


    public void set(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sum = this.upFlow + this.downFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"upFlow\":").append(upFlow);
        sb.append(",\"downFlow\":").append(downFlow);
        sb.append(",\"sum\":").append(sum);
        sb.append('}');
        return sb.toString();
    }

    /**
     * 自定义对象用于网络传输时候，所需的序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
        dataOutput.writeLong(this.sum);
    }

    /**
     * 自定义对象用于网络传输时，所需的反序列化方法
     * @param dataInput
     * @throws IOException
     */
    @Override public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sum = dataInput.readLong();
    }

    @Override
    public int compareTo(FlowAnalysisBean o) {
        //实现按照总流量的倒序排序
        return this.sum > o.getSum()? -1 : 1;
    }
}
