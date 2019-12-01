package com.atguigu.ct.analysis.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分析数据的key
 */
public class AnalysisKey implements WritableComparable<AnalysisKey> {

    public AnalysisKey(){}

    public AnalysisKey(String tel,String date){
        this.tel=tel;
        this.date=date;
    }

    private String tel;
    private String date;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    /**
     * 比较tel，date
     * @param key
     * @return
     */
    @Override
    public int compareTo(AnalysisKey key) {
        int result = tel.compareTo(key.getTel());

        if(result==0){
            result=date.compareTo(key.getDate());
        }

        return result;
    }

    /**
     * 写数据
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tel);
        dataOutput.writeUTF(date);
    }

    /**
     * 读数据
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tel=dataInput.readUTF();
        date=dataInput.readUTF();
    }
}
