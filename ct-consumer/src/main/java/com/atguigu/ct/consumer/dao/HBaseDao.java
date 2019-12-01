package com.atguigu.ct.consumer.dao;

import com.atguigu.ct.common.bean.BaseDao;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.common.constant.ValueConstant;
import com.atguigu.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase的数据访问对象
 */
public class HBaseDao extends BaseDao {
    /**
     * 初始化
     */
    public void init() throws Exception {
        start();

        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(),"com.atguigu.ct.comsumer.coprocessor.InsertCalleeCoprocessor", ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());

        end();
    }

    /**
     * 插入对象
     * @param log
     * @throws Exception
     */
    public void insertData(Calllog log) throws Exception{
        log.setRowkey(genRegionNum(log.getCall1(),log.getCalltime())+"_"+log.getCall1()+"_"+log.getCalltime()+"_"+log.getCall2()+"_"+log.getDuration());
        putData(log);
    }


    /**
     * 插入数据
     * @param value
     * @throws IOException
     */
    public void insertData(String value) throws IOException {

        // 将通话日志保存到HBase表中

        // 获取通话日志数据
        String[] values = value.split("\t");
        String call1=values[0];
        String call2=values[1];
        String calltime=values[2];
        String duration=values[3];

        // 创建数据对象
        // rowkey设计
        // 长度原则，最大值64KB，推荐长度为10~100byte，最好是8的倍数，能短则短，rowkey如果太长，影响性能
        // 唯一性原则，rowkey应该具备唯一性
        // 散列原则，盐值散列：不能使用时间戳做rowkey，在rowkey前增加随机数
        //         字符串的反转
        //         计算分区号：hashmap

        // rowkey = regionNum + call1 + time + call2 + duration

        String rowkey=genRegionNum(call1,calltime)+"_"+call1+"_"+calltime+"_"+call2+"_"+duration+"_1";

        // 主叫用户
        Put put=new Put(Bytes.toBytes(rowkey));

        byte[] family=Bytes.toBytes(Names.CF_CALLER.getValue());

        put.addColumn(family,Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(family,Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(family,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(family,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(family,Bytes.toBytes("flg"),Bytes.toBytes("1"));

        String calleeRowkey=genRegionNum(call2,calltime)+"_"+call2+"_"+calltime+"_"+call1+"_"+duration+"_0";

        // 被叫用户
//        Put calleePut=new Put(Bytes.toBytes(calleeRowkey));
//        byte[] calleeFamily=Bytes.toBytes(Names.CF_CALLEE.getValue());
//
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("flg"),Bytes.toBytes("0"));

        // 保存数据
        List<Put> puts =new ArrayList<Put>();
        puts.add(put);
//        puts.add(calleePut);

        putData(Names.TABLE.getValue(),puts);

    }
}
