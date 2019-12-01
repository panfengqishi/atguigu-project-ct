package com.atguigu.ct.common.bean;

import com.atguigu.ct.common.api.Column;
import com.atguigu.ct.common.api.RowKey;
import com.atguigu.ct.common.api.TableRef;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.common.constant.ValueConstant;
import com.atguigu.ct.common.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

/**
 * 基础的数据访问对象
 */

public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder=new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder=new ThreadLocal<Admin>();

    protected void start() throws Exception {
        getConnection();
        getAdmin();
    }

    protected void end() throws Exception {
        Admin admin=getAdmin();
        if(admin!=null){
            admin.close();
            adminHolder.remove();
        }

        Connection connection=getConnection();
        if(connection!=null){
            connection.close();
            connHolder.remove();
        }
    }

    /**
     * 创建表，如果表已经存在，那么删除后创建新表
     * @param name
     * @param families
     * @throws Exception
     */

    protected void createTableXX(String name,String... families) throws Exception {
        createTableXX(name,null,null,families);
    }

    protected void createTableXX(String name,String coprocessorClass,Integer regionCount,String... families) throws Exception {
        Admin admin=getAdmin();

        TableName tableName=TableName.valueOf(name);
        if(admin.tableExists(tableName)){
            // 表存在，删除表
            deleteTable(name);
        }
        // 创建表
        createTable(name,coprocessorClass,regionCount,families);
    }

    private void createTable(String name,String coprocessorClass,Integer regionCount,String... families) throws Exception{
        Admin admin=getAdmin();
        TableName tableName=TableName.valueOf(name);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        if(families==null||families.length==0){
            families= new String[1];
            families[0]= Names.CF_INFO.getValue();
        }
        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }

        if(coprocessorClass!=null&&!"".equals(coprocessorClass)){
            tableDescriptor.addCoprocessor(coprocessorClass);
        }

        // 增加预分区

        // 分区键
        if(regionCount==null||regionCount<=1){
            admin.createTable(tableDescriptor);
        }else{
            byte[][] splitKeys=genSplitKeys(regionCount);
            admin.createTable(tableDescriptor,splitKeys);
        }


    }


    /**
     * 获取查询时的startrow,stoprow集合
     * @return
     */
    protected  List<String[]> getStartStopRowKeys(String tel,String start,String end){
        List<String[]> rowKeyss=new ArrayList<String[]>();

        String stringTime=start.substring(0,6);
        String endTime=end.substring(0,6);

        Calendar startCal=Calendar.getInstance();
        startCal.setTime(DateUtil.parse(stringTime,"yyyyMM"));

        Calendar endCal=Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime,"yyyyMM"));

        while(startCal.getTimeInMillis()<=endCal.getTimeInMillis()){

            // 当前时间
            String nowTime=DateUtil.format(startCal.getTime(),"yyyyMM");

            int regionNum=genRegionNum(tel,nowTime);

            String startRow=regionNum+"_"+tel+"_"+nowTime;
            String stopRow=startRow+"|";

            String[] rowKeys={startRow,stopRow};
            rowKeyss.add(rowKeys);

            // 月份加一
            startCal.add(Calendar.MONTH,1);
        }
        return rowKeyss;
    }

    /**
     * 计算分区号
     * @param tel
     * @param date
     * @return
     */
    protected int genRegionNum(String tel,String date){
        // 15935158888
        String usercode=tel.substring(tel.length()-4);
        // 20181010120000
        String yearMonth=date.substring(0,6);

        int userCodeHash=usercode.hashCode();
        int yearMonthHash=yearMonth.hashCode();

        // crc校验采用异或算法
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        // 取模
        int regionNum=crc% ValueConstant.REGION_COUNT;

        return regionNum;

    }


    /**
     * 生成分区键
     * @return
     */
    private byte[][] genSplitKeys(int regionCount){

        int splitKeyCount=regionCount-1;

        byte[][] bs=new byte[splitKeyCount][];

        List<byte[]> bsList=new ArrayList<byte[]>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitkey = i+"|";
            bsList.add(Bytes.toBytes(splitkey));
        }

        Collections.sort(bsList,new Bytes.ByteArrayComparator());

        bsList.toArray(bs);

        return bs;
    }

    /**
     * 增加对象，自动封装数据，将对象数据直接封装到HBase中
     * @param obj
     * @throws Exception
     */

    protected void putData(Object obj) throws Exception{

        // 反射
        Class clazz = obj.getClass();
        TableRef tableRef= (TableRef) clazz.getAnnotation(TableRef.class);
        String tableName=tableRef.value();

        Field[] fs = clazz.getDeclaredFields();
        String stringRowkey="";
        for (Field f : fs) {
            RowKey rowkey = f.getAnnotation(RowKey.class);
            if(rowkey!=null){
                f.setAccessible(true);
                stringRowkey=(String) f.get(obj);
                break;
            }
        }
        // 获取表对象
        Connection conn=getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put=new Put(Bytes.toBytes(stringRowkey));

        for (Field f : fs) {
            Column column = f.getAnnotation(Column.class);
            if(column!=null){
                String family=column.family();
                String colName=column.column();
                if(colName==null||"".equals(colName)){
                    colName=f.getName();
                }
                f.setAccessible(true);
                String value=(String)f.get(obj);

                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(colName),Bytes.toBytes(value));
            }
        }

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }

    /**
     * 增加多数据
     * @param name
     * @param puts
     * @throws IOException
     */
    protected void putData(String name, List<Put> puts) throws IOException {
        // 获取表对象
        Connection conn=getConnection();
        Table table = conn.getTable(TableName.valueOf(name));

        // 增加数据
        table.put(puts);

        // 关闭表
        table.close();
    }

    /**
     * 增加数据
     * @param name
     * @param put
     * @throws IOException
     */
    protected void putData(String name, Put put) throws IOException {
        // 获取表对象
        Connection conn=getConnection();
        Table table = conn.getTable(TableName.valueOf(name));

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }

    /**
     * 删除表格
     * @param name
     * @throws Exception
     */
    protected void deleteTable(String name) throws Exception{
        TableName tableName=TableName.valueOf(name);
        Admin admin=getAdmin();

        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 创建命名空间，如果命名空间存在，不需要创建，否则创建新的命名空间
     * @param namesapce
     */
    protected void createNamespaceNX(String namesapce) throws IOException {
        Admin admin=getAdmin();

        try{
            admin.getNamespaceDescriptor(namesapce);
        }catch (NamespaceNotFoundException e){
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namesapce).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 获取连接对象
     */
    protected synchronized Admin getAdmin() throws IOException {
        Admin admin=adminHolder.get();
        if(admin==null){
            admin=getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 获取连接对象
     */
    protected synchronized Connection getConnection() throws IOException {
        Connection conn=connHolder.get();
        if(conn==null){
            Configuration conf= HBaseConfiguration.create();
            conn= ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }


}
