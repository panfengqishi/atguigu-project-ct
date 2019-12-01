package com.atguigu.ct.analysis.io;

import com.atguigu.ct.analysis.kv.AnalysisKey;
import com.atguigu.ct.analysis.kv.AnalysisValue;
import com.atguigu.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MySQL数据格式化的输出对象
 */
public class MySQLBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {

    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey,AnalysisValue>{

        private Connection connection=null;
        private Jedis jedis=null;

        public MySQLRecordWriter(){
            // 获取资源
            connection= JDBCUtil.getConnection();
            jedis = new Jedis("hadoop105",6379);
        }

        /**
         * 输出数据
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {


            PreparedStatement pstat=null;

            try {
                String insertSQL="insert into ct_call(telid,dateid,sumcall,sumduration) values(?,?,?,?)";
                pstat=connection.prepareStatement(insertSQL);

                pstat.setInt(1,Integer.parseInt(jedis.hget("ct_user",key.getTel())));
                pstat.setInt(2,Integer.parseInt(jedis.hget("ct_date",key.getDate())));
                pstat.setInt(3,Integer.parseInt(value.getSumCall()));
                pstat.setInt(4,Integer.parseInt(value.getSumDuration()));
                pstat.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                if(pstat!=null){
                    try {
                        pstat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        /**
         * 释放资源
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if(jedis!=null){
                jedis.close();
            }
        }
    }

    @Override
    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null: new Path(name);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
}
