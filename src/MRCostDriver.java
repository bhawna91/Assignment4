/**
 * Created with IntelliJ IDEA.
 * User: Bhawna
 * Date: 11/12/12
 * Time: 10:44 PM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class MRCostDriver {

    public static class CostMapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {

            Text userId = new Text(Bytes.toString(values.getValue(Bytes.toBytes("UserId"), Bytes.toBytes("UserId"))));
            Text userService = new Text(Bytes.toString(values.getValue(Bytes.toBytes("Transaction"), Bytes.toBytes("Service"))));

            try {
                context.write(userId, userService);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }

        }
    }


    public static class CostReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cost = 0;
            for (Text val : values) {
                if (val.toString().equals("SMS")) cost += 2;
                else cost += 100;
            }

            byte[] rowkey = key.toString().getBytes();
            Put put = new Put(rowkey);
            put.add(Bytes.toBytes("UserId"), Bytes.toBytes("UserId"), Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("Transaction"), Bytes.toBytes("Cost"), Bytes.toBytes(Integer.toString(cost)));


            context.write(new ImmutableBytesWritable(rowkey), put);
        }
    }

    public static void main(String[] args) throws Exception {
        MRFileToTable obj = new MRFileToTable();
        obj.execute();
        Configuration myConf = HBaseConfiguration.create();

        Job job = new Job(myConf, "MainJob");
        job.setJarByClass(MRCostDriver.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(CostMapper.class);
        job.setReducerClass(CostReducer.class);
        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob("UserTable", scan, CostMapper.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob("SummaryTable", CostReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

