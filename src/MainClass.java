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




public class MainClass {

    static class Mapper1 extends TableMapper<Text,Text> {

        @Override
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


    public static class Reducer1 extends TableReducer<Text,Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int cost = 0;
            for (Text val : values) {
                if(val.toString().equals("SMS"))cost+=2;
                else cost+=100;
            }

            byte[] rowkey=key.toString().getBytes();
            Put put = new Put(rowkey);
            put.add(Bytes.toBytes("UserId"), Bytes.toBytes("UserId"), Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("Transaction"), Bytes.toBytes("Cost"), Bytes.toBytes(Integer.toString(cost)));



            context.write(new ImmutableBytesWritable(rowkey),put);
        }
    }

    public static void main(String[] args) throws Exception {
        MapReduceFileToTable obj= new MapReduceFileToTable();
        obj.execute();
        Configuration myConf = HBaseConfiguration.create();

        Job job = new Job(myConf,"MainJob");
        job.setJarByClass(MainClass.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob("UserTable", scan, Mapper1.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob("SummaryTable", Reducer1.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
