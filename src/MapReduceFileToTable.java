/**
 * Created with IntelliJ IDEA.
 * User: bhawna
 * Date: 11/11/12
 * Time: 5:58 PM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.Mapper;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
    import java.io.IOException;



    public class MapReduceFileToTable{

        static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

                 protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
                String LogStr = value.toString();
                if (LogStr.contains(" ")) {
                    String[] logArr = LogStr.split(" ");

                        System.out.println(logArr[0]);
                    byte[] rowkey=key.toString().getBytes();
                    Put put = new Put(rowkey);
                        put.add(Bytes.toBytes("UserId"), Bytes.toBytes("UserId"),
                                Bytes.toBytes(logArr[0]));
                        put.add(Bytes.toBytes("Transaction"), Bytes.toBytes("Date"),
                                Bytes.toBytes(logArr[1]));
                        put.add(Bytes.toBytes("Transaction"), Bytes.toBytes("Service"),
                                Bytes.toBytes(logArr[2]));

                    context.write(new ImmutableBytesWritable(rowkey),put);

                }

            }

        }

        public int execute() throws Exception {
            Configuration myConf = HBaseConfiguration.create();
            Job job = new Job(myConf,"TransferFileToTable");
            job.setJarByClass(MapReduceFileToTable.class); // class that
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, new Path("Out"));

            job.setMapperClass(Map.class);

            TableMapReduceUtil.initTableReducerJob(
                    "UserTable",        // output table
                     null,    // reducer class
                    job);
           job.setNumReduceTasks(5);   // at least one, adjust as required
           job.waitForCompletion(true);
            return 0;
        }

    }
