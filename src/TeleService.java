

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Counters;


public class TeleService {

    //Counters
    enum Teleservice {
        INVALID_INPUTS,
        NUMBER_OF_SMS,
        NUMBER_OF_VOICE_CALLS,
        NUMBER_OF_TRANSACTIONS ,
    }


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text,Text> {
        private Text userKey= new Text();
        private Text userValue=new Text();

        //Map function
        public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] inputArray= line.split(" ",-1);
            userKey.set(inputArray[0]);
            userValue.set(inputArray[2]);
            output.collect(userKey,userValue);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text,Text, Text, LongWritable> {

        //Reduce Function
        public void reduce(Text key,Iterator<Text>services, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            int cost=0;
            while (services.hasNext()) {
                String userService = services.next().toString();
                if (userService.equals("SMS") ) {
                    cost+=2;
                    incCounter(reporter,key,Teleservice.NUMBER_OF_SMS,"perUserSMS");
                } else if(userService.equals("VOICE")){
                    cost+=100;
                    incCounter(reporter,key,Teleservice.NUMBER_OF_VOICE_CALLS,"perUserVOICE");
                } else
                    incCounter(reporter,key,Teleservice.INVALID_INPUTS,userService);
                incCounter(reporter,key,Teleservice.NUMBER_OF_TRANSACTIONS,"perUserTransactions");
            }
            if (cost > 0) {
                output.collect(key, new LongWritable(cost));
            }
        }

        //Increments counter values
        void incCounter (Reporter reporter,Text key,Teleservice service,String str){
            reporter.getCounter(service).increment(1);    //increment Counter
            reporter.getCounter(str,key.toString()).increment(1);  //dynamic Counter

        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(TeleService.class);
        Job job=new Job(conf);
        job.setJarByClass(TeleService.class);
        conf.setJobName("teleservice");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("input"));
        FileOutputFormat.setOutputPath(conf, new Path("output"));

        JobClient.runJob(conf);


    }
}
    

