/**
 * Created with IntelliJ IDEA.
 * User: Bhawna
 * Date: 10/28/12
 * Time: 1:11 PM
 */

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
        COST
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
                    incSMSCounter(reporter,key);
                } else if(userService.equals("VOICE")){
                    cost+=100;
                    incVoiceCounter(reporter,key);
                } else
                    incInvalidCounter(reporter,userService);
                incTransactionCounter(reporter,key);
            }
            if (cost > 0) {
                output.collect(key, new LongWritable(cost));
            }
        }

        //Increments number of SMS
        void incSMSCounter (Reporter reporter,Text key){
            reporter.getCounter(Teleservice.NUMBER_OF_SMS).increment(1);    //Counter for Total Number Of SMS in Transaction
            reporter.getCounter("perUserSMS",key.toString()).increment(1);  //dynamic Counter for Number Of SMS Per User

        }
        //Increments number of Voice Calls
        void incVoiceCounter (Reporter reporter,Text key){
            reporter.getCounter(Teleservice.NUMBER_OF_VOICE_CALLS).increment(1); //Counter For Total Number Of Voice Calls in Transaction
            reporter.getCounter("perUserVOICE",key.toString()).increment(1);    //dynamic Counter for NUmber Of Voice Calls Per User


        }
        //Increments number of Invalid Inputs
        void incInvalidCounter(Reporter reporter,String userService){
            System.err.println("Ignoring invalid input: " + userService);
            reporter.getCounter(Teleservice.INVALID_INPUTS).increment(1);     //Counter for Invalid Inputs(Other than VOICE or SMS)

        }
        //Increments number of Transactions
        void incTransactionCounter(Reporter reporter,Text key){
            reporter.getCounter(Teleservice.NUMBER_OF_TRANSACTIONS).increment(1);   //Counter For Total Number Of Transactions
            reporter.getCounter("perUserTransactions",key.toString()).increment(1); //Dynamic Counter for Number Of Transactions Per User

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



