/**
 * Created by 2016st19 on 11/8/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class HBaseWuxia {
    public static class InvertedIndexMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String[] split = String.valueOf(fileSplit.getPath().getName()).split("\\.");
            String filename;
            if(split.length == 4) filename = split[0] + split[1];
            else filename = split[0];
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String temp = itr.nextToken();
                word.set(temp + "#" + filename);
                context.write(word, one);
            }
        }
    }

    public static class SumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class NewPartitioner
            extends HashPartitioner<Text, IntWritable>{
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String term;
            term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer
            extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp;
        static Text CurrentItem = new Text("*");
        static List<String> positionList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            String[] split = key.toString().split("#");
            word1.set(split[0]);
            temp = split[1];
            for(IntWritable val : values){
                sum += val.get();
            }
            word2.set(temp + ":" + sum + ";");
            if((!CurrentItem.equals(word1)) && (!CurrentItem.equals("*"))){
                myOutPut(context);
                positionList = new ArrayList<String>();
            }
            CurrentItem = new Text(word1);
            positionList.add(word2.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            myOutPut(context);
        }

        private static void myOutPut(Context reducerContext) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            long count = 0;
            int fileNum = positionList.size();
            for(String p : positionList){
                out.append(p);
                count += Long.parseLong(p.substring(p.indexOf(":") + 1, p.indexOf(";")));
            }
            if(count > 0) {
                double avg = (double) count / fileNum;
                String format = String.format("%.2f", avg);
                Put put = new Put(Bytes.toBytes(CurrentItem.toString()));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("avg"), Bytes.toBytes(format));
                reducerContext.write(new ImmutableBytesWritable(CurrentItem.getBytes()), put);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "HBaseWuxia");
        job.setJarByClass(HBaseWuxia.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        TableMapReduceUtil.initTableReducerJob("Wuxia", InvertedIndexReducer.class, job);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}