/**
 * Created by userhp on 14/03/2016.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

public class MatrixMultiplication {

        public static class TokenizerMapper
                extends Mapper<Object, Text, Text, IntWritable>{

            private Text word = new Text();
            private static String vector;


            public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
                String[] itr = value.toString().split("\n");
                String[] vectorSplit = vector.toString().split("\n");
                for (String val : itr) {
                    String [] splitLine = val.split(",");
                    word.set(splitLine[0]);
                    int value1 = Integer.parseInt(splitLine[2]) * Integer.getInteger(vectorSplit[Integer.getInteger(splitLine[1])].split(",")[2]);
                    IntWritable v = new IntWritable(value1);
                    context.write(word,v);
                }

            }

            public static void setVector(String vectorString) {
                vector = vectorString;
            }
        }


        public static class IntSumReducer
                extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                               Context context
            ) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
        }


        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            TokenizerMapper.setVector(new File(args[3]).toString());
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(MatrixMultiplication.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

