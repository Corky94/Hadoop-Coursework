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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
                    System.out.println(val);
                    String [] splitLine = val.split(",");
                    word.set(splitLine[0]);
                    int i = Integer.parseInt(splitLine[2]);
                    String s = vectorSplit[Integer.parseInt(splitLine[1])];
                    String nm = s.split(",")[2];
                    int value1 = i * Integer.parseInt(nm);
                    IntWritable v = new IntWritable(value1);
                    context.write(word,v);
                }

            }

            public static void setVector(String vectorString) {
                vector = vectorString;
                System.out.println("\n\n\n\n" + vectorString + "\n\n\n\n");
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
            StringBuilder builder = new StringBuilder();

            BufferedReader fileReader = new BufferedReader(new FileReader(new File(args[2])));
            String line = fileReader.readLine();
            while(line!= null){
                builder.append(line);
                builder.append("\n");
                line = fileReader.readLine();
            }
            TokenizerMapper.setVector(builder.toString());
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

