package selfy;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HadoopIndexing
{

    public static final Set<String> googleStopwords;

    static
    {
        googleStopwords = new HashSet<>();
        googleStopwords.add("I");
        googleStopwords.add("a");
        googleStopwords.add("about");
        googleStopwords.add("an");
        googleStopwords.add("are");
        googleStopwords.add("as");
        googleStopwords.add("at");
        googleStopwords.add("be");
        googleStopwords.add("by");
        googleStopwords.add("com");
        googleStopwords.add("de");
        googleStopwords.add("en");
        googleStopwords.add("for");
        googleStopwords.add("from");
        googleStopwords.add("how");
        googleStopwords.add("in");
        googleStopwords.add("is");
        googleStopwords.add("it");
        googleStopwords.add("la");
        googleStopwords.add("of");
        googleStopwords.add("on");
        googleStopwords.add("or");
        googleStopwords.add("that");
        googleStopwords.add("the");
        googleStopwords.add("this");
        googleStopwords.add("to");
        googleStopwords.add("was");
        googleStopwords.add("what");
        googleStopwords.add("when");
        googleStopwords.add("where");
        googleStopwords.add("who");
        googleStopwords.add("will");
        googleStopwords.add("with");
        googleStopwords.add("and");
        googleStopwords.add("the");
    }

    public static class Map extends Mapper<Text, Text, Text, IntWritable>
    {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            for (String a : line.split("\\W")) //[ \\t\\n\\x0B\\f\\r#.,;-'!?]"))
            {
                a = a.toLowerCase();
                if (a.equals("selfie") || googleStopwords.contains(a))
                {
                    continue;
                }
                //stopwords :D

                word.set(a);
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(JSONInput.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path("testout")))
        {
            hdfs.delete(new Path("testout"), true);
        }

        FileInputFormat.addInputPath(job, new Path("hadoop"));
        FileOutputFormat.setOutputPath(job, new Path("testout"));

        job.waitForCompletion(true);
    }

}
