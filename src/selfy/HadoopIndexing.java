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
/**
 *
 * @author lukas
 */
public class HadoopIndexing
{

    public static final String HADOOP_INDEX_DIR = "hadoopIndex";
    public static final String HADOOP_DATA_DIR = "hadoopFull";
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

    public static class Map extends Mapper<Text, Text, Text, Text>
    {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            for (String a : line.split("\\W")) //[ \\t\\n\\x0B\\f\\r#.,;-'!?]"))
            {
                a = a.toLowerCase();
                if (a.length() <= 1 || a.equals("selfie") || googleStopwords.contains(a))
                {
                    continue;
                }
                //stopwords :D

                word.set(a);
//                TupleWritable out = new TupleWritable(new Writable[]{new Text(key),one});

                context.write(word, key);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, MapWritable>
    {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
//            System.out.println("working on " + key + ":");
            MapWritable sums = new MapWritable();
            for (Text val : values)
            {
//                System.out.print(val.toString() + ", ");
//                Text k = (Text) val.get(0);

//                IntWritable v = (IntWritable) val.get(1);
//                System.out.println(k.toString()+" "+v.toString());
                if (sums.containsKey(val))
                {
                    IntWritable soFar = (IntWritable) sums.get(val);
                    sums.put(val, new IntWritable(1 + soFar.get()));
                }
                else
                {
                    sums.put(new Text(val), new IntWritable(1));
                }
            }
//            System.out.println();
//            System.out.print("[");
//            for (java.util.Map.Entry<Writable, Writable> a : sums.entrySet())
//            {
//                String id = ((Text) a.getKey()).toString();
//                int score = ((IntWritable) a.getValue()).get();
//                System.out.print(id+" => "+score+", ");
//            }
//            System.out.println("]");
//            System.out.println("done with " + key);
            context.write(key, sums);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "invertedIndex");

        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(JSONInput.class);
        job.setOutputFormatClass(JSONOutput.class);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(HADOOP_INDEX_DIR)))
        {
            hdfs.delete(new Path(HADOOP_INDEX_DIR), true);
        }

//        MultipleOutputs.addNamedOutput(job, "index", TextOutputFormat.class, Text.class, MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(HADOOP_DATA_DIR));
        FileOutputFormat.setOutputPath(job, new Path(HADOOP_INDEX_DIR));

        job.waitForCompletion(true);
    }

}
