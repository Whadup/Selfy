/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package selfy;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author lukas
 */
public class JSONOutput extends FileOutputFormat<Text, MapWritable>
{

    @Override
    public RecordWriter<Text, MapWritable> getRecordWriter(final TaskAttemptContext tac) throws IOException, InterruptedException
    {
        Path p = FileOutputFormat.getOutputPath(tac);
        FileSystem fileSystem = p.getFileSystem(tac.getConfiguration());

        final int n = 1024;
        final FSDataOutputStream[] streams = new FSDataOutputStream[n];
        final boolean[] first = new boolean[n];
        
        for (int i = 0; i < n; i++)
        {
            first[i]=true;
            Path output = new Path(p, i+".json");
            streams[i] = fileSystem.create(output);
            streams[i].write("{\n".getBytes());
        }

        return new RecordWriter<Text, MapWritable>()
        {

            @Override
            public void write(Text k, MapWritable v) throws IOException, InterruptedException
            {
                long d = k.toString().hashCode();
                
                d = d - Integer.MIN_VALUE;
                int indexNumber = (int)(d % n);
                FSDataOutputStream stream = streams[indexNumber];
                if(!first[indexNumber])
                    stream.write(",\n".getBytes());
                else
                    first[indexNumber]=false;
                stream.write(("\""+k + "\" : {\n").getBytes());
                stream.write("\t\"documentCount\" : ".getBytes());
                stream.write(("" + v.size() + ",\n").getBytes());
                stream.write("\t\"documents\": [\n".getBytes());
                int i=0;
                for (Map.Entry<Writable, Writable> a : v.entrySet())
                {
                    String id = ((Text) a.getKey()).toString();
                    int score = ((IntWritable) a.getValue()).get();
                    stream.write("\t\t{\n".getBytes());
                    stream.write(("\t\t\t\"document\" : \"" + id + "\",\n").getBytes());
                    stream.write(("\t\t\t\"score\" : " + (float) score + "\n").getBytes());
                    stream.write("\t\t}".getBytes());
                    if(i+1<v.size())
                        stream.write(",\n".getBytes());
                    else
                        stream.write("\n".getBytes());
                    i++;
                }
                stream.write("\t]\n".getBytes());
                stream.write("}".getBytes());

            }

            @Override
            public void close(TaskAttemptContext tac) throws IOException, InterruptedException
            {
                for (int i = 0; i < n; i++)
                {
                    streams[i].write("}\n".getBytes());
                    streams[i].close();
                }
            }
        };
    }

}
