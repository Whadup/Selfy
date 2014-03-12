/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package selfy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 *
 * @author lukas
 */
public class JSONInput extends CombineFileInputFormat<Text, Text>
{

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException
    {
        return new JSONRecordReader();
    }

    private class JSONRecordReader extends RecordReader<Text, Text>
    {

        private int fileNumber;
        private int objectNumber;
        private JsonArray array;

        private CombineFileSplit split;
        private TaskAttemptContext context;
        private JsonReader reader = null;
        private FSDataInputStream fileIn = null;

        private void loadJSON() throws IOException
        {
            
            if (reader != null)
            {
                reader.close();
            }
            if (fileIn != null)
            {
                fileIn.close();
            }

            JsonObject json;
            //Open JSON
            try
            {
                Path path = split.getPath(fileNumber);
                System.out.println("LOADING NEXT JSON FILE "+path.getName());
                FileSystem fs = path.getFileSystem(context.getConfiguration());
                fileIn = fs.open(split.getPath(fileNumber));
                reader = Json.createReader(fileIn); //new FileReader(new File(split.g)));
                json = reader.readObject();
                array = json.getJsonArray("data");
                objectNumber = -1;
            } catch (javax.json.stream.JsonParsingException e)
            {
//                System.out.print("failed");
            }
        }

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException
        {
            
            this.split = (CombineFileSplit) is;
            System.out.println("THIS LOADER IS SUPPOSED TO LOAD "+split.getNumPaths()+" FILES");
            System.out.println(Arrays.toString(split.getPaths()));
            this.context = tac;
            this.fileNumber = 0;
            this.loadJSON();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException
        {
            
            if (objectNumber < array.size() - 1)
            {
                objectNumber++;
                return true;
            }
            else
            {
                if (fileNumber + 1 < split.getNumPaths())
                {
                    fileNumber++;
                    loadJSON();
                    if (objectNumber < array.size() - 1)
                    {
                        objectNumber++;
                        return true;
                    }
                    else
                    {
                        return nextKeyValue();
                    }
                }
                else
                {
                    return false;
                }
            }

        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException
        {
//            if (objectNumber < array.size())
            {
                JsonObject o = array.getJsonObject(objectNumber);
                
                return new Text(o.getString("id"));
            }

        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException
        {
//            if (objectNumber < array.size())
            {
                JsonObject json = array.getJsonObject(objectNumber);

                //I want to extract these Strings from the json
                String captionText = "";
                ArrayList<String> commentsText = new ArrayList<>();

                //caption of the image, save it in 'captionText'
                if (!json.isNull("caption"))
                {
                    JsonObject caption = json.getJsonObject("caption");
                    captionText = caption.getString("text");
                }
                //comments, iterate over all comments and save them in the list 'commentsText'
                if (!json.isNull("comments"))
                {
                    JsonObject comments = json.getJsonObject("comments");
                    if (!comments.isNull("data"))
                    {
                        JsonArray commentsArray = comments.getJsonArray("data");
                        for (int i = 0; i < commentsArray.size(); i++)
                        {
                            JsonObject comment = commentsArray.getJsonObject(i);
                            commentsText.add(comment.getString("text"));
                        }
                    }
                }
                String body = captionText + " ";
                for (String comment : commentsText)
                {
                    body += comment + " ";
                }
                return new Text(body);
            }
        }

        @Override
        public float getProgress() throws IOException, InterruptedException
        {
            return 1.0f * fileNumber / split.getNumPaths();
        }

        @Override
        public void close() throws IOException
        {
            if (reader != null)
            {
                reader.close();
            }
            if (fileIn != null)
            {
                fileIn.close();
            }
        }

    }

}
