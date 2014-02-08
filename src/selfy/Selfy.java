/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package selfy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import javax.json.*;
import javax.json.stream.JsonParser;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.store.SimpleFSDirectory;

/**
 *
 * @author lukas
 */
public class Selfy
{
    public static final String CRAWLER_TAG = "selfie";
    
    private final StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
    private Directory index;
    private IndexWriter w;
    private SpatialStrategy spatialStrategy;
    private SpatialContext spacialContext;

    public Selfy()
    {
        try
        {
            //create an index
            index = new SimpleFSDirectory(new File("index"));
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);

            
            //Index GPS Locations
            spacialContext = SpatialContext.GEO;
            SpatialPrefixTree grid = new GeohashPrefixTree(spacialContext, 11);
            spatialStrategy = new RecursivePrefixTreeStrategy(grid, "location");

            //index writer
            w = new IndexWriter(index, config);

            
            //Iterate over all files
            int i = 0;
            for (File selfie : new File("/Users/lukas/Desktop/selfies").listFiles())
            {
                //index all json files
                if (selfie.isFile() && selfie.getName().endsWith(".json"))
                {
                    addSelfie(selfie);
                    i++;
                }
                if(i==10000)
                    break;
            }
            
            //save the index
            w.commit();
            w.close();
            index.close();

        } catch (IOException ex)
        {
            w = null;
            index = null;
            Logger.getLogger(Selfy.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void addSelfie(File f)
    {
        try
        {
            System.out.print(f.getName() + ": ");
            
            //Open JSON
            JsonReader reader = Json.createReader(new FileReader(f));
            JsonObject json = reader.readObject();

            
            //I want to extract these Strings from the json
            String captionText = "";
            ArrayList<String> commentsText = new ArrayList<>();
            String hashtagText = "";
            String idText = "";
            String userText = "";

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
            //read all tags and save them in the String 'hashtagText' separated by whitespaces
            if (!json.isNull("tags"))
            {
                JsonArray tags = json.getJsonArray("tags");
                for (int i = 0; i < tags.size(); i++)
                {
                    String s = tags.getString(i);
                    //don't add the hashtag used to crawl the data
                    if (!s.equals(CRAWLER_TAG))
                    {
                        hashtagText += s + " ";
                    }
                }
            }
            // extract the id
            if (!json.isNull("id"))
            {
                idText = json.getString("id");
            }
            // extract the username and save it in String 'userText'
            if (!json.isNull("user"))
            {
                JsonObject user = json.getJsonObject("user");
                if (!user.isNull("username"))
                {
                    userText = user.getString("username");
                }
            }

            // Build a String used to index the image by concatinating
            // the caption with all comments
            String body = captionText + " ";
            for (String comment : commentsText)
            {
                body += comment + " ";
            }
            
            // remove the hashtag used to crawl tha data, as every 
            // document contains it, thus it is useless for creating a reverse index
            body = body.replaceAll(CRAWLER_TAG, "");
            // remove hashtags and linebreaks
            body = body.replaceAll("#", "");
            body = body.replaceAll("\n", " ");

            // Create a Lucene Document and add attributes
            Document doc = new Document();
            doc.add(new TextField("user", userText, Field.Store.YES));
            doc.add(new TextField("hashtags", hashtagText, Field.Store.YES));
            doc.add(new StringField("id", idText, Field.Store.YES));

            
            //Index location
            if (!json.isNull("location"))
            {
                JsonObject location = json.getJsonObject("location");

                if (location != null)
                {
                    //if the location has a name, eg "Laguna Beach", I add it to
                    // the body (caption + comments) to use it for search
                    if (location.containsKey("name"))
                    {
                        if (!location.isNull("name"))
                        {
                            body = body + " " + location.getString("name");
                        }
                    }
                    //add gps coordinates to the spatial index
                    if (location.containsKey("longitude") && location.containsKey("latitude"))
                    {
                        double lat = location.getJsonNumber("latitude").doubleValue();
                        double lon = location.getJsonNumber("longitude").doubleValue();

                        try
                        {
                            Point p = spacialContext.makePoint(lon, lat);
                            for (Field field : spatialStrategy.createIndexableFields(p))
                            {
                                doc.add(field);
                            }
                        } catch (InvalidShapeException e)
                        {
                            System.out.println("corrupt location");
                        }

                    }
                }

            }

            doc.add(new TextField("body", body, Field.Store.YES));

            System.out.println(idText);

            try
            {
                w.addDocument(doc);
            } catch (IOException ex)
            {
                Logger.getLogger(Selfy.class.getName()).log(Level.SEVERE, null, ex);
            }

        } catch (FileNotFoundException ex)
        {
            Logger.getLogger(Selfy.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)
    {
        // TODO code application logic here
        new Selfy();
    }

}
