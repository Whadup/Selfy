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
import org.apache.lucene.store.SimpleFSDirectory;

/**
 *
 * @author lukas
 */
public class Selfy
{

    private final StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
    private Directory index;
    private IndexWriter w;
    
    public Selfy()
    {
        try
        {
            index  = new SimpleFSDirectory(new File("index"));
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
            w = new IndexWriter(index, config);

            int i = 0;

            for (File selfie : new File("/Users/lukas/Desktop/selfies").listFiles())
            {
                if (selfie.isFile() && selfie.getName().endsWith(".json"))
                {
                    addSelfie(selfie);
                    i++;
                }

              
            }
            w.commit();
            w.close();
            index.close();
            

        } catch (IOException ex)
        {
            w=null;
            index = null;
            Logger.getLogger(Selfy.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
    }

    public void addSelfie(File f)
    {
        try
        {
            System.out.print(f.getName() + ": ");
            JsonReader reader = Json.createReader(new FileReader(f));
            JsonObject json = reader.readObject();

            String captionText = "";
            ArrayList<String> commentsText = new ArrayList<>();
            String hashtagText = "";
            String idText = "";
            String userText = "";
            
            
            if (!json.isNull("caption"))
            {
                JsonObject caption = json.getJsonObject("caption");
                captionText = caption.getString("text");
            }

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

            if (!json.isNull("tags"))
            {
                JsonArray tags = json.getJsonArray("tags");
                for (int i = 0; i < tags.size(); i++)
                {
                    String s=tags.getString(i);
                    if(!s.equals("selfie"))
                        hashtagText += s + " ";
                }
            }
            if(!json.isNull("id"))
                idText = json.getString("id");
            
            if(!json.isNull("user"))
            {
                JsonObject user = json.getJsonObject("user");
                if(!user.isNull("username"))
                    userText = user.getString("username");
            }
            
            
            
            String body = captionText + " ";
            for (String comment : commentsText)
            {
                body += comment + " ";
            }
            body = body.replaceAll("#selfie", "");
            body = body.replaceAll("#", "");
            body = body.replaceAll("\n", " ");
            
            if(!json.isNull("location"))
            {
                JsonObject location = json.getJsonObject("location");
                if(!location.isNull("name"))
                    body = body + " " + location.getString("location");
                double lat = Double.parseDouble(json.getString("latitude"));
                double lon = Double.parseDouble(json.getString("longitude"));
            }
            
            System.out.println(idText);
            
            Document doc = new Document();
            doc.add(new TextField("body", body, Field.Store.YES));
            doc.add(new TextField("user", userText, Field.Store.YES));
            doc.add(new TextField("hashtags", hashtagText, Field.Store.YES));
            doc.add(new StringField("id",idText,Field.Store.YES));
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
