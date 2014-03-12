/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package selfy;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author lukas
 */
public class QueryAnswerer
{

    private final StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
    private Directory index;
    private final IndexSearcher searcher;

    private boolean running;

    public QueryAnswerer() throws IOException
    {

        index = new SimpleFSDirectory(new File("index"));

        IndexReader reader = IndexReader.open(index);
        searcher = new IndexSearcher(reader);
    }

    public List<String> searchHadoop(String query)
    {
        String[] terms = query.split("\\W");
        //remove stop words
        //okay, stop words are removed...just don't search for stopwords
        //Find index files
        final HashMap<String, Double> documents = new HashMap<>();
        for (String term : terms)
        {
            term = term.toLowerCase();
            long d = term.hashCode();
            d = d - Integer.MIN_VALUE;
            int indexNumber = (int) (d % 256);
            System.out.println(indexNumber);
            File index = new File("hadoopIndex/" + indexNumber + ".json");

                //load index, add all files to the documents hashmap with
            //appropriate scores.
            JsonReader reader;
            JsonObject json;
            //Open JSON
            try
            {
                reader = Json.createReader(new FileReader(index));
                json = reader.readObject();
                System.out.println(term);
                if (json.containsKey(term))
                {
                    System.out.println("Found the term " + term + " in index");
                    json = json.getJsonObject(term);
                    JsonArray jsonArray = json.getJsonArray("documents");
                    double n = json.getJsonNumber("documentCount").doubleValue();
                    double N = 400000; //YAY, hardcoded
                    for (int i = 0; i < jsonArray.size(); i++)
                    {
                        JsonObject jsonObject = jsonArray.getJsonObject(i);
                        String id = jsonObject.getString("document");
                        double f = jsonObject.getJsonNumber("score").doubleValue();
                        
                        //pseudo bm25 score with k=1.2 and documentlength=average documentlength and assuming queryfrequency=1
                        double score = 2.2*f/(1.2*f) * Math.log((N - n + 0.5) / (n + 0.5));

                        if (!documents.containsKey(id))
                        {
                            documents.put(id, score);
                        }
                        else
                        {
                            documents.put(id, documents.get(id) + score);
                        }

                    }
                }
            } catch (javax.json.stream.JsonParsingException e)
            {
                System.out.print("failed"+e.toString());
            } catch (FileNotFoundException ex)
            {
                Logger.getLogger(QueryAnswerer.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
        ArrayList<String> ranking = new ArrayList(documents.keySet());
        Collections.sort(ranking, new Comparator<String>()
        {

            @Override
            public int compare(String o1, String o2)
            {
                return (int) Math.signum(documents.get(o2) - documents.get(o1));
            }
        }
        );
        return ranking.subList(0, Math.min(ranking.size(),64));
    }

    public List<String> searchSpatial(String query, double a, double b, double c, double d)
    {
        return null;
    }

    public List<String> searchLucene(String query)
    {
        query = query.replaceAll("#", " hashtags:");
        Set<String> results = new HashSet<>(50);
        ArrayList<String> ranking = new ArrayList<>();
        try
        {
            Query q = new QueryParser(Version.LUCENE_CURRENT, "body", analyzer).parse(query);

            TopScoreDocCollector collector = TopScoreDocCollector.create(64, true);
            searcher.search(q, collector);
            ScoreDoc[] hits = collector.topDocs().scoreDocs;

            for (int i = 0; i < hits.length; ++i)
            {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                if (!results.contains(d.get("id")))
                {
                    results.add(d.get("id"));
                    ranking.add(d.get("id"));
                }
                //System.out.println((i + 1) + ". " + d.get("body") + "\t" + d.get("hashtags") + "\t" + d.get("id"));
            }

        } catch (ParseException ex)
        {
            Logger.getLogger(QueryAnswerer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex)
        {
            Logger.getLogger(QueryAnswerer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ranking;
    }

    private class QueryThread implements Runnable
    {

        private Socket socket;

        public QueryThread(Socket s)
        {
            this.socket = s;
        }

        @Override
        public void run()
        {
            BufferedReader inFromClient = null;
            try
            {
                inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                DataOutputStream outToClient = new DataOutputStream(socket.getOutputStream());
                String query = inFromClient.readLine();
                char method = query.charAt(0);
                query = query.substring(1);
                if (query.equals("killtheserver"))
                {
                    running = false;
                }
                outToClient.writeBytes("{\"data\":[");
                Object[] search;
                if (method == 'l')
                {
                    search = searchLucene(query).toArray();
                }
                else if (method == 'h')
                {
                    search = searchHadoop(query).toArray();
                }
                else if (method == 'g')
                {
                    throw new RuntimeException("not implemented");
                }
                else
                {
                    search = searchLucene(query).toArray();
                }
                System.out.println("Found " + search.length + " hits.");
                for (int i = 0; i < search.length; i++)
                {
                    String id = (String) search[i];
                    String dir = id.substring(0, 5);
                    File f = new File("/Users/lukas/Desktop/Desktop/selfies/" + dir + "/" + id + ".json");
                    if (!f.exists())
                    {
                        continue;
                    }
                    FileInputStream reader = new FileInputStream(f);
                    int read;
                    while ((read = reader.read()) != -1)
                    {
                        outToClient.write(read);
                    }
                    if (i != search.length - 1)
                    {
                        outToClient.writeBytes(",");
                    }
                }
                outToClient.writeBytes("]}\n");
                outToClient.flush();
                outToClient.close();
            } catch (IOException ex)
            {
                Logger.getLogger(QueryAnswerer.class.getName()).log(Level.SEVERE, null, ex);
            } finally
            {
                try
                {
                    inFromClient.close();
                } catch (IOException ex)
                {
                    Logger.getLogger(QueryAnswerer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public void serverLoop() throws IOException, InterruptedException
    {
        running = true;
        ServerSocket server = new ServerSocket(8999);
        server.setSoTimeout(1);
        ExecutorService e = Executors.newFixedThreadPool(Math.min(32, Math.max(1, Runtime.getRuntime().availableProcessors())));
        while (running)
        {
            try
            {
                Socket accept = server.accept();
                e.execute(new QueryThread(accept));
            } catch (SocketTimeoutException ex)
            {

            }
        }
        server.close();
        e.awaitTermination(1, TimeUnit.SECONDS);
        e.shutdown();
    }

    public static void main(String... args) throws IOException, InterruptedException
    {
        System.out.println("Starting Searchengine Backbone");

        QueryAnswerer q = new QueryAnswerer();
        //System.out.println(q.searchHadoop("pizza pretty"));
        q.serverLoop();
        System.out.println("Bye Bye!");
    }
}
