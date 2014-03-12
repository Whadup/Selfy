/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package selfy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author lukas
 */
public class HadoopFileMerger
{

    public void mergeFiles(String path)
    {
        FileOutputStream currentStream = null;
        try
        {
            int i = 1;
            File current = new File("hadoop/1.json");
            currentStream = new FileOutputStream(current);
            currentStream.write("{\"data\":[".getBytes());
            int size = 0;
            for (File dir : new File(path).listFiles())
            {
                if (dir.isDirectory())
                {
                    for (File selfie : dir.listFiles())
                    {
                        if(!selfie.getName().endsWith(".json"))
                            continue;
                        FileInputStream inputStream = new FileInputStream(selfie);
                        int read;
                        while ((read = inputStream.read()) != -1)
                        {
                            currentStream.write(read);
                            size++;
                        }
                        inputStream.close();
                        
                        if (size < 10000000)
                        {
                            currentStream.write(",".getBytes());
                        }
                        else
                        {
                            currentStream.write("]}\n".getBytes());
                            currentStream.close();
                            i++;
                            current = new File("hadoop/" + i + ".json");
                            currentStream = new FileOutputStream(current);
                            currentStream.write("{\"data\":[".getBytes());
                            size = 0;
                        }

                    }

                }
            }
            currentStream.write("]}\n".getBytes());
            currentStream.close();
        } catch (FileNotFoundException ex)
        {
            Logger.getLogger(HadoopFileMerger.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex)
        {
            Logger.getLogger(HadoopFileMerger.class.getName()).log(Level.SEVERE, null, ex);
        } finally
        {
            try
            {
                currentStream.write("]}\n".getBytes());
                currentStream.close();
            } catch (IOException ex)
            {
                Logger.getLogger(HadoopFileMerger.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void main(String... args)
    {
        new HadoopFileMerger().mergeFiles(args.length > 0 ? args[0] : "/Users/lukas/Desktop/Desktop/selfies");
    }

}
