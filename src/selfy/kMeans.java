/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package selfy;

import java.io.File;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author lukas
 */
public class kMeans
{

    public static double distance(Double[] a, Double[] b)
    {
        double sum = 0;
        for (int i = 0; i < a.length; i++)
        {
            sum += (a[i] - b[i]) * (a[i] - b[i]);
        }
        return sum;
    }
    
    public static Double[][] kMeans(File f)
    {
        return null;
    }
    
    public static Double[][] kMeans(List<Double[]> dataset, int k)
    {
        Double[][] centroids = new Double[k][];
        for (int i = 0; i < k; i++)
        {
            centroids[i] = dataset.get((int) Math.random() * dataset.size());
        }
        HashSet<Double[]>[] clusters = new HashSet[k];
        for (int i = 0; i < k; i++)
        {
            clusters[i] = new HashSet<>();
        }
        for (int iteration = 0; iteration < 20; iteration++)
        {
            for (int i = 0; i < k; i++)
            {
                clusters[i].clear();
            }
            for (Double[] d : dataset)
            {
                double min = distance(d, centroids[0]);
                int minn = 0;
                for (int i = 1; i < k; i++)
                {
                    double dd = distance(d, centroids[i]);
                    if (dd < min)
                    {
                        min = dd;
                        minn = i;
                    }
                }
                clusters[minn].add(d);
            }
            for (int i = 0; i < k; i++)
            {
                Double[] newCentroid = new Double[dataset.get(0).length];
                for (int j = 0; j < newCentroid.length; j++)
                {
                    newCentroid[j] = 0d;
                }
                for (Double[] d : clusters[i])
                {
                    for (int j = 0; j < newCentroid.length; j++)
                    {
                        newCentroid[j] += d[j];
                    }
                }
                for (int j = 0; j < newCentroid.length; j++)
                {
                    newCentroid[j] /= clusters[i].size();
                }
                centroids[i] = newCentroid;
            }
        }
        return centroids;

    }
}
