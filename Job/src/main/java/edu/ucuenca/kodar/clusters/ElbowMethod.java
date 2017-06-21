/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.clusters;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public class ElbowMethod {

    public int run(int start, int end, int inc, double delta, String path) throws Exception {
        List<Double> interclusterdensitiesKMeans = new LinkedList<>();
        List<Double> interclusterdensitiesFkMeans = new LinkedList<>();
        int finalK = 0;
        for (int i = start; i <= end; i += inc) {
            Clustering c = new Clustering(path);
            c.setEvaluation(true);
            c.run(i);
            interclusterdensitiesKMeans.add(c.getInterClusterDensityKmeans());
            interclusterdensitiesFkMeans.add(c.getInterClusterDensityFuzzyKmeans());

            if (1.0 - c.getInterClusterDensityKmeans() <= delta) {
                finalK = i;
                break;
            }
        }

        int k = start;
        System.out.println("### Inter-cluster densities K-Means ###");
        for (Double interclusterdensity : interclusterdensitiesKMeans) {
            System.out.println(k + "," + interclusterdensity);
            k += inc;
        }

        k = start;
        System.out.println("### Inter-cluster densities Fuzzy K Means ###");
        for (Double interclusterdensity : interclusterdensitiesFkMeans) {
            System.out.println(k + "," + interclusterdensity);
            k += inc;
        }

        return finalK;
    }
}
