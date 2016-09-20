/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucuenca.kodar.clusters.evaluation;

import edu.ucuenca.kodar.clusters.Clustering;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public class Evaluate {

    public void elbowMethod(int start, int end, int inc) throws Exception {
        List<Double> interclusterdensitiesKMeans = new LinkedList<>();
        List<Double> interclusterdensitiesFkMeans = new LinkedList<>();
        for (int i = start; i <= end; i += inc) {
            Clustering c = new Clustering("/home/cuent/Downloads/tuple.csv");
            //Clustering c = new Clustering("src/test/resources/edu/ucuenca/kodar/data/evaluation.csv");
            c.setEvaluation(true);
            c.run(i);
            interclusterdensitiesKMeans.add(c.getInterClusterDensityKmeans());
            interclusterdensitiesFkMeans.add(c.getInterClusterDensityFuzzyKmeans());
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
    }

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("log4j.properties");

        Evaluate e = new Evaluate();
        e.elbowMethod(50, 600, 50);
    }
}
