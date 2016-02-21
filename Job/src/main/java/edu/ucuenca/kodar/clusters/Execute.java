/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.clusters;

/**
 *
 * @author cuent
 */
public class Execute {

    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            Clustering c = new Clustering(args[0]);
            c.run();
        } else {
            throw new Exception("ERROR: Invalid number of arguments");
        }
    }

}
