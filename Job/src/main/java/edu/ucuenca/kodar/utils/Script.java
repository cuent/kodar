/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *
 * @author cuent
 */
public class Script {

    private static final String PATH_POM = "/Users/cuent/NetBeansProjects/KODAR/Job";
    private static final String cd = "cd";
    private static final String SCRIPT_M2 = "mvn \"-Dexec.args=-classpath %classpath "
            + "edu.ucuenca.kodar.researchareas.Execute mahout-base/original/authors.csv "
            + "mahout-base/original/keywords.csv\" -Dexec.executable=/Library/Java/"
            + "JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home/bin/java "
            + "org.codehaus.mojo:exec-maven-plugin:1.2.1:exec";

    public void execute() throws IOException, InterruptedException {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "/Users/cuent/NetBeansProjects/KODAR/Job/execute.sh");
            Process p = pb.start();     // Start the process.
            p.waitFor();                // Wait for the process to finish.
            System.out.println("Script executed successfully");

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//        StringBuffer output = new StringBuffer();
//        Process p = Runtime.getRuntime().exec(String.format("%s %s", cd, PATH_POM));
//        p.waitFor();
//        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
//        BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
//        String line = "";
//        while ((line = reader.readLine()) != null) {
//            System.out.println(line + "\n");
//        }
//
//        p = Runtime.getRuntime().exec(SCRIPT_M2);
//        p.waitFor();
//        reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
//        error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
//        line = "";
//        while ((line = reader.readLine()) != null) {
//            System.out.println(line + "\n");
//        }
    }

}
