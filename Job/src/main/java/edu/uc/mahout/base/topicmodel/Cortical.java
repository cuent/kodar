/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.uc.mahout.base.topicmodel;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import edu.ucuenca.kodar.utils.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public class Cortical {

    private List<String> labels = new ArrayList<>();

    public void addLabels(String text) throws IOException {
        labels.add(Service.getInstance().findKeywords(text));
    }

    public void removeLabels(int index) {
        labels.remove(index);
    }

    public String getLabel() throws IOException {
        StringBuilder text = new StringBuilder();

        for (String label : labels) {
            text.append(label);
        }
        JsonArray array = (JsonArray)new JsonParser().parse(Service.getInstance().findKeywords(text.toString()));
        return array.get(0).getAsString();
    }

    
}
