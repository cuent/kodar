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
package edu.ucuenca.kodar.utils.nlp;

import java.util.ArrayList;

/**
 *
 * @author cuent
 */
public class Word {

    private String value;
    private ArrayList<String> definitions;
    private ArrayList<String> hypernymy;
    private ArrayList<String> hyponymy;
    private ArrayList<String> synonym;
    private ArrayList<String> similar;
    
    private static final String newline = System.getProperty("line.separator");

    public Word(String value) {
        this.value = value;
        definitions = new ArrayList<>();
        hypernymy = new ArrayList<>();
        hyponymy = new ArrayList<>();
        synonym = new ArrayList<>();
        similar = new ArrayList<>();
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public ArrayList<String> getDefinition() {
        return definitions;
    }

    public void setDefinition(ArrayList<String> definition) {
        this.definitions = definition;
    }

    public ArrayList<String> getHypernymy() {
        return hypernymy;
    }

    public void setHypernymy(ArrayList<String> hypernymy) {
        this.hypernymy = hypernymy;
    }

    public ArrayList<String> getHyponymy() {
        return hyponymy;
    }

    public void setHyponymy(ArrayList<String> hyponymy) {
        this.hyponymy = hyponymy;
    }

    public ArrayList<String> getSynonym() {
        return synonym;
    }

    public void setSynonym(ArrayList<String> synonym) {
        this.synonym = synonym;
    }

    public ArrayList<String> getSimilar() {
        return similar;
    }

    public void setSimilar(ArrayList<String> similar) {
        this.similar = similar;
    }

    @Override
    public String toString() {
        StringBuilder words = new StringBuilder();
        words.append(value);
        words.append(newline);

        for (String definition : definitions) {
            words.append(definition);
            words.append(newline);
        }
        for (String hyper : hypernymy) {
            words.append(hyper);
            words.append(newline);
        }
        for (String hypon : hyponymy) {
            words.append(hypon);
            words.append(newline);
        }
        for (String syn : synonym) {
            words.append(syn);
            words.append(newline);
        }
        for (String sim : similar) {
            words.append(sim);
            words.append(newline);
        }

        return words.toString();
    }

}
