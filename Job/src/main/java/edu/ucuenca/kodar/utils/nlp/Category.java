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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.didion.jwnl.JWNLException;
import net.didion.jwnl.data.IndexWord;
import net.didion.jwnl.data.POS;
import net.didion.jwnl.data.PointerType;
import net.didion.jwnl.data.Synset;
import net.didion.jwnl.data.relationship.Relationship;

/**
 *
 * @author cuent
 */
public class Category {

    private String kw;
    private HashMap<String, Word> corpus;

    public Category(String text) throws IOException, JWNLException {
        corpus = new HashMap<>();
        this.kw = text;
    }

    public void populate() throws JWNLException, IOException {
        String[] keywords = kw.split(",");
        for (String keyword : keywords) {
            keyword = NlpUtils.getInstance().stopWords(keyword);
            POS[] pos = NlpUtils.getInstance().getPOS(keyword);
            if (pos.length > 0) {
                for (POS po : pos) {
                    generate(po, keyword);
                }
            } else {
                String[] tokens = NlpUtils.getInstance().tokenizer(keyword);
                for (String token : tokens) {
                    pos = NlpUtils.getInstance().getPOS(token);
                    if (pos.length > 0) {
                        for (POS p : pos) {
                            generate(p, token);
                        }
                    }
                }
//                if (tokens.length == 2) {
//                    POS[] posStart = NlpUtils.getInstance().getPOS(tokens[0]);
//                    POS[] posEnd = NlpUtils.getInstance().getPOS(tokens[1]);
//                    for (POS ps : posStart) {
//                        for (POS pe : posEnd) {
//                            findRelationships(NlpUtils.getInstance().getWord(ps, tokens[0]), NlpUtils.getInstance().getWord(pe, tokens[1]),
//                                    PointerType.HYPERNYM);
//                            findRelationships(NlpUtils.getInstance().getWord(ps, tokens[0]), NlpUtils.getInstance().getWord(pe, tokens[1]),
//                                    PointerType.SIMILAR_TO);
//                        }
//                    }
//                }
            }
        }
    }

    //Load definitions, synonyms, hyponym and hypernym
    private void generate(POS pos, String word) throws JWNLException {
        definitions(NlpUtils.getInstance().getWord(pos, word));
        relatedWords(NlpUtils.getInstance().getWord(pos, word), PointerType.SIMILAR_TO);
        relatedWords(NlpUtils.getInstance().getWord(pos, word), PointerType.HYPONYM);
        relatedWords(NlpUtils.getInstance().getWord(pos, word), PointerType.HYPERNYM);
    }

    private void definitions(IndexWord word) throws JWNLException {
        // Get an array of Synsets for a word
        Synset[] senses = word.getSenses();
        // Display all definitions
        for (int i = 0; i < senses.length; i++) {
            Word w = corpus.containsKey(word.getLemma())
                    ? corpus.get(word.getLemma())
                    : new Word(word.getLemma());
            w.getDefinition().add(senses[i].getGloss());
            corpus.put(word.getLemma(), w);
        }
    }

    // This function lists related words of type of relation for a given word
    private void relatedWords(IndexWord w, PointerType type) throws JWNLException {
        // Call a function that returns an ArrayList of related senses
        ArrayList a = NlpUtils.getInstance().getRelated(w, type);
        if (a != null) {
            // Display the words for all the senses
            for (int i = 0; i < a.size(); i++) {
                Synset s = (Synset) a.get(i);
                net.didion.jwnl.data.Word[] words = s.getWords();
                for (int j = 0; j < words.length; j++) {
                    Word w1 = corpus.containsKey(w.getLemma())
                            ? corpus.get(w.getLemma())
                            : new Word(w.getLemma());

                    switch (type.getLabel()) {
                        case "similar":
                            w1.getSynonym().add(words[j].getLemma());
                            break;
                        case "hypernym":
                            w1.getHypernymy().add(words[j].getLemma());
                            break;
                        case "hyponym":
                            w1.getHyponymy().add(words[j].getLemma());
                            break;
                    }
                    corpus.put(w.getLemma(), w1);
                }
            }
        }
    }

    private void findRelationships(IndexWord start, IndexWord end, PointerType type) throws JWNLException {
        // Ask for a Relationship object
        Relationship rel = NlpUtils.getInstance().getRelationship(start, end, type);
        // If it's not null we found the relationship
        if (rel != null) {
            // Get a list of the words that make up the relationship
            ArrayList a = NlpUtils.getInstance().getRelationshipSenses(rel);
            // Display all senses
            for (int i = 0; i < a.size(); i++) {
                Synset s = (Synset) a.get(i);
                net.didion.jwnl.data.Word[] words = s.getWords();
                for (int j = 0; j < words.length; j++) {
                    Word wStart = corpus.containsKey(start.getLemma())
                            ? corpus.get(start.getLemma())
                            : new Word(start.getLemma());
                    Word wEnd = corpus.containsKey(end.getLemma())
                            ? corpus.get(end.getLemma())
                            : new Word(end.getLemma());
                    wStart.getSimilar().add(words[j].getLemma());
                    wEnd.getSimilar().add(words[j].getLemma());
                    corpus.put(start.getLemma(), wStart);
                    corpus.put(end.getLemma(), wEnd);
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder text = new StringBuilder();
        Iterator it = corpus.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<String, Word> pair = (Map.Entry) it.next();
            text.append(pair.getValue().toString());
            text.append(System.getProperty("line.separator"));
        }
        return text.toString();
    }

}
