// Class with some static WordNet helper functions
// Daniel Shiffman
// Programming from A to Z, Spring 2007
// http://www.shiffman.net/a2z
package edu.ucuenca.kodar.utils.nlp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;

import net.didion.jwnl.*;
import net.didion.jwnl.data.*;
import net.didion.jwnl.data.list.*;
import net.didion.jwnl.data.relationship.*;
import net.didion.jwnl.dictionary.*;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;

public class NlpUtils {

    // Dictionary object
    public static Dictionary wordnet;
    private static NlpUtils instance;

    private NlpUtils() {
        initialize("properties.xml");
    }

    public static NlpUtils getInstance() {
        if (instance == null) {
            instance = new NlpUtils();
        }
        return instance;
    }

    // Initialize the database!
    private void initialize(String propsFile) {
        try {
            JWNL.initialize(new FileInputStream(propsFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (JWNLException e) {
            e.printStackTrace();
        }
        // Create dictionary object
        wordnet = Dictionary.getInstance();
    }

    // Return array of POS objects for a given String
    public POS[] getPOS(String s) throws JWNLException {
        // Look up all IndexWords (an IndexWord can only be one POS)
        IndexWordSet set = wordnet.lookupAllIndexWords(s);
        // Turn it into an array of IndexWords
        IndexWord[] words = set.getIndexWordArray();
        // Make the array of POS
        POS[] pos = new POS[words.length];
        for (int i = 0; i < words.length; i++) {
            pos[i] = words[i].getPOS();
        }
        return pos;
    }

    // Just gets the related words for first sense of a word
    // Revised to get the list of related words for the 1st Synset that has them
    // We might want to try all of them
    public ArrayList getRelated(IndexWord word, PointerType type) throws JWNLException {
        try {
            Synset[] senses = word.getSenses();
            // Look for the related words for all Senses
            for (int i = 0; i < senses.length; i++) {
                ArrayList a = getRelated(senses[i], type);
                // If we find some, return them
                if (a != null && !a.isEmpty()) {
                    return a;
                }
            }
        } catch (NullPointerException e) {
            // System.out.println("Oops, NULL problem: " + e);
        }
        return null;
    }

    // Related words for a given sense (do synonyms by default)
    // Probably should implement all PointerTypes
    public ArrayList getRelated(Synset sense, PointerType type) throws JWNLException, NullPointerException {
        PointerTargetNodeList relatedList;
        // Call a different function based on what type of relationship you are looking for
        if (type == PointerType.HYPERNYM) {
            relatedList = PointerUtils.getInstance().getDirectHypernyms(sense);
        } else if (type == PointerType.HYPONYM) {
            relatedList = PointerUtils.getInstance().getDirectHyponyms(sense);
        } else {
            relatedList = PointerUtils.getInstance().getSynonyms(sense);
        }
        // Iterate through the related list and make an ArrayList of Synsets to send back
        Iterator i = relatedList.iterator();
        ArrayList a = new ArrayList();
        while (i.hasNext()) {
            PointerTargetNode related = (PointerTargetNode) i.next();
            Synset s = related.getSynset();
            a.add(s);
        }
        return a;
    }

    // Just shows the Tree of related words for first sense
    // We may someday want to the Tree for all senses
    private void showRelatedTree(IndexWord word, int depth, PointerType type) throws JWNLException {
        showRelatedTree(word.getSense(1), depth, type);
    }

    private void showRelatedTree(Synset sense, int depth, PointerType type) throws JWNLException {
        PointerTargetTree relatedTree;
        // Call a different function based on what type of relationship you are looking for
        if (type == PointerType.HYPERNYM) {
            relatedTree = PointerUtils.getInstance().getHypernymTree(sense, depth);
        } else if (type == PointerType.HYPONYM) {
            relatedTree = PointerUtils.getInstance().getHyponymTree(sense, depth);
        } else {
            relatedTree = PointerUtils.getInstance().getSynonymTree(sense, depth);
        }
        // If we really need this info, we wil have to write some code to Process the tree
        // Not just display it  
        relatedTree.print();
    }

    // This method looks for any possible relationship
    public Relationship getRelationship(IndexWord start, IndexWord end, PointerType type) throws JWNLException {
        // All the start senses
        Synset[] startSenses = start.getSenses();
        // All the end senses
        Synset[] endSenses = end.getSenses();
        // Check all against each other to find a relationship
        for (int i = 0; i < startSenses.length; i++) {
            for (int j = 0; j < endSenses.length; j++) {
                RelationshipList list = RelationshipFinder.getInstance().findRelationships(startSenses[i], endSenses[j], type);
                if (!list.isEmpty()) {
                    return (Relationship) list.get(0);
                }
            }
        }
        return null;
    }

    // If you have a relationship, this function will create an ArrayList of Synsets
    // that make up that relationship
    public ArrayList getRelationshipSenses(Relationship rel) throws JWNLException {
        ArrayList a = new ArrayList();
        PointerTargetNodeList nodelist = rel.getNodeList();
        Iterator i = nodelist.iterator();
        while (i.hasNext()) {
            PointerTargetNode related = (PointerTargetNode) i.next();
            a.add(related.getSynset());
        }
        return a;
    }

    // Get the IndexWord object for a String and POS
    public IndexWord getWord(POS pos, String s) throws JWNLException {
        IndexWord word = wordnet.getIndexWord(pos, s);
        return word;
    }

    // Words are separated by tokens
    public String[] tokenizer(String text) throws FileNotFoundException, IOException {
        //load model to train tokens in english language
        InputStream is = new FileInputStream("en-token.bin");
        TokenizerModel tm = new TokenizerModel(is);
        Tokenizer tokenizer = new TokenizerME(tm);
        String[] tokens = tokenizer.tokenize(text);

        if (is != null) {
            is.close();
        }
        return tokens;
    }

    //Eliminate most common words of english language
    public String stopWords(String text) throws IOException {
        Analyzer analyzer = new StopAnalyzer(Version.LUCENE_46);
        //load default dataset of stop words from Lucene
        TokenStream ts = analyzer.tokenStream("contents", new StringReader(text));

        OffsetAttribute offsetAttribute = ts.addAttribute(OffsetAttribute.class);
        CharTermAttribute charTermAttribute = ts.addAttribute(CharTermAttribute.class);

        ts.reset();
        text = "";

        while (ts.incrementToken()) {
//            int startOffset = offsetAttribute.startOffset();
//            int endOffset = offsetAttribute.endOffset();
            String term = charTermAttribute.toString();
            text += term + " ";
        }

        return text;
    }

    public void findPartsOfSpeech(String word) throws JWNLException {
        System.out.println("\nFinding parts of speech for " + word + ".");
        // Get an array of parts of speech
        POS[] pos = getPOS(word);
        // If we found at least one we found the word
        if (pos.length > 0) {
            // Loop through and display them all
            for (int i = 0; i < pos.length; i++) {
                System.out.println("POS: " + pos[i].getLabel());
            }
        } else {
            System.out.println("I could not find " + word + " in WordNet!");
        }
    }
}
