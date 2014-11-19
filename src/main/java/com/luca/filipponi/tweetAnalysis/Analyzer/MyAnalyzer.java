package com.luca.filipponi.tweetAnalysis.Analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

import java.io.Reader;
import java.util.Arrays;
import java.util.List;

public class MyAnalyzer extends Analyzer {
    /**
     */

    @SuppressWarnings("resource") //suppress warning for resource leak
    protected TokenStreamComponents createComponents(String fieldName,
                                                     Reader reader) {
        /*
         * the Reader object is a java object for reading a stream of text and
		 * each bufferedReader(Reader).readLine() is a tweet, in this way all
		 * the stream is analyzed once
		 */


        //using my tokenizer that return a stream of token using some char separator
        Tokenizer source = new MyTokenizer(Version.LUCENE_48, reader);


        //Tokenizer source = new WhitespaceTokenizer(Version.LUCENE_48, reader);// separates
        // with
        // whitespace
        TokenStream filter = new LowerCaseFilter(Version.LUCENE_48, source);// all
        // in
        // lowerCase
        //adding myFilter in order to remove link and other bad thing


        //the stop word are checked only when the token are created, in this sense i've to create an italian
        //stop word list


        //sarebbe meglio trovare un modo per aggiungerlo da file "",


        //dovrei organizzare la lista di stop in modo che possa differenziare le parole
        //partendo da quelle che è piu importante aggungere, fino ad arrivare a quello che rischiano di farmi
        //perdere degli n-gram significativi, come il numero 5, che da solo non significa niente, ma crea il bi-gram
        // 5 stelle che è abbastanza in alto come ranking, l'approccio che sto seguendo per la creazione delle stop
        //word è abbastanza semplice, vado a vedere se ci sono dei termini nn significativi che hanno cmq
        //unranking alto, se questo accade allora lo inserisco nelle stop word

        //un esaustiva lista di stopword si trova qui http://snowball.tartarus.org/algorithms/italian/stop.txt
        //anche se ha un po troppe parole

        List<String> wordList = Arrays.asList(

                //derived from link decomposition
                "https", "it", "/", "//t", "http",

                //six character
                "nostro", "vostro", "nostri", "contro", "vostre", "quanto",
                "questi", "questa",


                //four e five character
                "alle", "tutti", "questo", "hanno", "fare", "fatto", "della", "delle",
                "ecco", "dava", "deve", "quelli", "quella", "quelle", "quello", "agli",
                "sono", "come", "alla", "dall", "dell", "nell", "sull", "coll", "pell",
                "dagl", "degl", "negl", "sugl", "anche", "degli", "qundi", "nella", "sulle",
                "solo", "dopo", "cosa", "sulla", "allo", "dallo", "dagli", "dalla", "dalle",
                "dello", "nello", "negli", "nelle", "sugli", "loro", "miei", "tuoi",
                "dove", "tutto", "tutti", "dice", "dire", "&amp", "però", "però",

                //three character
                "non", "suo", "più", "con", "che", "via", "sul", "può", "gli", "chi",
                "tra", "per", "all", "dei", "ora", "noi", "poi", "mio", "sia", "&lt",
                "dai", "gl", "agl", "mai", "sta", "gia", "far", "sto", "dal",
                "voi", "del", "nel", "una", "lui", "mia", "sua", "tua", "col", "cui",
                "tuo", "piu", "fai", "coi", "sui", "lei", "mie", "tue", "sue", "&gt",
                "&lt", "hai",

                //double character
                "la", "lo", "il", "!!", "ai", "al", "un", "vi", "!!!", "ha", "mi", "fa",
                "si", "ci", "le", "//", "se", "ma", "de", "so", "sa", "ce", "un",
                "nn", "li", "no", "ne", "ad", "tu", "te", "fi", "rt", "su", "io", "ho",
                "di", "ed", "va", "do", "ti", "da", "in", "po",

                //single character (ci devo mettere tutto l'alfabeto??)
                "x", "t", "s", "v", "d", "è", "i", "é", "o", "|", "a", "-", "e", "?", "c", "l", "!", "m",
                ";", "p", "n",

                //word rischiose
                "1", "2", "3", "perchè", "me", "così", "era", "già", "già", "sei", "uno", "ncd", "nei", "qui",
                "perché"
        );

        CharArraySet stopWord = new CharArraySet(Version.LUCENE_48, wordList, true);

        filter = new StopFilter(Version.LUCENE_48, filter, stopWord);

        filter = new MyFilter(Version.LUCENE_48, true, filter);


        // filter = new PorterStemFilter(filter); LO STEM FILTER POTREBBE ESSERE MOLTO UTILE
        return new TokenStreamComponents(source, filter);
    }

}