package com.luca.filipponi.tweetAnalysis.Analyzer;


import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.Version;

import java.io.Reader;

/**
 * MyTokenizer is an analyzer similar to whiteSpace tokenizer, but with more separator value
 * due to the presence of many noise char value in the tweet (! . ecc ecc)
 */
public final class MyTokenizer extends CharTokenizer {

    /**
     * Construct a MyTokenizer
     *
     * @param matchVersion Version Lucene version
     * @param in           the input to split up into tokens
     */
    public MyTokenizer(Version matchVersion, Reader in) {
        super(matchVersion, in);
    }

    /**
     * Construct a MyTokenizer
     *
     * @param matchVersion Lucene version to match See
     *                     {@link <a href="#version">above</a>}
     * @param factory      the attribute factory to use for this {@link Tokenizer}
     * @param in           the input to split up into tokens
     */
    public MyTokenizer(Version matchVersion, AttributeFactory factory, Reader in) {
        super(matchVersion, factory, in);
    }

    /**
     * Most important function of the class, here is defined all character that constitutes the separator
     * for the tokenizer, is an extension fo the whitspaceTokenizer, becouse in addiction to the
     * Charachter.isWhiteSpace(c) is added the char . ! = | and more will be added
     */
    @Override
    protected boolean isTokenChar(int c) {
        boolean check = (Character.isWhitespace(c) || //check if whitspace
                c == '.' ||  //remove the useless punctuation
                c == '"' ||
                c == '!' ||
                c == '|' ||
                c == ':' ||
                c == '…' || // for Horizontal ellipsis … in utf-16 see http://unicode-table.com/en/#control-character
                c == '“' || // value for Left double quotation mark “ in utf-16
                c == '‘' ||
                c == '’' ||
                c == '«' ||
                c == '»' ||             //potrebbe sembrare strano che nn ho inserito l'underscore, ma è l'unico
                c == '(' ||             //carattere utilizzabile nei nomi utenti oltre ai numer
                c == ')' ||             //quindi ho preferito tenerlo per nn perdere i nomi utenti
                c == '”' ||
                c == '\'' ||
                c == '=' ||
                c == '∞' ||
                c == '^' ||
                c == '*' ||
                c == '§' ||
                c == ';' ||
                c == '\\' || //backslash
                c == ',' ||
                c == '‹' ||
                c == '›' ||
                c == '>' ||
                c == '<' ||
                c == '›' ||
                c == '[' ||
                c == ']' ||
                c == '{' ||
                c == '}' ||
                c == '?' || //il punto interrogativo cosi come quello esclamativo mi poteva dare qualche significato semantico
                c == '+' ||
                c == '-' ||
                // c == '_' || se tolgo il @ non posso metterlo, senno non potrei risalire a utenti cn il nome cn _
                // c == '@' || //cn questo elimino il riferimento alla menzione, ma l'utente apparirà ugualmente
                c == '#'); //cn questo ultimo carattere mi scompaiono gli hashtag, anche se perdo l'informazione
        //dell'hashtag ho il vantaggio di accorpare insiene, #grillo e grillo, in questo modo le occorrenze di
        //parole di questo tipo aumenteranno notevolmente (cioè parole che apparivano sia da sole che in hashtag)


        //e qui sia inserendo i caratteri, che inserendo il valore in decimale degli elementi da togliere posso cambiare la tokenizzazione
        //probabile che i caratteri speciali li posso trovare anche con alt, tipo char p='¥' (alt+4)
        //da vedere se in qualche modo la codifica del file da problemi.

        return !check;    //return with the ! beacause if false don't take
    }
}