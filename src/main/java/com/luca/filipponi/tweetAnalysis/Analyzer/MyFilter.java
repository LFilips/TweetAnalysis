package com.luca.filipponi.tweetAnalysis.Analyzer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.util.Version;

import java.io.IOException;

/**
 * Class used for override the Tokenizer and Analyzer from lucene.
 */


public class MyFilter extends FilteringTokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public MyFilter(Version version, boolean enablePositionIncrements,
                    TokenStream input) {
        super(version, input);

    }

    @Override
    protected boolean accept() throws IOException {

        String s = new String(termAtt.buffer(), 0, termAtt.length());
        /*
         *
		 * Qua mi è venuta in mente una fissa sulle prestazioni, perchè per ogni
		 * token sto creando un oggetto stringa ed ho paura che possa rallentare troppo
		 * anche se cmq le stringhe sono un tipo "primitivo" e molto ottimizzato
		 * a questo punto potrei anche pensare a fare unconfronto diverso piu a basso livello
		 * magari con direttamente con i caratteri (anche l'implementazione delle stopfilter è leggermente diversa)
		 * potrei fare in modo di usare il charSet delle stop word per andare ad elimare quei token che iniziano 
		 * con "co/" ho pensato anche a un qualcosa che per ogni token oltre a vedere la stop word
		 * 
		 * 
		 */

        //elimate all the token that starts with co/ (all the link) and when starts with //
        if (s.startsWith("co/") || s.startsWith("/") || s.startsWith("aha") || s.startsWith("ahh"))
            return false;


        /*
        Qui ci sono un po di cose importanti rispetto al co/ e al //:

        Ho dovuto inserirli per il grande numero di link che c'erano all'interno dei tweet, tutti della forma
        http://t.co/IGhgap ....
        ed avendo inserito il punto e i due punti come separatori di stringa, mi vengono fuori i token

        http //t co/IGha ...

        http è tra le stop word, e con il pezzo di codice superiore elimino anche questi due pezzi
        anche se la parte del // mi serve anche per altri tipi di link


        Un altra cosa importante è come valutare le menzioni, il problema è che quando si menziona un utente di twitter
        si scrive @nomeutente, e puo anche contenere un underscore, ma a questo punto è meglio lasciare la chiocciola
        per far vedere esplicitamente che è un utente


         */


        return true;
    }

}
