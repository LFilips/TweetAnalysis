/**
 * Created by luca on 09/07/14.
 */


/*

 m5s                                     => 0.05480624688344097
 vinciamonoi                             =>0.033548660999835715
 pd                                      =>0.009356974165495961
 piazzapulita                            =>0.006866244241482181
 @beppe_grillo                           =>0.006425017232813838
 vinciamopoi                             =>0.005651820209935984
 ep2014                                  =>0.0042734776597554355
 italia                                  =>0.003734751231244024
 renzi                                   =>0.0036934067809166514
 vinciamonoi m5s                         =>0.0034849174379124816



 */


c3.generate({
    bindto: '#chart',
    data: {
        columns: [
            ['Word Ranking Cluster 0', 0.05480, 0.03354, 0.00935, 0.00686, 0.00642, 0.00565, 0.00427, 0.00373, 0.003693, 0.003484]
        ],
        type: 'bar'
    },
    bar: {
        width: {
            ratio: 0.5 // this makes bar width 50% of length between ticks
        }
        // or
        //width: 100 // this makes bar width 100px
    },

    axis: {

        x: {
            type: 'category',
            categories: ['m5s', 'vinciamonoi', 'pd', 'piazzapulita', '@beppe_grillo', 'vinciamopoi', 'ep2014', 'italia', 'renzi', 'vinciamonoi m5s'],
            show: true,

            label: { // ADD
                text: 'Top 10 Word',
                position: 'outer-left'
            }
        },
        y: {
            show: true,
            label: { // ADD
                text: 'Word Score',
                position: 'outer-middle'
            }
        }
        /*
         regions: [
         //le regioni non funzionano
         {axis: 'y', start: minValue, end: threshold1, class: 'regionRed'},
         {axis: 'y', start: treshold1, end: maxValue, class: 'regionGreen'},

         ]
         */
    }






});

/*
 politica                                => 0.07184456108125664
 europee2014                             => 0.01673390920330253
 italia                                  =>0.004232680492458185
 oggi                                    =>0.0038876784396239972
 momento                                 =>0.0038466156122860086
 renzi                                   =>0.0037849743812886625
 posizione                               =>0.003583650404582145
 tweet                                   =>0.003543831275499747
 scopri                                  =>0.0033760015053979357
 grillo                                  =>0.0031763671875738144

 */


c3.generate({
    bindto: '#chart2',
    data: {
        columns: [
            ['Word Ranking Cluster 1', 0.071844, 0.016733, 0.004232, 0.003887, 0.0038466, 0.0037849, 0.003583, 0.00354383, 0.003376001, 0.00317636]
        ],
        type: 'bar'
    },
    bar: {
        width: {
            ratio: 0.5 // this makes bar width 50% of length between ticks
        }
        // or
        //width: 100 // this makes bar width 100px
    },

    axis: {

        x: {
            type: 'category',
            categories: ['politica', 'europee2014', 'italia', 'oggi', 'momento', 'renzi', 'posizione', 'tweet', 'scopri', 'grillo'],
            show: true,

            label: { // ADD
                text: 'Top 10 Word',
                position: 'outer-left'
            }
        },
        y: {
            show: true,
            label: { // ADD
                text: 'Word Score',
                position: 'outer-middle'
            }
        }
        /*
         regions: [
         //le regioni non funzionano
         {axis: 'y', start: minValue, end: threshold1, class: 'regionRed'},
         {axis: 'y', start: treshold1, end: maxValue, class: 'regionGreen'},

         ]
         */
    }






});


/*

 berlusconi                              =>  0.0324735111659255
 @beppe_grillo                           =>0.014430024600119728
 politico                                =>0.008795682852165542
 alfano                                  =>0.007343274861075387
 @forza_italia                           => 0.00588591006101028
 @matteorenzi                            =>0.005300878216309827
 italia                                  =>0.004944980714591891
 bersagliomobile                         => 0.00458640648363402
 riforme                                 =>0.004573565018712568
 @giuliadivita                           => 0.00414344573714314

 */


c3.generate({
    bindto: '#chart3',
    data: {
        columns: [
            ['Word Ranking Cluster 2', 0.032473, 0.0144300 , 0.008795, 0.007343, 0.005885, 0.005300, 0.004944, 0.004586, 0.0045735, 0.004143]
        ],
        type: 'bar'
    },
    bar: {
        width: {
            ratio: 0.5 // this makes bar width 50% of length between ticks
        }
        // or
        //width: 100 // this makes bar width 100px
    },

    axis: {

        x: {
            type: 'category',
            categories: ['berlusconi', '@beppe_grillo', 'politico', 'alfano', '@forza_italia', '@matteorenzi', 'italia', 'bersagliomobile', 'riforme', '@giuliadivita'],
            show: true,

            label: { // ADD
                text: 'Top 10 Word',
                position: 'outer-left'
            }
        },
        y: {
            show: true,
            label: { // ADD
                text: 'Word Score',
                position: 'outer-middle'
            }
        }
        /*
         regions: [
         //le regioni non funzionano
         {axis: 'y', start: minValue, end: threshold1, class: 'regionRed'},
         {axis: 'y', start: treshold1, end: maxValue, class: 'regionGreen'},

         ]
         */
    }






});


/*

 renzi                                   => 0.02484273312993139
 pd                                      =>0.024376299138269917
 lavoro                                  => 0.02145595165360417
 europee                                 => 0.01775170964142727
 elezioni                                =>0.007652726156259538
 governo                                 =>0.006506030152024203
 italia                                  =>0.0059026239242428126
 elezioni europee                        =>0.0057175658792484405
 voto                                    =>0.003974477577687858
 2014                                    =>0.0036631732236308044


 */


c3.generate({
    bindto: '#chart4',
    data: {
        columns: [
            ['Word Ranking Cluster 3', 0.024842, 0.024376 , 0.021455, 0.017751, 0.007652, 0.006506, 0.005902, 0.0057175, 0.003974, 0.003663]
        ],
        type: 'bar'
    },
    bar: {
        width: {
            ratio: 0.5 // this makes bar width 50% of length between ticks
        }
        // or
        //width: 100 // this makes bar width 100px
    },

    axis: {

        x: {
            type: 'category',
            categories: ['renzi', 'pd', 'lavoro', 'europee', 'elezioni', 'governo', 'italia', 'elezioni europee', 'voto', '2014'],
            show: true,

            label: { // ADD
                text: 'Top 10 Word',
                position: 'outer-left'
            }
        },
        y: {
            show: true,
            label: { // ADD
                text: 'Word Score',
                position: 'outer-middle'
            }
        }
        /*
         regions: [
         //le regioni non funzionano
         {axis: 'y', start: minValue, end: threshold1, class: 'regionRed'},
         {axis: 'y', start: treshold1, end: maxValue, class: 'regionGreen'},

         ]
         */
    }






});

c3.generate({
    bindto: '#chart5',
    data: {
        columns: [
            ['Word Ranking Cluster 4', 0.074095, 0.017232, 0.011551, 0.009025, 0.008459, 0.007448, 0.006495, 0.006417, 0.005405, 0.004615]
        ],
        type: 'bar'
    },
    bar: {
        width: {
            ratio: 0.5 // this makes bar width 50% of length between ticks
        }
        // or
        //width: 100 // this makes bar width 100px
    },

    axis: {

        x: {
            type: 'category',
            categories: ['grillo', 'portaaporta', 'vespa', 'renzi', 'm5s', 'beppe', 'beppe grillo', 'berlusconi', 'grilloinvespa', 'vinciamopoi'],
            show: true,

            label: { // ADD
                text: 'Top 10 Word',
                position: 'outer-left'
            }
        },
        y: {
            show: true,
            label: { // ADD
                text: 'Word Score',
                position: 'outer-middle'
            }
        }
        /*
         regions: [
         //le regioni non funzionano
         {axis: 'y', start: minValue, end: threshold1, class: 'regionRed'},
         {axis: 'y', start: treshold1, end: maxValue, class: 'regionGreen'},

         ]
         */
    }






});