/*
 * Calaca
 * Simple search client for ElasticSearch
 * https://github.com/romansanchez/Calaca
 * Author - Roman Sanchez
 * http://romansanchez.me
 * @rooomansanchez
 *
 * v1.0.1
 * MIT License
 */

/* Configs */
var indexName = "table";
var maxResultsSize = 100000;
var host = "localhost";
var port = 9200;
var metalist = ['caption', 'issue', 'journal', 'issn', 'volume', 'page', 'date', 'article-title']
var typeFields = {
    "table" : ["caption", "footnote_*", "header_row*", "data_row_*.value_0"],
    "row" : ["header_row*", "data_row_*.value_0"],
    "column" : ["row_header_*", "col_*.header_*"]
}

/* Module */
window.Calaca = angular.module('calaca', ['elasticsearch'],
    ['$locationProvider', function($locationProvider){
        $locationProvider.html5Mode(true);
    }]
);

/* Service to ES */
Calaca.factory('calacaService',
    ['$q', 'esFactory', '$location', function($q, elasticsearch, $location) {

         //Defaults if host and port aren't configured above
        var esHost = (host.length > 0 ) ? host : $location.host();
        var esPort = (port.length > 0 ) ? port : 9200;

        var client = elasticsearch({ host: esHost + ":" + esPort });

        var search = function(term, type) {

            var deferred = $q.defer();

            client.search({
                "index": indexName,
                "type": type,
                "body": {
                    "size": maxResultsSize,
                    "query": {
                        "multi_match": {
                            "query" : term,
                            "type" : "cross_fields",
                            "fields" : typeFields[type]
                        }
                    }
                }
            }).then(function(result) {
                var ii = 0, hits_in, hits_out = [];
                hits_in = (result.hits || {}).hits || [];
                for(;ii < hits_in.length; ii++){
                    hits_out.push(hits_in[ii]._source);
                }
                deferred.resolve(hits_out);
            }, deferred.reject);

            return deferred.promise;
        };
        return {
            "search": search
        };
    }]
);

Calaca.factory('aggregate', [function() {
    function comparer(a, b) {
        return a['table_id'] - b['table_id'];
    }

    var group = function(results) {
        if (results.length == 0 || !('table_id' in results[0])) {
            return results;
        }
        results.sort(comparer);
        var ret = []
        var prev = results[0];
        var i = 1;
        while (i < results.length) {
            var cur = results[i];
            if (cur['table_id'] == prev['table_id']) {
                for (var key in cur)
                    if (!(key in prev)) {
                        prev[key] = cur[key];
                    }
            } else {
                ret.push(prev);
                prev = cur;
            }
            i++;
        }
        ret.push(prev);
        return ret;
    }

    return {
        'group' : group
    }
}]);

Calaca.factory('renderer', ['aggregate', function(aggregate) {
    function get_list(src, tag) {
        retlist = [];
        for (var key in src) {
            if (key.search(tag) != -1) {
                idx = parseInt(key.split("_").slice(-1));
                retlist[idx] = src[key];
            }
        }
        return retlist;
    }

    var render = function(results) {
        var ret = [];
        var ii = 0;
        results = aggregate.group(results);
        for(;ii < results.length; ii++){
            var data = {};
            var table = results[ii];
            for (var i = 0; i < metalist.length; i++) {
                var tag = metalist[i];
                if (tag in table) {
                    data[tag] = table[tag];
                }
            }
            data['authors'] = get_list(table, 'author');
            data['keywords'] = get_list(table, 'keyword');
            data['headings'] = get_list(table, 'heading');
            data['citations'] = get_list(table, 'citation');
            console.log(JSON.stringify(data, null, 4));
            var width = 0;
            var header_rows = [];
            var data_rows = [];
            for (var key in table) {
                if (key.search("\\.") != -1) {
                    var info = key.split(".");
                    var idx = parseInt(key.split("_").slice(-1));
                    var row = data_rows;
                    var rid = 0, cid = 0;
                    if (info[0].search("header_row") != -1) {
                        rid = parseInt(info[0].slice(11));
                        cid = idx;
                        row = header_rows;
                    } else if (info[0].search("data_row") != -1) {
                        rid = parseInt(info[0].slice(9));
                        cid = idx;
                        row = data_rows;
                    } else if (info[0].search("row_head") != -1) {
                        if (info[1].search("header") != -1) {
                            row = header_rows;
                        } else {
                            row = data_rows;
                        }
                        rid = idx;
                        cid = 0;
                    } else if (info[0].search("col_") != -1) {
                        if (info[1].search("header") != -1) {
                            row = header_rows;
                        } else {
                            row = data_rows;
                        }
                        rid = idx;
                        cid = parseInt(info[0].slice(4));
                        if (cid > width) {
                            width = cid;
                        }
                    }
                    if (row[rid] == undefined) {
                        row[rid] = [];
                    }
                    row[rid][cid] = table[key];
                }
            }
            var empty_cols = [];
            for (var col = 0; col < width; col++) {
                var empty = true;
                for (var row = 0; row < header_rows.length; row++) {
                    var val = header_rows[row][col];
                    if (!(val == undefined || val == null)) {
                        empty = false;
                    }
                }
                for (var row = 0; row < data_rows.length; row++) {
                    var val = data_rows[row][col];
                    if (!(val == undefined || val == null)) {
                        empty = false;
                    }
                }
                if (empty) {
                    empty_cols.push(col);
                }
            }
            for (var i = empty_cols.length-1; i >= 0; i--) {
                var col = empty_cols[i];
                for (var j = 0; j < header_rows.length; j++) {
                    header_rows[j].splice(col, 1);
                }
                for (var j = 0; j < data_rows.length; j++) {
                    data_rows[j].splice(col, 1);
                }
            }
            data["data_rows"] = data_rows;
            data["header_rows"] = header_rows;
            ret.push(data);
        }
        return ret;
    };
    return {
        "render": render
    };
}]);

/* Controller
 *
 * On change in search box, search() will be called, and results are bind to scope as results[]
 *
*/
Calaca.controller('calacaCtrl', ['calacaService', 'renderer', '$scope', '$location', function(results, renderer, $scope, $location){

        //Init empty array
        $scope.results = [];
        $scope.doctypelist = ["table", "row", "column"];
        $scope.type = $scope.doctypelist[0];

        //On search, reinitialize array, then perform search and load results
        $scope.search = function(){
            $scope.results = [];
            $location.search({'q': $scope.query});
            $scope.loadResults();
        };

        //Load search results into array
        $scope.loadResults = function() {
            results.search($scope.query, $scope.type)
            .then(function(results) {
                $scope.results = renderer.render(results);
            });
        };

    }]
);
