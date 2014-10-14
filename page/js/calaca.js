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
var typeFields = {
    "table" : ["caption", "footnote", "header_row*", "data_row_*.value_0"],
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

Calaca.factory('renderer', [function() {
    var render = function(results) {
        var ret = [];
        var ii = 0;
        for(;ii < results.length; ii++){
            var data = {};
            var table = results[ii];
            if ("caption" in table) {
                data["caption"] = table["caption"];
            }
            var width = 0;
            var header_rows = [];
            var data_rows = [];
            for (var key in table) {
                if (key.search("\\.") != -1) {
                    var info = key.split(".");
                    var idx = parseInt(key.split("_").slice(-1));
                    var row = data_rows;
                    var rid = 0, cid = 0;
                    if (idx > width) {
                        width = idx;
                    }
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
                        cid = 1;
                    }
                    if (row[rid] == undefined) {
                        row[rid] = [];
                    }
                    row[rid][cid] = table[key];
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
