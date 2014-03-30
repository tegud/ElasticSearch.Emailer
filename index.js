var fs = require('fs');
var _ = require('lodash');
var async = require('async');
var elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({
	host: '10.44.35.21:9200'
});

client.search({
	index: 'logstash-2014.03.28',
	body: {
	    "aggregations": {
	        "HotelID": {
	            "terms": {
	                "field": "@fields.HotelID.raw",
	                "size": 1000000000
	            },
	            "aggs": {
	                "ProductID": {
	                    "terms": {
	                        "field": "@fields.ProductID.raw",
	                        "size": 100000000
	                    }
	                }
	            }
	        }
	    }
}
}).then(function (body) {
	async.map(body.aggregations.HotelID.buckets, 
		function(hotel, callback) {
			return async.map(hotel.ProductID.buckets, function(product, callback) {
				callback(null, {
					hotel: hotel.key,
					product: product.key,
					instances: product.doc_count
				});
			}, callback);
		},
		function(err, data) {
			console.log(JSON.stringify(data, null, 4));
			fs.writeFile(__dirname + '/test.json', JSON.stringify(body, null, 4), function (err) {
				console.log('Output written to test.json');
				process.exit(0);
			});
		});
}, function (error) {
	console.trace(error.message);
});