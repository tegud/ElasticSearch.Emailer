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
	var hits = body.hits.hits;

	console.log(body.aggregations.HotelID.buckets.length + ' unique hotels with missing rooms found');
	var totalUniqueProducts = _.reduce(body.aggregations.HotelID.buckets, function(memo, hotelBucket) {
		return memo + hotelBucket.ProductID.buckets.length;
	}, 0);

	console.log(totalUniqueProducts + ' unique products with missing rooms found');			

	fs.writeFile(__dirname + '/test.json', JSON.stringify(body, null, 4), function (err) {
		console.log('Output written to test.json');
		process.exit(0);
	});

}, function (error) {
	console.trace(error.message);
});