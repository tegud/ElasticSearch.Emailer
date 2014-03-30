var fs = require('fs');
var _ = require('lodash');
var async = require('async');
var elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({
	host: '10.44.35.21:9200'
});

function parseHotels(hotels, callback) {
	var hotelCount = 0;
	var productCount = 0;

	async.map(hotels, 
		function(hotel, callback) {
			hotelCount++;

			return async.map(hotel.ProductID.buckets, function(product, callback) {
				productCount++;

				callback(null, {
					hotel: hotel.key,
					product: product.key,
					instances: product.doc_count
				});
			}, callback);
		},
		function(err, data) {
			callback(err, {
				data: data,
				hotelCount: hotelCount,
				productCount: productCount
			});
		});
}

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
	var hotelCount = 0;
	var productCount = 0;

	parseHotels(body.aggregations.HotelID.buckets, function(err, data) {
		fs.writeFile(__dirname + '/test.json', JSON.stringify(data.data, null, 4), function (err) {
			console.log(data.hotelCount + ' unique missing hotels found');				
			console.log(data.productCount + ' unique missing products found');				

			console.log('Output written to test.json');

			process.exit(0);
		});
	});
}, function (error) {
	console.trace(error.message);
	process.exit(0);
});