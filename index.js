var json2csv = require('json2csv');
var fs = require('fs');
var _ = require('lodash');
var async = require('async');
var elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({
	host: '10.44.35.21:9200'
});

var aggregateSearch = {
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
};

function parseProducts(products, callback) {
	async.map(products, function(product, callback) {
		callback(null, {
			id: product.key,
			instances: product.doc_count
		});
	}, callback);	
}

function parseHotels(hotels, callback) {
	var productCount = 0;

	async.map(hotels, 
		function(hotel, callback) {
			productCount += hotel.ProductID.buckets.length;

			parseProducts(hotel.ProductID.buckets, function(err, missingProducts) {
				callback(err, {
					id: hotel.key,
					products: missingProducts
				})
			});
		},
		function(err, data) {
			callback(err, {
				data: data,
				hotelCount: hotels.length,
				productCount: productCount
			});
		});
}

var fileWriters = {
	json: function(data, callback) {
		fs.writeFile(__dirname + '/output/test.json', JSON.stringify(data, null, 4), callback);
	},
	csv: function(data, callback) {
		var rows = [];

		async.each(data, function(hotel, callback) {
			async.each(hotel.products, function(product, callback) {
				rows.push({
					hotel: hotel.id,
					product: product.id,
					instances: product.instances
				})
				callback();
			}, callback);
		}, function() {
			json2csv({data: rows, fields: ['hotel', 'product', 'instances']}, function(err, csv) {
				fs.writeFile(__dirname + '/output/test.csv', csv, callback);
			});
		});
	}
};

client
	.search(aggregateSearch)
	.then(function (body) {
		parseHotels(body.aggregations.HotelID.buckets, function(err, data) {
			fileWriters['csv'](data.data, function (err) {
				console.log(data.hotelCount + ' unique missing hotels found');				
				console.log(data.productCount + ' unique missing products found');				

				console.log('Output written to test.json');

				process.exit(0);
			});
		});
	}, 
	function (error) {
		console.trace(error.message);
		process.exit(0);
	});