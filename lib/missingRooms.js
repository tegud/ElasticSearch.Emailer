var async = require('async');

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

module.exports = {
	buildSearch: function(date, callback) {
		callback(null, {
			index: 'logstash-' + date,
			body: {
			    "aggregations": {
			        "HotelID": {
			            "terms": {
			                "field": "@fields.HotelID.raw",
			                "size": 1000000000
			            },
			            "aggregations": {
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
		});
	},
	parseSearch: function(body, callback) {
		parseHotels(body.aggregations.HotelID.buckets, callback);
	}
};