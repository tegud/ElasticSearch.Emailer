var fs = require('fs');
var async = require('async');
var json2csv = require('json2csv');

module.exports = function(folder, name, data, callback) {
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
			var filePath = folder + name + '.csv';

			console.log('Saving file: ' + filePath);

			fs.writeFile(filePath, csv, callback);
		});
	});
};