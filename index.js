var fs = require('fs');
var _ = require('lodash');
var async = require('async');
var elasticsearch = require('elasticsearch');
var moment = require('moment');

var fileWriters = require('./lib/fileWriters');
var missingRooms = require('./lib/missingRooms');

fs.readFile(__dirname + '/config.json', { encoding: 'utf-8' }, function(err, fileContents) {
	var config = JSON.parse(fileContents);

	var client = new elasticsearch.Client({
		host: config.host
	});
	var date = config.date;
	var todaysDate = moment();

	if(date === 'today') {
		date = todaysDate.format('YYYY.MM.DD');
	} else if (date === 'yesterday') {
		date = todaysDate.subtract('days', 1).format('YYYY.MM.DD');
	}

	missingRooms.buildSearch(date, function(err, aggregateSearch) {
		client
			.search(aggregateSearch)
			.then(function (body) {
				missingRooms.parseSearch(body, function(err, data) {
					console.log(data.hotelCount + ' unique missing hotels found');				
					console.log(data.productCount + ' unique missing products found');

					async.each([fileWriters['json'], fileWriters['csv']], function(writer, callback) {
						writer(__dirname + '/output/', 'missingRooms-' + date, data.data, callback);
					}, function(err) {
						if(err) {
							console.log('Errors Occurred');
							console.log(err);
							console.log('Exiting...')
						}
						else {
							console.log('Job complete, exiting...');
						}
						process.exit(0);
					});
				});
			}, 
			function (error) {
				console.trace(error.message);
				process.exit(0);
			});	
	});
});
