var fs = require('fs');

module.exports = function(folder, name, data, callback) {
	var filePath = folder + name + '.json';

	console.log('Saving file: ' + filePath);

	fs.writeFile(filePath, JSON.stringify(data, null, 4), callback);
};