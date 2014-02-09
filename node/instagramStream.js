var http = require('http');
var url = require('url');
var fs = require("fs");
var path = require("path");
var querystring = require('querystring');
var requestData = require("request");

var politeness = 5;

counter = 0;
api = "https://api.instagram.com/v1/tags/selfie/media/recent?client_id=620c46f7e7da46149b0ad5d3cd8268ba";

callback = function(error, response, body){
	if(!body.data)
		return;
	for(key in body.data)
	{
		prefix = body.data[key].id.substring(0,5);
		if(!path.existsSync("selfies/"+prefix))
			fs.mkdirSync("selfies/"+prefix);
		fs.writeFile("selfies/"+prefix+"/"+body.data[key].id+".json", JSON.stringify(body.data[key], null, 4), function(err) {
			if(err) 
			{
				console.log(err);
			}
		}); 
		console.dir(body.data[key].images.low_resolution.url);
		requestData(body.data[key].images.thumbnail.url).pipe(fs.createWriteStream("selfies/"+prefix+"/"+body.data[key].id+".jpg"));
	}
};


http.createServer(function (request, res) 
{
	if(request.method == "GET")
	{
		parameters = url.parse(request.url,true).query;
		console.log(parameters["hub.challenge"]);
		res.writeHead(200, {'Content-Type': 'text/plain'});
		res.end(parameters["hub.challenge"]);
	}
	if(request.method == "POST")
	{
		request.setEncoding('utf8');
		fullbody="";
		request.on('data',function(chunk)
		{
			fullbody+=chunk;
		});
		request.on('end',function(){
			parameters = JSON.parse(fullbody); 
			counter++;
			if(counter == politeness)
			{
				requestData({url: api, json:1},callback);
				counter=0;
			}
		});
	}
	res.writeHead(200, {'Content-Type': 'text/plain'});
	res.end('');
}).listen(1337, "127.0.0.1");



console.log('Server running at http://127.0.0.1:1337/');

//start and stop commands
//curl -F 'client_id=620c46f7e7da46149b0ad5d3cd8268ba' -F 'client_secret=c57beab5c2864e608b0dc4e46e5f8b50' -F 'object=tag' -F 'aspect=media' -F 'object_id=selfie' -F 'callback_url=http://whadup.caelum.uberspace.de/nodejs/' https://api.instagram.com/v1/subscriptions/
//curl -X DELETE 'https://api.instagram.com/v1/subscriptions?client_secret=c57beab5c2864e608b0dc4e46e5f8b50&object=all&client_id=620c46f7e7da46149b0ad5d3cd8268ba'