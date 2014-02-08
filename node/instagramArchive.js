var request = require("request");
var http = require("http");
var fs = require("fs");
api = "https://api.instagram.com/v1/tags/selfie/media/recent?client_id=620c46f7e7da46149b0ad5d3cd8268ba&max_id=122940555115274338_13327215";

callback = function(error, response, body){
	if(!body.data)
		return;
	api = body.pagination.next_url;
	for(key in body.data)
	{
		fs.writeFile("selfies/"+body.data[key].id+".json", JSON.stringify(body.data[key], null, 4), function(err) {
			if(err) 
			{
				console.log(err);
			}
		}); 
		console.dir(body.data[key].images.low_resolution.url);
		request(body.data[key].images.thumbnail.url).pipe(fs.createWriteStream("selfies2/"+body.data[key].id+".jpg"));
	}
	request({url: api, json:1},callback);
};

request({url: api, json:1},callback);

