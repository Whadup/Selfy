var http = require('http');
var url = require('url');
var fs = require("fs");
var path = require("path");
var querystring = require('querystring');
//var requestData = require("request");
var net = require('net');

String.prototype.endsWith = function(suffix) {
    return this.indexOf(suffix, this.length - suffix.length) !== -1;
};

dataDir ="/Users/lukas/Desktop/Desktop/selfies/";

writeHeader = function(res)
{
	res.write("<html><head>\r\n<script src=\"http:\/\/openlayers.org\/api\/OpenLayers.js\"><\/script>\r\n<script src=\"http:\/\/ajax.googleapis.com\/ajax\/libs\/jquery\/1.11.0\/jquery.min.js\"><\/script>\r\n<style>\r\n\tbody\r\n\t{\r\n\t\tfont-family: \'Helvetica\', Verdana, sans-serif;\r\n\t\tfont-weight: 300;\r\n\t\tfont-size: 13pt;\r\n\t\tbackground-color:white;\r\n\t}\r\n\timg\r\n\t{\r\n\t\tmargin:10px;\r\n\t\tborder-radius: 4px;\r\n\t\tborder:0px solid black;\r\n\t\ttext-align: center;\r\n\t}\r\n\tdiv.info\r\n\t{\t\r\n    position: absolute;\r\n    width: 100%;\r\n    padding: 10px;\r\n    margin: 0px;\r\n    background-color: gray;\r\n    text-align: left;\r\n    box-sizing:border-box;\r\n\r\n\t}\r\n\tdiv.mapwrapper\r\n\t{\r\n\t\twidth:90%;\r\n\t\theight:90%;\r\n\t\tmargin: 0px auto;\r\n\t\tbox-sizing:border-box;\r\n\t}\r\n\tdiv.map\r\n\t{\r\n\t\twidth:100%;\r\n\t\theight:100%;\r\n\t}\r\n<\/style>\r\n\r\n<script type=\"text\/javascript\">\r\n\tfunction showMoreInfo(i)\r\n\t{\r\n\t\t$(\".info\").remove();\r\n\t\t$(\"<div class=\\\"info\\\"><\/div>\").insertAfter(\"#\"+i);\r\n\t\t$.getJSON( i+\".json\", function(data)\r\n\t\t{\r\n\t\t\t$(\"<img src=\\\"\"+data.images.low_resolution.url+\"\\\" style=\\\"float:left;margin-right: 20px;\\\" \/>\").appendTo(\".info\");\r\n\t\t\t$(\"<p>\"+data.caption.text+\"<\/p>\").appendTo(\".info\");\r\n\t\t\t$.each(data.comments.data,function(i,item){\r\n\t\t\t\t$(\"<p style=\\\"margin-left:350px;font-size:12pt\\\">\"+item.text+\"<\/p>\").appendTo(\".info\");\r\n\t\t\t})\r\n\t\t\t$(\"<p><a href=\\\"\"+data.link+\"\\\">Show on Instagram<\/a><\/p>\").appendTo(\".info\");\r\n\t\t\tvar rowpos = $(\'.info\').position();\r\n\t\t\t\/\/$(\"body\").scrollTop(rowpos.top);\r\n\r\n\t\t});\r\n\t}\r\n\r\n\r\n<\/script>\r\n\r\n<\/head><body style=\"\r\n    width: 100%;\r\n    padding: 0px;\r\n    margin: 0px;\r\n\">\r\n\t<h1 style=\"\r\n    FONT-FAMILY: Helvetica;\r\n    font-size: 160px;\r\n    padding-left: 25%;\r\n    background-color: gray;\r\n    width: 100%;\r\n    vertical-align: bottom;\r\n    height: 147px;\r\n    \/* line-height: 0px; *\/\r\n    color: white;\r\n    box-sizing:border-box;\r\n\">Self<span style=\"color: rgb(241, 213, 37);\">y<\/span><\/h1>\r\n\t<form method=\"POST\" style=\"\r\n    width: 550px;\r\n    margin: 100px auto 50px auto;\r\n    padding: 5px;\r\n    text-align:center;\r\n\">\r\n\t\t<input name =\"query\" type=\"text\" style=\"\r\n    width: 75%;\r\n    height: 30px;\r\n    display: block;\r\n    float: left;\r\n    \r\n    \r\n    \r\n\">\r\n\t\t<button type=\"submit\" style=\"\r\n    width: 24%;\r\n    height: 30px;\r\n    \/* margin: 0px; *\/\r\n    margin-left: 5px;\r\n    display: inline;\r\n\">Search!<\/button>\r\n\t<input style=\"\r\n    padding-left: 50px;\r\n    height: 25px;\r\n    line-height: 25px;\r\n    vertical-align: sub;\r\n\" type=\"radio\" name=\"engine\" value=\"lucene\" checked>Lucene\r\n\t<input style=\"\r\n    padding-left: 50px;\r\n    height: 25px;\r\n    line-height: 25px;\r\n    vertical-align: sub;\r\n\" type=\"radio\" name=\"engine\" value=\"hadoop\">Hadoop\r\n\t<input style=\"\r\n    padding-left: 50px;\r\n    height: 25px;\r\n    line-height: 25px;\r\n    vertical-align: sub;\r\n\"  type=\"radio\" name=\"engine\" value=\"geo\">Geospatial\r\n\t<\/form>\r\n\t<!--<div class=\"mapwrapper\">\r\n\t<div class=\"map\" id = \"map\">\r\n\t<\/div>\r\n\t<\/div>\r\n\t<script defer=\"defer\" type=\"text\/javascript\">\r\n        var map = new OpenLayers.Map(\'map\');\r\n        var wms = new OpenLayers.Layer.WMS( \"OpenLayers WMS\",\r\n            \"http:\/\/vmap0.tiles.osgeo.org\/wms\/vmap0\", {layers: \'basic\'} );\r\n        map.addLayer(wms);\r\n        map.zoomToMaxExtent();\r\n      <\/script>-->\r\n\r\n    <script>\r\n    \t$(\'body\').keyup(function(e){\r\n\t\t\tif(e.which == 27){\r\n\t\t\t\t$(\".info\").remove();\r\n        \r\n\t\t\t}\r\n\t\t});\r\n    <\/script>\r\n\t<div class=\"results\" style=\"\r\n    text-align: center;\r\n\">\r\n\t\t<div class=\"info\" style=\"display:none;\" > <\/div>");
}
http.globalAgent.maxSockets = 200;

server = http.createServer(function (request, res) 
{
	
	if(request.method == "GET")
	{
		console.log(request.url);
		if(request.url.endsWith(".jpg"))
		{
			res.writeHead(200, {'Content-Type': 'image/jpeg', 'Connection' : 'Keep-Alive'});	
			id = request.url.substring(1);
			prefix = id.substring(0,5);
			fs.readFile(dataDir+prefix+"/"+request.url, function (err, data) {
  				if (err) throw err;
  				
  				a = res.write(data);
  				res.end();
  				
			});
			res.once("finish",function(){res.end();});
			res.once("drain",function(){res.end();});
		}
		else if(request.url.endsWith(".json"))
		{
			res.writeHead(200, {'Content-Type': 'text/plain', 'Connection' : 'Keep-Alive'});	
			id = request.url.substring(1);
			prefix = id.substring(0,5);
			fs.readFile(dataDir+prefix+"/"+request.url, function (err, data) {
  				if (err) throw err;
  				
  				a = res.write(data);
  				res.end();
  				
			});
			res.once("finish",function(){res.end();});
			res.once("drain",function(){res.end();});
		}
		else
		{
			res.writeHead(200, {'Content-Type': 'text/html', 'Connection' : 'Keep-Alive'});
			writeHeader(res);
			res.end("</div></body></html>");
		}
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
			console.log(fullbody);
			parameters = querystring.parse(fullbody); 
			console.log(parameters);

			var connection = net.createConnection({'port':8999});
			connection.write(parameters.engine.substring(0,1)+parameters.query+"\n");

			fullbody = "";
			connection.setEncoding('utf8');
			connection.on('data',function(chunk){fullbody+=chunk;});
			connection.on('end',function(){
				res.writeHead(200, {'Content-Type': 'text/html'});
				
				writeHeader(res);

				
				selfies = JSON.parse(fullbody);
				
				for(key in selfies.data)
				{
					id = selfies.data[key].id;
					res.write("<img id=\""+id+"\" onclick=\"showMoreInfo('"+id+"')\" src=\""+id+".jpg\" />");
				}
				res.end('</div></body></html>');
			});

		});
	}
	
});
server.on("connect",function (request, socket, head){
	console.log("new connection");
	request.setSocketKeepAlive(true);
});
server.listen(8080, "127.0.0.1");



console.log('Server running at http://127.0.0.1:8080/');
