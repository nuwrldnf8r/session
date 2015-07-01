var config = require('./config');
var conhash = require('consistent-hashing');
var redis = require("redis");

var TIMEOUT = 60*24;

var $ = {};

$.nodes = {};
$.cons = new conhash([]);

/*
function guid(){
	return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    	var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
    	return v.toString(16);
	});
}
*/

$.addConnection = function(conf){
	var nd = conf.name;
	$.nodes[nd] = redis.createClient(conf.port,conf.host);
	$.nodes[nd].nodeName = nd;
	
	//add events
	$.nodes[nd].on("error", function (err) {
        console.log(err);
	});
	
	$.nodes[nd].on("end", function (err,ret) {
        console.log(this.nodeName + ' disconnected');
        $.removeNode(this.nodeName);
	});
	
	$.nodes[nd].on("ready", function (err,ret) {
       if($.nodes[this.nodeName]){
        	$.addNode(this.nodeName);
        	console.log(this.nodeName + ' now online');
    	}
    	else{
    		$.addNode(this.nodeName,this);
    	}
       
	});
};

$.getNode = function(key){
	return $.nodes[$.cons.getNode(key)];
};

$.removeNode = function(nodeName){
	$.cons.removeNode(nodeName);
	//delete $.nodes[nodeName].nodeName;
	delete $.nodes[nodeName];
};

$.addNode = function(nodeName,node){
	if(node){
		node.nodeName = nodeName;
		$.nodes[nodeName] = node;
		$.cons.addNode(nodeName);
		console.log(nodeName + ' now online');
	}
	else{
		$.cons.addNode(nodeName);
	}
};

$.set = function(key,value,callback){
	//console.log(key);
    $.getNode(key).set(key,value,callback);
};


$.get = function(key,callback){
	//console.log('getting');
	$.getNode(key).get(key,callback);
};

$.exists = function(key,callback){
	$.getNode(key).exists(key,callback);
};

$.sadd = function(key,value,callback){
	$.getNode(key).sadd(key,value,function(err,ret){
		if(callback){
			callback(err,ret);
		}
	});
};

$.smembers = function(key,callback){
	$.getNode(key).smembers(key,callback);
};

$.sismember = function(key,value,callback){
	$.getNode(key).sismember(key,value,callback);
};

$.srem = function(key,value,callback){
	$.getNode(key).srem(key,value,function(err,ret){
		if(callback){
			callback(err,ret);
		}
	});
};

$.spop = function(key,callback){
	$.getNode(key).spop(key,function(err,ret){
		if(ret){
			if(callback){
				callback(null,ret);
			}
		}
		else{
			if(callback){
				callback(null,null);
			}
		}
	})
};

$.zadd = function(key,score,value,callback){
	//console.log(key);
	$.getNode(key).zadd(key,score,value,function(err,ret){
		if(callback){
			callback(err,ret);
		}
	});
};

$.zcount = function(key,from,to,callback){
	$.getNode(key).zcount(key,from,to,callback);
};

$.zrange = function(key,from,to,callback){
	$.getNode(key).zrangebyscore(key,from,to,callback);
};

$.zremrangebyscore = function(key,from,to,callback){
	$.getNode(key).zremrangebyscore(key,from,to,function(err,ret){
		if(callback){
			callback(err,ret);
		}
	});
};

$.rpush = function(key,value,callback){
	$.getNode(key).rpush(key,value,function(err,ret){
		if(callback){
			callback(err,ret);
		}
	});
};

$.lpop = function(key,callback){

	$.getNode(key).lpop(key,function(err,ret){
		if(callback){
			callback(err,ret);
		}
	});
};

$.srem = function(key,value,callback){
	$.getNode(key).srem(key,value,function(err,ret){
		if(callback){
			callback(err,ret);
		}
	});
};

$.spop = function(key,callback){
	$.getNode(key).spop(key,function(err,ret){
		if(ret){
			if(callback){
				callback(null,ret);
			}
		}
		else{
			if(callback){
				callback(null,null);
			}
		}
	})
};

$.addToList = function(key,value,trim,callback){
	
	$.getNode(key).lpush(key,value,function(err,ret){
		if(trim>-1){
			//console.log(key);
			//console.log(trim);
			$.getNode(key).ltrim(key,0,trim,function(err,ret){});
		}
		if(callback){
			callback(err,ret);
		}
	});
};

$.getList = function(key,callback){
	try{
		$.getNode(key).lrange( key, 0,-1,function(err,ret){
			callback(err,ret);
		});
	}
	catch(e){
		callback(e,null);
	}
};

$.removeFromList = function(key,value,nm,callback){
	if(!nm){
		nm = 0;
	}
	$.getNode(key).lrem(key,nm,value,function(err,ret){
		if(callback){
			callback(err,ret);
		}
	});
};

$.mget = function(keys,callback){
	//break keys up into nodes
	var nodes = {};
	keys.forEach(function(key){
		var node = $.getNode(key);
		if(!nodes[node.nodeName]){
			nodes[node.nodeName] = {node:node,keys:[]};
		}
		nodes[node.nodeName].keys.push(key);
	});
	var rslts = [];
	var nodeCount = Object.keys(nodes).length;
	if(nodeCount>0){
	
		//to do: set timeout in case a node is unreachable
		for(var n in nodes){
			nodes[n].node.mget(nodes[n].keys,function(err,ret){
				if(err){
					nodeCount = nodeCount - 1;
					callback(err,null);
				}
				else{
					rslts = rslts.concat(ret);
					nodeCount = nodeCount - 1;
	
				}
				if(nodeCount === 0){
					callback(null,rslts);
				}
			});
		}
	}
	else{
		callback(null,null);
	}
};


$.del = function(key,callback){
	$.getNode(key).del(key,function(err,ret){
		if(callback)callback(err,ret);
	});
	
};

$.incr = function(key,callback){
	$.getNode(key).incr(key,callback);
};

$.setTimeout = function(key,timeout){
	var ttl;
	if(timeout)ttl = timeout;
	else ttl = TIMEOUT;
	$.getNode(key).expire(key,ttl);
};

config.redis.forEach(function(conf){
	$.addConnection(conf);
	console.log(conf);
});

module.exports = $;