/**
 * Created by Gavin on 5/26/15.
 */
require('http').globalAgent.maxSockets = Infinity;

var elasticsearch = require('elasticsearch');
var config = require('./config');
var crypto = require('crypto');
var scheduler = require('./scheduler');
var directoryClient = require('./directoryClient');


var $ = {};

$.sessionTimeout = 30000;

$.mappings = {};

$.esClient = new elasticsearch.Client({
    host: config.elastichost,
    maxSockets: Infinity
});

$.getId = function(domain){
    var id = domain + parseInt(Math.random()*10000000).toString() + Date.now();
    var hash = crypto.createHash('md5');
    return hash.update(id).digest('hex');
};

$.getSessionId = function(){
    return parseInt((Math.random() * 1000)).toString() + Date.now().toString();
}

$.INDEX = 'sessionlog';

$.initialize = function(callback){
    $.esClient.indices.getMapping({index: $.INDEX},function(err,ret){
        if(!err){
            if(ret[$.INDEX] && ret[$.INDEX]['mappings']){
                $.mappings = ret[$.INDEX]['mappings'];
            }
            //initialize scheduler
            directoryClient.getService('session',function(err,ret){
                if(ret){
                    scheduler.initialize(ret.mq_channel,config.privateChannel,callback);
                }
                else{
                    scheduler.initialize(config.publicChannel,config.privateChannel,callback);
                }
            })

        }
        else{
            callback(err,null);
        }
    });
};

$.addMappingForDomain = function(domain,callback){
    $.esClient.indices.putMapping(
        {
            index:$.INDEX,
            type:domain,
            ignoreConflicts: true,
            body: config.mapping

        },
        function(err,ret){
            if(err){
                $.esClient.indices.create({index: $.INDEX},function(err,ret){
                    if(!err){
                        $.esClient.indices.putMapping(
                            {
                                index:$.INDEX,
                                type:domain,
                                ignoreConflicts: true,
                                body: config.listMapping

                            },
                            function(err,ret){
                                if(!err){
                                    callback(null,ret);
                                }
                                else{
                                    callback(err,null);
                                }
                            }
                        );
                    }
                    else{
                        callback(err,null);
                    }
                })
            }
            else{
                callback(null,ret);
            }

        }
    );
};

$.getSessionData = function(domain, userid, sessionid, callback){
    //process.nextTick(function(){

        if(!sessionid){
            //create sessionid and capture info anyway

            sessionid = $.getSessionId();

            $.addLogItem(domain, {sessionid: sessionid, userid: userid, event: 'begin', data: 'new session'}, function(err,ret){

                if(!err){
                    var sessionData = ret;

                    scheduler.addEvent(domain,sessionid,Date.now() + $.sessionTimeout,function(err,ret){
                        console.log(sessionData);
                        callback(null,sessionData);
                    });
                }
                else{
                    callback(err,null);
                }
            })

        }
        else{
            $.esClient.search({
                index: $.INDEX,
                type: domain,
                body:{
                    query:
                    {
                        filtered:{
                            filter:{
                                bool:{
                                    must:  [
                                        {term:{sessionid: sessionid}},
                                        {term:{event: 'begin'}},
                                    ],
                                    must_not:{
                                        term: {expired: true}
                                    }
                                }
                            }
                        }
                    }
                }
            },function(err,ret){
                if(!err){
                    if(ret.hits.total>0){
                        callback(null,ret.hits.hits[0]._source);
                    }
                    else{
                        callback(null,null);
                    }
                }
                else{
                    callback(err,null);
                }
            });
        }

    //});
};

$.addLogItem = function(domain, params, callback){
    //console.log(params);
    var sessionid = params.sessionid || null;
    var userid = params.userid || null;
    var domain = domain;
    var event = params.event
    var data = params.data;
    var timestamp = Date.now();

    if(!$.mappings[domain]){
        $.addMappingForDomain(domain,
            function(err,ret){
                if(!err){
                    $.initialize(function(err,ret){
                        if(!err){
                            $.addLogItem(domain, {userid: userid, sessionid: sessionid}, callback);
                        }
                        else{
                            callback(err,null);
                        }
                    })
                }
                else{
                    callback(err,null);
                }
            }
        );
    }
    else {

        var id = $.getId(domain);
        $.esClient.create({
            index: $.INDEX,
            type: domain,
            id: id,
            body: {
                userid: userid,
                sessionid: sessionid,
                event: event,
                data: data,
                timestamp: timestamp
            }

        }, function(err,ret){
            if(!err){

                scheduler.touch(domain,sessionid,Date.now() + $.sessionTimeout,function(err,ret){

                    if(callback) {
                        console.log(sessionid);
                        var retval = {sessionid: sessionid, id: id, userid: userid, timestamp: timestamp};
                        callback(null,retval);
                    }
                });

            }
            else{
                if(callback) {

                    callback(err, null);
                }
            }
        });


    }

};


$.getLogItems = function(domain, params, callback){
    var domain = domain;
    var sessionid = params.sessionid || null;
    if(sessionid){
        sessionid = sessionid.replace(domain + '.','');
    }
    var userid = params.userid || null;
    var event = params.event || null;
    var events = params.events || null;

    var terms = [];
    if(sessionid){
        terms.push({term:{sessionid:sessionid}});
    }
    if(userid){
        terms.push({term:{userid:userid}});
    }
    if(event){
        terms.push({term:{event:event}});
    }
    if(events){
        terms.push({terms:{event:events}});
    }

    $.getSessionData(domain,userid,sessionid, function(err,ret){
        if(!err){
            var sessionData = ret;
            delete sessionData.event;
            delete sessionData.data;
            delete sessionData.timestamp;

            sessionData.domain = domain;

            $.esClient.search({
                index: $.INDEX,
                type: domain,
                body:{
                    query:{
                        filtered:{
                            filter:{
                                and: terms
                            }
                        }
                    }
                }
            }, function(err,ret){
                if(!err){
                    var ar = [];
                    if(ret.hits.total > 0){

                        for(var i in ret.hits.hits){
                            ar.push(ret.hits.hits[i]._source);
                        }

                    }
                    sessionData.logs = ar.sort(
                        function(a,b){
                            return a.timestamp - b.timestamp
                        }
                    );
                    if(events){
                        sessionData.events = events;
                    }
                    if(event){
                        sessionData.event = event;
                    }
                    callback(null,sessionData);
                }
                else{
                    callback(err,null);
                }
            });
        }
        else{
            callback(err,null);
        }
    })



};

$.initialize(function(err,ret){
   console.log('initialized');
});

module.exports = $;