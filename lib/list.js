/**
 * Created by Gavin on 5/16/15.
 */
require('http').globalAgent.maxSockets = Infinity;

var cache = require('./cache');
var elasticsearch = require('elasticsearch');
var config = require('./config');
var crypto = require('crypto');


//list.esClient.indices.putMapping({index:'userlist',type:'test',ignoreConflicts: true,body:{ properties: {itemid: { type: 'string', index: 'not_analyzed'}}}},function(err,ret){console.log(err);console.log(ret);});

var doCallback = function(err,ret,callback){
    if(callback){
        if(err){
            callback(err,null);
        }
        else{
            callback(null,ret);
        }
    }
};

var $ = {};

$.mappings = {};

$.esClient = new elasticsearch.Client({
    host: config.elastichost,
    maxSockets: Infinity
});

$.INDEX = 'userlist';

$.getId = function(domain, listname, userid, itemid){
    var id = domain + '.' + listname + '.' + userid + '.' + itemid;
    var hash = crypto.createHash('md5');
    return hash.update(id).digest('hex');
};

$.initialize = function(callback){
    $.esClient.indices.getMapping({index: $.INDEX},function(err,ret){
        if(!err){
            if(ret[$.INDEX] && ret[$.INDEX]['mappings']){
                $.mappings = ret[$.INDEX]['mappings'];
            }
            callback(null,true);
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
            body: config.listMapping

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

$.get = function(domain, listname, userid, params, callback){
    var andArray = [
        {
            term:
            {
                userid: userid
            }
        },
        {
            term:
            {
                listname: listname
            }
        }
    ];

    if(typeof(params)==='function'){
        callback = params;
    }
    else{
        for(var i in params){
            var term = {};
            term[i] = params[i];
            andArray.push({term: term});
        }

    }
    $.esClient.search(
        {
            index: $.INDEX,
            type: domain,
            body:
            {
                query:
                {
                    filtered:
                    {
                        filter:
                        {
                            and: andArray

                        }

                    }
                }
            }
        },
        function(err,ret){
            if(!err){
                var results = [];
                if(ret.hits.total > 0){
                    for(var i in ret.hits.hits){
                        results.push(ret.hits.hits[i]._source);
                    }
                }
                callback(null,results);
            }
            else{
                callback(err,null);
            }
        }
    );
};

$.getByItemId = function(domain, listname, itemids, callback){

    var itm = {};
    if(typeof(itemids)==='string'){
        itm = {
            term: {
                itemid: itemids
            }
        };
    }
    else{
        itm = {
            terms: {
                itemid: itemids
            }
        };
    }

    console.log(itm);

    $.esClient.search(
        {
            index: $.INDEX,
            type: domain,
            body:
            {
                query:
                {
                    filtered:
                    {
                        filter:
                        {
                            and:
                                [
                                    itm,
                                    {
                                        term:
                                        {
                                            listname: listname
                                        }
                                    }
                                ]

                        }

                    }
                }
            }
        },
        function(err,ret){
            if(!err){
                var results = [];
                if(ret.hits.total > 0){
                    for(var i in ret.hits.hits){
                        results.push(ret.hits.hits[i]._source);
                    }
                }
                callback(null,results);
            }
            else{
                callback(err,null);
            }
        }
    );
};

$.exists = function(domain,listname,userid,itemids, callback){

    var itm = {};
    if(typeof(itemids)==='string'){
        itm = {
            term: {
                itemid: itemids
            }
        };
    }
    else{
        itm = {
            terms: {
                itemid: itemids
            }
        };
    }

    console.log(itm);

    $.esClient.search(
        {
            index: $.INDEX,
            type: domain,
            body:
            {
                query:
                {
                    filtered:
                    {
                        filter:
                        {
                            and:
                                [
                                    itm,
                                    {
                                        term:
                                        {
                                            listname: listname
                                        }
                                    },
                                    {
                                        term: {
                                            userid: userid
                                        }
                                    }
                                ]

                        }

                    }
                }
            }
        },
        function(err,ret){
            if(!err){
                var results = [];
                if(ret.hits.total > 0){
                    for(var i in ret.hits.hits){
                        results.push(ret.hits.hits[0]._source);
                    }
                }
                callback(null,results);
            }
            else{
                callback(err,null);
            }
        }
    );
};

$.add = function(domain, listname, userid, itemid, metadata, callback) {
    if(!$.mappings[domain]){
        $.addMappingForDomain(domain,
            function(err,ret){
                if(!err){
                    $.initialize(function(err,ret){
                        if(!err){
                            $.add(domain, listname, userid, itemid, metadata, callback);
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
        var body = {
            userid: userid,
            listname: listname,
            itemid: itemid
        };

        if (metadata) {
            body.metadata = metadata;
        }
        $.esClient.create(
            {
                index: $.INDEX,
                type: domain,
                id: $.getId(domain,listname,userid,itemid),
                body: body
            },
            function (err, ret) {
                doCallback(err, ret, callback);
            }
        );
    }
};

$.del = function(domain, userid, listname, itemid, callback){
    var id = $.getId(domain,listname,userid,itemid);
    $.esClient.delete({index: $.INDEX, type: domain, id: id},function(err,ret){
        if(!err){
            callback(null,true);
        }
        else{
            callback(err,null);
        }
    });
};

$.getSimilar = function(domain, listname, userid, callback){
    //get list for userid
    $.get(domain, listname, userid, function(err,ret){
         if(!err){
             //create list of itemids
             var itemids = [];
             for(var i in ret){
                 itemids.push(ret[i].itemid);
             }

             //get aggregated list of userids who have these items - aggregate by userid

         }
         else{
             callback(err,ret);
         }
    });
    //aggregate others with similar
}

$.initialize(function(err,ret){
    console.log('initialized');
});

module.exports = $;