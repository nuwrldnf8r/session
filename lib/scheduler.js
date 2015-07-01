/**
 * Created by Gavin on 5/27/15.
 */
var cache = require('./cache');
var mq = require('./mq_event');


var $ = {};

var keycount = function(ob){
    var cnt = 0;
    for(var i in ob){
        cnt++;
    }
    return cnt;
}

$.sessions = {};

$.timer = null;

$.timeCheck = 10000;

$.syncing = false;

$.timeout = function(){
    console.log('processing timeout');
    var now = Date.now();
    for(var i in $.sessions){
        var timeout = $.sessions[i];
        if(timeout<=now){
            initiateTimeout(i);
        }
    }
    if(keycount($.sessions)===0){
        $.localPub.publish({event: 'hello', node: $.me}, function(err,ret){});
    }
    $.timer = setTimeout($.timeout, $.timeCheck);
}

var sendSync = function(node){

    process.nextTick(function(){
        for(var i in $.sessions){
            $.localPub.publish({event: 'sync', node: node, id: i, data: $.sessions[i]}, function(err,ret){});
        }
        $.localPub.publish({event: 'done_syncing', id: i, data: $.sessions[i]}, function(err,ret){});
    })
}

var internalListener = function(data){
    process.nextTick(function() {
        if (data.event === 'add') {
            $.sessions[data.id] = data.data;
        }
        else if(data.event === 'update') {
            $.sessions[data.id] = data.data;
        }
        else if(data.event === 'timeout'){
            delete $.sessions[data.id];
        }
        else if(data.event=== 'hello'){
            if(data.node != $.me && !$.syncing){
                setTimeout(function(){
                    if(!$.syncing){
                        $.syncing = true;
                        sendSync(data.node);
                    }
                }, parseInt(Math.random() * 5000));
            }
        }
        else if(data.event === 'sync'){
            if(data.node === $.me){

                $.sessions[data.id] = data.data;
            }
            else{
                $.syncing = true;
            }
        }
        else if(data.event === 'done_syncing'){
            $.syncing = false;
        }
    });
}

var remove = function(domain, id, callback){
    process.nextTick(function() {
        delete $.sessions[domain + '.' + id];
        if(callback) {
            callback(null, true);
        }
    });
};

var initiateTimeout = function(id){
    cache.exists(id,function(err,ret){
        if(ret){
            cache.del(id, function(err,ret){
                $.localPub.publish({event: 'timeout', id: id}, function(err,ret){
                    $.publisher.publish({event: 'timeout', id: id});
                });

            })
        }
    })
}

$.addEvent = function(domain, id, timeout, callback){
    process.nextTick(function(){
        cache.set(domain + '.' + id, timeout.toString(), function(err,ret){
            $.localPub.publish({event: 'add', id: domain + '.' + id, data: timeout}, callback);
        });
    });
};

$.touch = function(domain, id, newTimeout, callback){
    console.log(domain + '.' + id);
    //process.nextTick(function(){
        cache.exists(domain + '.' + id, function(err,ret){
            if(ret){
                cache.set(domain + '.' + id, newTimeout.toString(), function(err,ret){
                    $.localPub.publish({event: 'update', id: domain + '.' + id, data: newTimeout}, callback);
                });
            }
            else{

                remove(domain,id, function(){
                    callback(new Error('item does not exist'), null);
                });
            }
        })
    //});
};

$.initialize = function(publishChannel, localChannel, callback){
    console.log('initializing mq');
    $.me = parseInt(Math.random()*100000).toString();
    $.timer = setTimeout($.timeout, $.timeCheck);
    $.publisher = new mq.publisher(publishChannel);
    $.publisher.initialize(function(err,ret){
        $.localPub = new mq.publisher(localChannel);
        $.localPub.initialize(function(err,ret){
            $.localSub = new mq.listener(localChannel,internalListener);
            $.localSub.initialize(function(err,ret){
                $.localPub.publish({event: 'hello', node: $.me}, callback);
            });
        });
    });
};

$.shutDown = function(callback){
    //close all connections etc.
};



module.exports = $;