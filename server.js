/**
 * Created by Gavin on 5/26/15.
 */
var restify = require('restify');
var session = require('./lib/session');
var request = require('request');
var config = require('./lib/config');
var directoryClient = require('./lib/directoryClient');

var server = restify.createServer({
    name: 'SessionServer',
    version: '0.0.1'
});

server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser());

//get sessionid
server.post('/:domain/log', function(req,res,next){

    if(!req.params.sessionid){

        var userid = req.params.userid || null;
        var domain = req.params.domain;

        session.getSessionData(domain,userid,null,function(err,ret){

            if(!err){

                var sessionid = ret.sessionid;
                var event = req.params.event;
                var data = req.params.data;
                session.addLogItem(
                    domain,
                    { userid: userid, sessionid: sessionid, event: event, data: data},
                    function (err, ret) {
                        console.log(ret);
                        res.send(ret);
                    }
                );
            }
            else{

                res.send({error:err});
            }

        });
    }
    else {

        var domain = req.params.domain;
        delete req.params.domain;
        session.addLogItem(domain, req.params, function (err, ret) {
            if(!err){
                res.send(ret);
            }
            else{

                res.send({error:err});
            }
        });
    }

});

server.get('/:domain/session', function(req,res,next){
    var userid = req.params.userid || null;
    var sessionid = req.params.sessionid || null;

    var domain = req.params.domain;
    session.getSessionData(domain,userid,sessionid,function(err,ret){
        if(!err){
            res.send(ret);
        }
        else{
            res.send({error:err});
        }

    })
});

server.get('/:domain/log', function(req,res,next){
    var domain = req.params.domain;
    delete req.params.domain;
    if(req.params.events){
        if(typeof(req.params.events==='string')){
            req.params.events = req.params.events.split(',');
        }
    }
    session.getLogItems(domain,req.params,function(err,ret){
        if(!err){
            res.send(ret);
        }
        else{
            res.send({error:err});
        }
    });

});

directoryClient.getService('session',function(err,ret){
    if(ret){
        server.listen(ret.endpoint.port);
    }
    else{
        server.listen(8282);
    }
})

