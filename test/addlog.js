/**
 * Created by Gavin on 5/29/15.
 */
var request = require('request');

var events = [
    {event: 'view', data: '123'},
    {event: 'like', data: '123'},
    {event: 'view', data: '456'},
    {event: 'view', data: '567'},
    {event: 'search', data: 'iphone 5'},
    {event: 'view', data: '789'},
    {event: 'purchase', data: '789'}
]
function doevent(sessionid){
    if(events.length > 0){
        var event = events.pop();
        console.log(event);
        event.sessionid = sessionid;
        request.post({url:'http://localhost:8282/konga/log',form:event},function(err,ret,body){
            setTimeout(function(){
                doevent(sessionid);
            },1000 + parseInt(Math.random()* 1000));
        });
    }
}

request.get('http://localhost:8282/konga/session?userid=gavin',function(err,ret,body){
    var sessionData = JSON.parse(body);
    doevent(sessionData.sessionid);
});

/*
request.post({url:'http://localhost:8282/konga/log',form:{event:'test',data:'stuff'}},function(err,ret,body){
    console.log(err);
    console.log(body);
})
*/