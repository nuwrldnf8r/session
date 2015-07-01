/**
 * Created by Gavin on 6/10/15.
 */
var request = require('request');

var $ = {};

$.url = 'http://localhost:8181'

$.getService = function(service,callback){
    request.get($.url + '/' + service, function(err,ret,body) {
        try {
            body = JSON.parse(body);
            if(body.error){
                callback(new Error('service does not exist'),null);
            }
            else{
                callback(null,body);
            }
        }
        catch(e){
            callback(new Error('service does not exist'),null);
        }

    })
}

module.exports = $;