/**
 * Created by Gavin on 5/22/15.
 */
var rabbit = require('rabbit.js');

var $ = {};

$.broker = 'amqp://localhost';

$.listener = function(channel, messageHandler){
    var self = this;
    self.connected = false;
    self.messageHandler = messageHandler;
    self.channel = channel;

    self.initialize = function(callback){
       self.context = rabbit.createContext($.broker);
       self.context.on('ready',function(){
           self.ready = true;
           self.sub = self.context.socket('SUB');
           self.sub.setEncoding('utf8');
           self.sub.connect(self.channel, function() {
               self.connected = true;
               self.sub.on('data', function(data){
                   var parsedData = JSON.parse(data);
                   self.messageHandler(parsedData);
               });
               if(callback) {
                   callback(null, true);
               }
           });
       });
    };

    self.close = function(callback){
        self.sub.close();
        self.context.on('close',function(){
            if(callback) {
                callback(null, true);
            }
        });
    };
};

$.publisher = function(channel){
    var self = this;
    self.channel = channel;

    self.initialize = function(callback){
        self.context = rabbit.createContext($.broker);
        self.context.on('ready',function(){
            self.ready = true;
            self.pub = self.context.socket('PUB');
            self.pub.connect(self.channel, function() {
                self.connected = true;
                console.log('connected');
                if(callback) {
                    callback(null, true);
                }
            });

        });
    };

    self.publish = function(data,callback){
       if(self.connected){
           self.pub.write(JSON.stringify(data), 'utf8');
           if(callback){
               callback(null, true);
           }
       }
        else{
           if(callback){
               callback(new Error('not connected'),null);
           }

       }
    }
};


module.exports = $;












