var $ = {};

$.redis = [{port: 6379, host: 'localhost',name: 'chat1'}];
$.elastichost = 'localhost:9200';

$.mapping = {
    properties:
    {
        userid:
        {
            type: 'string',
            index: 'not_analyzed'
        },
        sessionid:
        {
            type: 'string',
            index: 'not_analyzed'
        },
        event:
        {
            type: 'string',
            index: 'not_analyzed'
        },
        data:
        {
            type: 'string',
            index: 'not_analyzed'
        }
    }
};

$.publicChannel = 'session';
$.privateChannel = 'session_local';


module.exports = $;