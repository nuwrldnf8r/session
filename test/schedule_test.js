/**
 * Created by Gavin on 5/27/15.
 */
var scheduler = require('../lib/scheduler');

var events = ['michelle','gavin','567','678','789','987','876','765','654','543','432','322'];

function populate(){
    if(events.length>0){
        var e = events.pop();
        scheduler.addEvent('konga',e,Date.now() + 30000,function(err,ret){
            setTimeout(populate,5000);
        });
    }
}
scheduler.initialize('session','session_local',function(err,ret){
    //setTimeout(populate,5000)
});
