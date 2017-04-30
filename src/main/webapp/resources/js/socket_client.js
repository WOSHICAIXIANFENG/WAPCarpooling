var _POS_CLIENT_SOCKET_ = null;
var socket;
function SocketClient(callback) {
//    var socket;
    var masterhost = '127.0.0.1';
    var socketport = '8889';
    var url = 'ws:' + masterhost + ':' + socketport;

    if ('WebSocket' in window) {
        socket = new WebSocket(url);
    } else if ('MozWebSocket' in window) {
        socket = new MozWebSocket(url);
    } else {
        return console.log('the broswer didn\'t support websocket!');
    }

    socket.SOPEN = false;

    socket.onopen = function (event) {
        socket.SOPEN = true;
    }

    socket.onmessage = function (event) {
        callback(event);
    }

    socket.onclose = function (event) {
        socket.SOPEN = false;
        window.setTimeout(function () { _POS_CLIENT_SOCKET_ = SocketClient(callback); }, 1e3);// reconnect in 1 second
        console.log("socket close() ");
    }

    socket.onerror = function (event) {
        socket.close();
    }
    return socket;
}

(function() {
	SocketClient(function (event) {
        var broadobj = JSON.parse(event.data);
        // check this post is come from myself ?
        let loginUserId = parseInt($("#myLink").attr("myAttr"));

        if (broadobj.userid != loginUserId) {
        	if (broadobj.posttype == 2) {
        		//refresh the notification view
        		refreshNotificationView($('#notify1'));
        	} else if (broadobj.posttype == 1) {
        		//refresh the notification view
                refreshNotificationView($('#notify2'));
        	}
        }
    });
	
	function refreshNotificationView(jQuery) {
        let con = jQuery.text();
        let count = isNaN(con) ? 1 : (parseInt(con) || 0);
        count++;
        jQuery.text(count);
        jQuery.css('background-color', 'red');
        //jQuery.css({opacity: 0}).text(count).css({top: '-20px'}).transition({top: '-5px', opacity: 1});
        console.log('count = ' + count);
	};
	
	// test send data from js function
	// {"postid": 123,"userid": 3,"postBody": "I need one trip"}
	// window.setInterval(function () { sendPostToServer("{\"postid\": 123,\"userid\": 3,\"postBody\": \"I need one trip\"}") }, 10e3);
})();


// send post to socket server in js 
function sendPostToServer(josnStr) {
//	var msg = {
//	        "postid" : text,
//	        "userid" : test
//	    };
//	msg = JSON.stringify(msg);
	socket.send(josnStr);
}

// clear notification view. type 1 --- 
function clearNotificationView(type) {
	if (type == 2) {
    	$('#notify1').text('').css('background-color', '');
    } else if (type == 1) {
    	$('#notify2').text('').css('background-color', '');
    }
};

