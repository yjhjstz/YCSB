var os = require('os');
var spawn = require('child_process').spawn;
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);
var port = process.env.PORT || 3000;


server.listen(port, function () {
  console.log('mini-Slack server listening at port %d', port);
});

// Routing
app.use(express.static(__dirname + '/public'));

var getIPAddress = function () {
  var ifaces = os.networkInterfaces();
  var ip = '';
  for (var dev in ifaces) {
    ifaces[dev].forEach(function (details) {
      if (ip === '' && details.family === 'IPv4' && !details.internal) {
        ip = details.address;
        return;
      }
    });
  }
  return ip || "127.0.0.1";
};

// Channels
var numUsers = 0;

function help(socket) {
  var usage = ["Usage:", "run : run benchmark", "help : help usage"];
  usage.forEach(function(element) {
    socket.emit('new message', {
      username: socket.username,
      message: element
    });
  });
}

function slack(socket, data) {
  if (data === "run") {
      var ls = spawn('./run.sh');
      ls.stdout.on('data', (result) => {
        socket.emit('new message', {
          username: socket.username,
          message: result.toString()
        });
      });
      ls.on('close', (code) => {
        socket.emit('new message', {
          username: socket.username,
          message: "Done, get result  Redirect to: http://" + getIPAddress() +":8000"
        });
      });
  } else {
    help(socket);
  }
}


io.on('connection', function (socket) {
  var addedUser = false;

  // when the client emits 'new message', this listens and executes
  socket.on('new message', function (data) {
    // we tell the client to execute 'new message'
    socket.broadcast.emit('new message', {
      username: socket.username,
      message: data
    });
    slack(socket, data);
  });

  // when the client emits 'add user', this listens and executes
  socket.on('add user', function (username) {
    if (addedUser) return;

    // we store the username in the socket session for this client
    socket.username = username;
    ++numUsers;
    addedUser = true;
    socket.emit('login', {
      numUsers: numUsers
    });
    // echo globally (all clients) that a person has connected
    socket.broadcast.emit('user joined', {
      username: socket.username,
      numUsers: numUsers
    });
  });

  // when the client emits 'typing', we broadcast it to others
  socket.on('typing', function () {
    socket.broadcast.emit('typing', {
      username: socket.username
    });
  });

  // when the client emits 'stop typing', we broadcast it to others
  socket.on('stop typing', function () {
    socket.broadcast.emit('stop typing', {
      username: socket.username
    });
  });

  // when the user disconnects.. perform this
  socket.on('disconnect', function () {
    if (addedUser) {
      --numUsers;

      // echo globally that this client has left
      socket.broadcast.emit('user left', {
        username: socket.username,
        numUsers: numUsers
      });
    }
  });
});

