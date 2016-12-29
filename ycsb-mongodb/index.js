var os = require('os');
var spawn = require('child_process').spawn;
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);
var program = require('commander');
var mongo = require('mongodb');
var Grid = require('gridfs-stream');
var fs = require('fs');
var MongoClient = mongo.MongoClient;

var url = 'mongodb://localhost:27017/myproject';
var database = null;
// Use connect method to connect to the Server
MongoClient.connect(url, function(err, db) {
  console.log("Connected correctly to mongodb server.");
  // create or use an existing mongodb-native db instance
  database = db;
});

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

function slack(socket, data) {
  var args = data.split(' ');
  // adapt to commander
  args.unshift('node', 'index.js');
  var env = Object.create(process.env);
  program
  .version('0.0.1')
  .command('run [workload] [host:port]')
  .action(function (workload, host) {
    workload = workload || 'workloada';
    env.workload = workload;
    if (host) {
      env.host = host;
    }

    var exec = spawn('sh',['run.sh'], {env: env});
    exec.stdout.on('data', (result) => {
      socket.emit('new message', {
        username: socket.username,
        message: result.toString()
      });
    });

    exec.on('close', (code) => {
      var gfs = Grid(database, mongo);
      // streaming to gridfs
      fs.readdir("result/image/",function(err, files){
       if (err) {
           return console.error(err);
       }
       files.forEach( function (file){
            if (file.indexOf(workload) == -1) {
                return;
            }
            var writestream = gfs.createWriteStream({
                filename: file,
                content_type: 'image/png',
                metadata: {
                    time: (new Date()).toLocaleString(),
                    workload: workload,
                    ip: host
                }
            });
            fs.createReadStream('result/image/' + file).pipe(writestream);
            writestream.on('close', function() {
                console.log( file );
            });

       });
      });

      socket.emit('new message', {
        username: socket.username,
        message: "Done, get result  Redirect to: http://" + getIPAddress() +":8000"
      });
    });

  });

  program.on('help', function() {
    var help = ' Usage:';
    help += '\r\n'
    help += 'run workload ip:port';
    help += '\r\n'
    socket.emit('new message', {
        username: socket.username,
        message: help
    });
  });
  program.parse(args);
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

