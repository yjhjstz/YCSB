#! /bin/bash
# install node.js
wget https://nodejs.org/dist/v6.9.1/node-v6.9.1-linux-x64.tar.gz --no-check-certificate
sudo tar --strip-components 1 -xzvf node-v* -C /usr/local
node -v
# install ycsb
mvn clean package
# install node.js modules
npm install
npm install -g anywhere pm2
# run
npm start
