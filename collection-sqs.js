var util = require('util');
var Rx = require('rx');
var StatsCollector = require('zetta-device-data-collection');
var AWS = require('aws-sdk');

var MAX_SIZE = 262144;

var SqsCollector = module.exports = function(options) {
  var self = this;
  StatsCollector.call(this);
  options = options || {};

  AWS.config.update({ accessKeyId: options.accessKeyId,
                      secretAccessKey: options.secretAccessKey,
                      region: options.region || 'us-east-1'
                    });
  
  if (!options.queueUrl) {
    throw new Error('Must supply queueUrl');
  }

  this.queueUrl = options.queueUrl;
  this.sqs = new AWS.SQS();
  
  var windowMs = options.windowMs || 30000;
  
  Rx.Observable.fromEvent(this.emitter, 'event')
    .window(function() { return Rx.Observable.timer(windowMs); })
    .flatMap(function(e) { return e.toArray(); })
    .filter(function(arr) { return arr.length > 0 })
    .subscribe(function (data) {
      var groups = [[]];
      var idx = 0;
      var currentSize = 0;
      data.forEach(function(x) {
        try {          
          var len = new Buffer(JSON.stringify(x)).length;
          if (len > MAX_SIZE) {
            return;
          } else if (currentSize + len > MAX_SIZE) {
            idx++;
            currentSize = 0;
            groups[idx] = [x];
          } else {
            groups[idx].push(x);
            currentSize += len;
          }
        } catch (err) {
        }
      });

      groups.forEach(function(group) {
        var params = {
          MessageBody: JSON.stringify(group),
          QueueUrl: self.queueUrl
        };

        self.sqs.sendMessage(params, function(err, data) {
          if (err) {
            self.server.error('Failed to send stats to sqs, ' + err);
          }
        });
      });
    });
};
util.inherits(SqsCollector, StatsCollector);








