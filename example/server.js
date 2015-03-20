var zetta = require('zetta');
var SqsCollector = require('../');

var sqs = new SqsCollector({
  accessKeyId: 'keyId',
  secretAccessKey: 'secret',
  region: 'us-east-1',
  queueUrl: 'http://somequeueurl'
});

zetta()
  .name('cloud')
  .use(sqs.collect())
  .listen(5000);
