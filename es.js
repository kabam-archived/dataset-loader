var esclient = require('elasticsearchclient');

// Initialize ES
module.exports = (function() {
  var opts = {
    host: 'target.monimus.com',
    port: 9200
  };

  return new (esclient)(opts);
})();