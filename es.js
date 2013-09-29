var esclient = require('elasticsearchclient');

// Initialize ES
module.exports = (function() {
  var opts = {
    host: 'localhost',
    port: 9200
  };

  return new (esclient)(opts);
})();