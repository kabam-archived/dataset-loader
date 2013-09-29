var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/mwc_seed_data');

var SeedDoc = mongoose.model(
  'SeedDoc', 
  { 
    title: String,
    content: String,
    source: String
  }
);

module.exports = {
  SeedDoc: SeedDoc
};