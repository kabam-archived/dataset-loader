var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/mwc_seed_data');

var SeedDoc = mongoose.model(
  'SeedDoc', 
  { 
    title: String,
    content: String,
    source: String,
    created_at: Date,
    updated_at: Date
  }
);

module.exports = {
  SeedDoc: SeedDoc
};