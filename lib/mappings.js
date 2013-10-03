module.exports = {
  "settings": {
    "analysis": {
      "analyzer": {
        "nameAnalyzer": {
          "type": "custom",
          "tokenizer": "keyword",
          "filter": "lowercase"
        }
      }
    }
  },
  "mappings": {
    "students": {
      "properties": {
        "birthdate": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "courses": {
          "properties": {
            "school": {
              "properties": {
                "name": {
                  "type": "string",
                  "index": "analyzed",
                  "analyzer": "nameAnalyzer"
                },
                "school_id": {
                  "type": "string",
                  "index": "not_analyzed"
                }
              }
            },
            "title": {
              "type": "string"
            },
            "topic_id": {
              "type": "string",
              "index": "not_analyzed"
            }
          }
        },
        "id": {
          "type": "long",
          "index": "not_analyzed"
        },
        "member_since": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "name": {
          "type": "string",
          "index": "analyzed",
          "analyzer": "nameAnalyzer"
        }
      }
    },
    // Schools
    "schools": {
      "properties": {
        "address": {
          "type": "string",
          "index": "not_analyzed"
        },
        "city": {
          "type": "string",
          "index": "not_analyzed"
        },
        "location": {
          "type": "double",
          "index": "not_analyzed"
        },
        "name": {
          "type": "string",
          "index": "analyzed",
          "analyzer": "nameAnalyzer"
        },        
        "state": {
          "type": "string",
          "index": "not_analyzed"
        },
        "unitid": {
          "type": "string",
          "index": "not_analyzed"
        },
        "web": {
          "type": "string",
          "index": "not_analyzed"
        }
      }
    },
    // Courses
    "courses": {
      "properties": {
        "categories": {
          "type": "string",
          "index": "not_analyzed"
        },
        "description": {
          "type": "string"
        },
        "end_date": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "instructors": {
          "type": "string",
          "index": "not_analyzed"
        },
        "school": {
          "properties": {
            "name": {
              "type": "string",
              "index": "analyzed",
              "analyzer": "nameAnalyzer"
            },
            "school_id": {
              "type": "string",
              "index": "not_analyzed"
            }
          }
        },
        "start_date": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "title": {
          "type": "string"
        },
        "topic_id": {
          "type": "string",
          "index": "not_analyzed"
        }
      }
    },
    // Documents
    "documents": {
      "properties": {
        "__v": {
          "type": "long"
        },
        "content": {
          "type": "string"
        },
        "created_at": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "id": {
          "type": "string",
          "index": "not_analyzed"
        },
        "source": {
          "type": "string",
          "index": "not_analyzed"
        },
        "student": {
          "properties": {
            "id": {
              "type": "long",
              "index": "not_analyzed"
            },
            "name": {
              "type": "string",
              "index": "not_analyzed"
            }
          }
        },
        "title": {
          "type": "string"
        },
        "updated_at": {
          "type": "date",
          "format": "dateOptionalTime"
        }
      }
    }
  }
};

// EdgeNGram
var edgeNGramAnalyzer = {
  "settings": {
    "analysis": {
      "analyzer": {
        "autocomplete": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [ "standard", "lowercase", "stop", "kstem", "ngram" ]
        }
      }
    }
  }
};

