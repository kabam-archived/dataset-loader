require("date-utils");
var fs = require("fs");
var _ = require("underscore");
var cheerio = require("cheerio");
var es = require("./es");
var mongo = require("./mongo");
var csv = require("csv");
var request = require("superagent");
var faker = require("./lib/Faker.js");

var Util = {
  randomDate: function(start, end) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
  }
};

var noop = function() {};

function getSchools(callback) {
  var schools = [];

  csv().from.path("./data/schools.csv", { delimiter: ',', escape: '"' })
    .on("record", function(row) {
      schools.push({
        unitid: row[0],
        name: row[1],
        address: row[2],
        city: row[3],
        state: row[4],
        web: row[14],
        location: [parseFloat(row[63]), parseFloat(row[64])] // [lat, lon]
      });
    })
    .on("end", function() {
      schools.shift();
      callback(null, schools);
    })
    .on("error", function(err) {
      callback(err);
    })
}

// Load all schools from CSV to ES
exports.loadSchools = function() {
  var bulk = [];

  csv().from.path("./data/schools.csv", { delimiter: ',', escape: '"' })
    .on("record", function(row) {
      var school = {
        unitid: row[0],
        name: row[1],
        address: row[2],
        city: row[3],
        state: row[4],
        web: row[14],
        location: [parseFloat(row[63]), parseFloat(row[64])] // [lat, lon]
      };

      bulk.push({ index: { _index: 'mwc_search', _type: 'schools', _id: school.unitid+'' } });
      bulk.push(school);
    })
    // Save to ES
    .on("end", function() {
      function save() {
        var batch = bulk.splice(0, 600);
        if(batch.length) {
          console.log(batch.length);
          es.bulk(batch, save);
        } else {
          console.log("I'm done!");	
          process.exit();
        }
      }
      save();
    });
};

// Import courses from Coursera
exports.importCoursesToJSON = function() {
  var data = [];

  function getInfo(course, cb) {
    console.log("Getting course "+course);
    var url = "https://www.coursera.org/maestro/api/topic/information?topic-id=";
    url += course;

    request.get(url, function(err, res) {
      var courseInfo = JSON.parse(res.text);
      cb({
        about: courseInfo.about_the_course,
        categories: (courseInfo.categories||[]).map(function(cat) {
          return cat.name
        })
      });
    });
  }

  function getCourse() {
    var course = courses.shift();
    if(!course) {
      save();
      return;
    }
    getInfo(course.topic_id, function(info) {
      course.description = info.about;
      course.categories = info.categories;
      data.push(course);
      getCourse();
    });
  }
  
  function save() {
    fs.writeFileSync("./data/courses.json", JSON.stringify(data, null, 2));
  }

  // Get courses from a base HTML grabbed from Coursera
  var $ = cheerio.load(fs.readFileSync("./data/courses.html", "utf8"));
  var courses = $(".coursera-catalog-course-listing-box")
    .map(function() {
      var id = $(this).attr("data-topic-id");
      var school = $(this).find(".coursera-catalog-listing-secondary-link")
        .text();
      var title = $(this).find(".coursera-catalog-listing-courselink")
        .text();
      var link = $(this).find(".coursera-catalog-listing-courselink")
        .attr("href")
        .replace(/\/course\//, ""); 
      var instructors = $(this).find(".coursera-catalog-listing-instructor")
        .text()
        .replace(/with\s/, "")
        .replace(/\&/, ",")
        .split(",")
        .map(function(i) {
          return i.trim();
        });

      return { 
        topic_id: link,
        title: title,
        instructors: instructors,
        start_date: start_date,
        end_date: end_date
      }
    });

  getCourse();

};

exports.loadCourses = function(schools, size, callback) {
  if(!(_.isArray(schools) && _.isNumber(size) && _.isFunction(callback))) {
    return;
  }

  var loadedCourses = [];
  var courses = require("./data/courses");
  loadBySchool(schools.shift());

  function loadBySchool(school) {
    if(!school) {
      callback(loadedCourses);
      return;
    }

    var bulk = [];
    var coursesBySchool = _.shuffle(courses).slice(0, size);

    coursesBySchool.forEach(function(course, j) {
      var unitid = school.unitid.toString();
      course.school = {
        school_id: unitid,
        name: school.name
      };
      course.start_date = Util.randomDate(new Date(2013,9,1), new Date(2014,5,1));
      course.end_date = Util.randomDate(
        course.start_date, 
        (new Date(course.start_date.getTime())).addDays(Math.random()*360|0)
      );
      course.price = course.start_date.getDaysBetween(course.end_date)*(20*Math.random()|0);

      var _id = unitid+"-"+course.topic_id;

      var _course = JSON.stringify(course);
      bulk.push({ index: { _index: "mwc_search_test", _type: "courses", _id: _id } });
      bulk.push(JSON.parse(_course));

      loadedCourses.push(JSON.parse(_course));
    });

    es.bulk(bulk, loadBySchool.bind(this, schools.shift()));
  }
};

exports.partialLoadSchools = function(size, callback) {
  var loadedSchools;
  
  getSchools(function(err, schools) {
    var _schools = _.shuffle(schools.slice(0, size));
    loadedSchools = _schools.slice(0);
    _load(_schools);
  });

  function _load(schools) {
    if(!schools.length) {
      console.log("Done loading schools");
      callback(loadedSchools);
      return;
    }

    var bulk = [];
    var _schools = schools.splice(0,300);
    _schools.forEach(function(school) {
      bulk.push({ index: { _index: "mwc_search_test", _type: "schools", _id: school.unitid+"" } });
      bulk.push(school);
    });
    es.bulk(bulk, _load.bind(this, schools));
  }
};

exports.loadStudents = function(courses, nStudents, nCoursesPerStudent, callback) {
  if(!(_.isArray(courses) && _.isNumber(nStudents) && _.isNumber(nCoursesPerStudent) && _.isFunction(callback))) {
    return;
  }

  if(nCoursesPerStudent > courses.length) {
    return;
  }

  var size = nStudents;
  var loadedStudents = [];

  var students = (function() {
    var _students = [];
    while(size) {
      _students.push({
        id: Date.now()+(Math.random()*1000000)|0,
        name: faker.Name.findName(),
        birthdate: Util.randomDate(new Date(1980,0,1), new Date(1998,0,1)),
        member_since: Util.randomDate(new Date(2013,0,1), new Date())
      });
      size--;
    }
    return _students;
  })();

  loadByStudent(students.shift());

  function loadByStudent(student) {
    if(!student) {
      callback(loadedStudents);
      return;
    }

    student.courses = courses.splice(0, nCoursesPerStudent)
      .map(function(course) {
        delete course.instructors;
        delete course.description;
        return course;
      });
    
    es.index("mwc_search_test", "students", student, student.id, function(err, data) {
      courses = courses.concat(student.courses);
      loadByStudent(students.shift());
    });
    
    loadedStudents.push(student);
  }
};

// Import documents from wikipedia articles to Mongo
exports.importDocumentsToMongo = function(students, callback) {
  var wikipediaBase = "http://en.wikipedia.org";
  var initialSeeds = [
    "/wiki/Quantum_mechanics",
    "/wiki/General_relativity",
    "/wiki/Genomics",
    "/wiki/Architecture",
    "/wiki/Politics",
    "/wiki/Philosophy",
    "/wiki/Mathematics",
    "/wiki/Computer_science",
    "/wiki/Colombia"
  ];

  var seedDocuments = [].concat(initialSeeds);
  
  getMoreSeeds(initialSeeds.shift());

  function getMoreSeeds(url) {
    if(!url) {
      seedDocuments = _.uniq(seedDocuments);
      console.log(seedDocuments.length);
      importDoc();
      return;
    }

    console.log("Getting from "+url);
    request.get(wikipediaBase+url, function(err, res) {
      var $ = cheerio.load(res.text);
      var $content = $("#bodyContent #mw-content-text");

      var links = $content.find("a").toArray();
      links = links.map(function(a) {
        return $(a).attr("href").trim();
      })
      .filter(function(link) {
        return link.match(/^\/wiki\//);
      });

      seedDocuments = seedDocuments.concat(links);
      getMoreSeeds(initialSeeds.shift());
    });
  }

  function importDoc() {
    var sourceUrl = seedDocuments.shift();
    if(!sourceUrl) {
      return;
    }
    
    console.log("Getting from "+sourceUrl);
    request.get(wikipediaBase+sourceUrl, function(err, res) {
      var $ = cheerio.load(res.text);
      var $content = $("#bodyContent #mw-content-text");
      var title = $("#firstHeading").text();
      var doc = new mongo.SeedDoc({
        title: title,
        content: $content.text().replace(/\r?\n|\r/g, " "),
        source: sourceUrl
      });
      doc.save(function() {
        importDoc();
      });
    });
  }
};

exports.loadDocuments = function(students, nDocsPerStudent, callback) {
  var n = nDocsPerStudent;
  var count;
  mongo.SeedDoc.count(function(err, _count) {
    count = _count;
    _load(0);
  });

  function _load(skip) {
    var student = students.shift();
    if(!student) {
      callback();
      return;
    }
    
    delete student.birthdate;
    delete student.member_since;
    delete student.courses;

    mongo.SeedDoc.find({}, null, { skip: skip, limit: n }, function(err, docs) {
      var bulk = [];
      docs.forEach(function(doc) {
        doc = doc.toObject();
        doc.id = doc._id;
        delete doc._id;
        doc.student = student;
        doc.created_at = Util.randomDate(new Date(2013,0,1), new Date());
        doc.updated_at = Util.randomDate(doc.created_at, new Date());

        var _id = Date.now()+(Math.random()*100000000)|0;
        bulk.push({ index: { _index: "mwc_search_test", _type: "documents", _id: _id } });
        bulk.push(doc);
      });
      es.bulk(bulk, function(err, data) {
        _load(skip + n > count ? 0 : skip + n);
      });
    });
  }
};

