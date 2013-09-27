var fs = require("fs");
var _ = require("underscore");
var cheerio = require("cheerio");
var es = require("./es");
var csv = require("csv");
var request = require("superagent");

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
      callback(null, schools);
    })
    .on("error", function(err) {
      callback(err);
    })
}

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

exports.loadStudents = function() {

};

exports.importCoursesToJSON = function() {
  var data = [];

  function getDescription(course, cb) {
    var url = "https://www.coursera.org/maestro/api/topic/information?topic-id=";
    url += course;

    request.get(url, function(err, res) {
      var about = JSON.parse(res.text).about_the_course;
      cb(about);
    });
  }

  function getCourse() {
    var course = courses.shift();
    if(!course) {
      save();
      return;
    }
    getDescription(course.topic_id, function(desc) {
      course.description = desc;
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
        instructors: instructors
      }
    });

  getCourse();

};

exports.loadCourses = function() {
  var courses = require("./data/courses");
  var bulk = [];

  getSchools(function(err, schools) {
    schools.shift();
    _loadCourses(schools);
    console.log(bulk.length);
  });

  function _loadCourses(schools) {
    schools = schools.slice(0, 3500);
    schools.forEach(function(school, i) {
      // Let's give 100 random courses to each school
      var _coursesBySchool = _.shuffle(courses).slice(0,100)
        .forEach(function(course, j) {
          var unitid = school.unitid.toString();
          course.school = {
            school_id: unitid,
            name: school.name
          };

          var _id = unitid+"-"+course.topic_id;

          var _course = JSON.stringify(course);
          bulk.push({ index: { _index: 'mwc_search', _type: 'courses', _id: _id } });
          bulk.push(JSON.parse(_course));
        });
    });
  }
};

exports.loadCourses();
