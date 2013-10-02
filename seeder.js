var loader = require("./loader");
var es = require("./es");

function start() {
  var mappings = require("./lib/mappings");

  es.deleteIndex("mwc_search_test", createIndex);

  function createIndex() {
    es.createIndex("mwc_search_test", mappings)
      .on("data", function(data) {
        console.log(data);
        seedSchools();
      })
      .exec();
  }
}

function seedSchools() {
  console.log("Seeding schools ...");
  loader.partialLoadSchools(100, seedCourses);
}

function seedCourses(schools) {
  console.log("Seeding courses ...");
  loader.loadCourses(schools, 100, seedStudents);
}

function seedStudents(courses) {
  console.log("Seeding students ...");
  // Load 1000 students and assign 10 courses to each
  loader.loadStudents(courses, 1000, 10, seedDocuments);
}

function seedDocuments(students) {
  console.log("Seeding documents ...");
  // Load 10 documents per student
  loader.loadDocuments(students, 10, function() {
    console.log("Done seeding :)");
  });
}

start();