
function couchapp_load(scripts) {
  for (var i=0; i < scripts.length; i++) {
    document.write('<script src="'+scripts[i]+'"><\/script>')
  };
};

// Ugly hack to work around different mount points of couchdb
dbpath = document.location.href.split('/_design')[0]
couchroot = dbpath.substring(0,dbpath.lastIndexOf('/'));
dbname = dbpath.substring(dbpath.lastIndexOf('/')+1,dbpath.length)
sitename = dbname.substring(0,dbname.lastIndexOf('_'))

couchapp_load([
  "vendor/couchapp/sha1.js",
  "vendor/couchapp/json2.js",
  "vendor/couchapp/jquery.js",
  "vendor/couchapp/jquery.couch.js",
  "vendor/couchapp/jquery.couch.app.js",
  "vendor/couchapp/jquery.couch.app.util.js",
  "vendor/couchapp/jquery.mustache.js",
  "vendor/couchapp/jquery.evently.js"
]);
