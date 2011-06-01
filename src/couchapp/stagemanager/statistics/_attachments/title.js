 $db = $.couch.db("t1_uk_ral/statistics");

//====================================================
// Get the name of site
//====================================================

function genTitle()
{
//Sadly I am running a view in order to get site name.
//Should find a better way to access site name as opposed to storing it many times in a db
	$db.view("statistics/stage_duration", {
    'reduce':false,
		success: function(data) {
      var siteName;
			for (var i in data.rows) {
 				if (data.rows[i].key[0] == "Database submissions"){
					siteName = data.rows[i].key[1];
					break;
				}
			}
      printTitle(siteName);
		}
	});
}

//===================================================
//Print Title
//===================================================

function printTitle(sName)
{
	$("#statsTitle").append("<H1>Stagemanager "+sName+" Statistics Plots</H1>")
}
