$db = $.couch.db("t1_uk_ral/statistics");

//==================================================
// Functions to store stats data in global variables
//=================================================

function addStageData(value){
    if (typeof list == 'undefined'){
	list = [];
    }

    if (value != 'undefined'){// should be typeof value
	list.push(value);
    }

    //plot when staged, incomplete and failed data have been added
    if (list.length == 3){
	plot2(list);
    }
}

function addStageBytes(value){
    if (typeof list2 == 'undefined'){
	list2 = [];
    }

    if (value != 'undefined'){
	list2.push(value);
    }

    //plot when staged, incomplete and failed data have been added
    if (list2.length == 3){
	plot(list2);
    }
}

//===================================
//Functions to plot data
//===================================

function plot(data) {
    pie(data, "pie");
}

function plot2(data) {
    pie(data, "pie2");
}

function areaPlot(data, canvas){
    area(data, canvas);
}

function stackedPlot(data, canvas){
    stacked(data, canvas);
}

//=======================================================
//functions called to run through views and collect data
//in global variables
//======================================================

function loadData() {
    $db.view("statistics/byte_report", {
	    'group_level': 1,
		success: function(data) {
		var element = $("#data");
		//file sizes are in bytes. Need to convert to Gbytes
		var bytes_to_Gbytes = 9.31322575*Math.pow(10,-10);
		for (i in data.rows) {
		    if (data.rows[i].key[0] == "staged_bytes"){
			addStageBytes(data.rows[i].value*bytes_to_Gbytes);
		    }
		}
		loadData1b();
	    }
	});
}

function loadData1b() {
    $db.view("statistics/byte_report", {
	    'reduce':false,
		success: function(data) {
		var element = $("#data");
		//file sizes are in bytes. Need to convert to Gbytes
		var bytes_to_Gbytes = 9.31322575*Math.pow(10,-10);
		var incomplete_bytes = 100;
		var failed_bytes = 200;
		for (i in data.rows) {
		    //iniefficient way of recording last two values.
		    if (data.rows[i].key[0] == "incomplete_bytes"){
			incomplete_bytes = data.rows[i].value*bytes_to_Gbytes;
		    }
		    if (data.rows[i].key[0] == "failed_bytes"){
			failed_bytes = data.rows[i].value*bytes_to_Gbytes;
		    }
		}
		addStageBytes(incomplete_bytes);
		addStageBytes(failed_bytes);
	    }
	});
}

function loadData2() {
    var last_staged = 0;
    $db.view("statistics/success_failure", {
	    'group_level': 1,
		success: function(data) {
		var element = $("#data");
		for (i in data.rows) {
		    //for staged files we want the reduced value. ie.sum of all staged files
		    if (data.rows[i].key[0] == "no_of_staged_files"){
			last_staged = data.rows[i].value;
		    }
		}
		addStageData(last_staged);
		loadData3();
	    }
	});
    //for incomplete and failed we do not want the sum
    //we want the last value
}

function loadData3() {
    $db.view("statistics/success_failure", {
	    'reduce':false,
		success: function(data) {
		var element = $("#data");
		var index = 0;
		var index2 = 0;//only need one index
		var index3 = 0;
		var staged = [];
		var incomplete = [];
		var failed = [];
		var ival = 0;
		var fval = 0;
		for (i in data.rows) {
		    //Only plot values for staged files
		    if (data.rows[i].key[0] == "no_of_staged_files"){
			++index;
			staged.push({'x':index, 'y':data.rows[i].value});
		    }
		    if (data.rows[i].key[0] == "no_of_incomplete_files"){
			++index2;
			incomplete.push({'x':index2, 'y':data.rows[i].value});
		    }
		    if (data.rows[i].key[0] == "no_of_failed_files"){
			++index3;
			failed.push({'x':index3, 'y':data.rows[i].value});
		    }
		}
		var mystacked = [staged, incomplete, failed];
		stackedPlot(mystacked, "stacked");

		//Want the last values of failed and incomplete files
		//The add values to stage stats pie chart
		ival = incomplete[(index2)-1].y;
		fval = failed[(index3)-1].y;
		addStageData(ival);
		addStageData(fval);
	    }
	});
}

function loadData4() {
  $db.view("statistics/stage_times_report", {
    'reduce':false,
    success: function(data) {
      var element = $("#data");
      var index = 0;
      var index2 = 0;
      var stage_duration = [];
      var ave_stage_time = [];
      for (i in data.rows) {
      //Only plot values for staged files
        if (data.rows[i].key == "stage_duration"){
          ++index;
          stage_duration.push({'x':index, 'y':data.rows[i].value});
        }
        if (data.rows[i].key == "ave_stage_time_per_file"){
          ++index2;
          ave_stage_time.push({'x':index2, 'y':data.rows[i].value});
        }
      }
      areaPlot(stage_duration, "area2");
      areaPlot(ave_stage_time, "area3");
    }
  });
}

//===========================================================
//main doc through which functions are called asynchronously
//============================================================

$(document).ready(function() {
  loadData();
  loadData2();
  loadData4();
});
