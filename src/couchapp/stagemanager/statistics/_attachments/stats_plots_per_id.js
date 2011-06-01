$db = $.couch.db("t1_uk_ral/statistics");

//===================================
//Functions to plot data
//===================================

function plot(data, reqid) {
    var id ="pie_"+reqid;
    pie(data, id);
}

function plot2(data, reqid) {
    var id = "pie2_" + reqid;
    pie(data, id);
}

function areaPlot(data, canvas){
    area(data, canvas);
}

function stackedPlot(data, reqid){
    var id = "stacked_" + reqid;
    stacked(data, id);
}

//====================================
//Funtion to generate html template
//=====================================

function genDivHtml(value){
    $("#links").append("<a href='stats_plots_per_id.html#link"+value+"'>"+value+"</a><br>");
    $("#container").append("<center><h3 id='link"+value+"'>Request ID "+value+"</h3></center>");
    $("#container").append("<div id='pie_"+value+"'>This is div is called 'pie"+value+"', this text will be replaced by a plot.</div>");
    $("#container").append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
    $("#container").append("<div id='pie2_"+value+"'>This is div is called 'pie2_"+value+"', this text will be replaced by a plot</div><p>");
    $("#container").append("<div id='area2_"+value+"'>This is div is called 'area2_"+value+"' ,this text will be replaced by a plot </div>");
    $("#container").append("<div id='area3_"+value+"'>This is div is called 'area3_"+value+"' ,this text will be replaced by a plot </div><p>");
    $("#container").append("<div id='stacked_"+value+"'>This is div is called 'stacked_"+value+"' ,this text will be replaced by a plot </div><p><br><br>");
}

//==================================================
// Functions to store stats data in global variables
//=================================================

function addStageData(value, reqid){
    if (typeof list == 'undefined'){
	list = []; // variable pushed into global scope
    }

    if (typeof list[reqid] == 'undefined'){
	list[reqid] = [];
    }

    if (value != 'undefined'){// should be typeof value
	list[reqid].push(value);
    }

    //plot when staged, incomplete and failed data have been added
    if (list[reqid].length == 3){
	plot2(list[reqid], reqid);
    }
}

function addStageBytes(value, reqid){
    if (typeof list2 == 'undefined'){
	list2 = [];//variable pushed into global scope
    }

    if (typeof list2[reqid] == 'undefined'){
	list2[reqid] = [];
    }

    if (value != 'undefined'){
	list2[reqid].push(value);
    }

    //plot when staged, incomplete and failed data have been added
    if (list2[reqid].length == 3){
	plot(list2[reqid], reqid);
    }
}

//=================================================================
//function to run through views (via other functions sequentially)
//and collect data in global variables
//===============================================================

function run_through_views()
{
    $db.view("statistics/byte_report", {
	    'group_level': 2,
		success: function(data) {
		var element = $("#data");
		var mydata = [];
		var tmp_reqid;
		for (var i in data.rows) {
		    if (data.rows[i].key[0] == "staged_bytes"){
			mydata.push(data.rows[i].value);
			tmp_reqid = data.rows[i].key[1];
			genDivHtml(tmp_reqid);
			loadData(tmp_reqid);
			loadData2(tmp_reqid);
			loadData4(tmp_reqid);
		    }
		}
	    }
	});
}

//=======================================================
//functions called to run through views and collect data
//in global variables
//======================================================

function loadData(reqid) {
    $db.view("statistics/byte_report", {
	    'group_level': 2,
		success: function(data) {
		var element = $("#data");
		//file sizes are in bytes. Need to convert to Gbytes
		var bytes_to_Gbytes = 9.31322575*Math.pow(10,-10);
		for (i in data.rows) {
		    if (data.rows[i].key[0] == "staged_bytes"){
			if (data.rows[i].key[1] == reqid){
			    addStageBytes(data.rows[i].value*bytes_to_Gbytes,reqid);
			}
		    }
		}
		loadData1b(reqid);
	    }
	});
}

function loadData1b(reqid) {
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
			if (data.rows[i].key[1] == reqid){
			    incomplete_bytes = data.rows[i].value*bytes_to_Gbytes;
			}
		    }
		    if (data.rows[i].key[0] == "failed_bytes"){
			if (data.rows[i].key[1] == reqid){
			    failed_bytes = data.rows[i].value*bytes_to_Gbytes;
			}
		    }
		}
		addStageBytes(incomplete_bytes, reqid);
		addStageBytes(failed_bytes, reqid);
	    }
	});
}

function loadData2(reqid) {
    var last_staged = 0;
    $db.view("statistics/success_failure", {
	    'group_level': 2,
		success: function(data) {
		var element = $("#data");
		for (i in data.rows) {
		    //for staged files we want the reduced value. ie.sum of all staged files
		    if (data.rows[i].key[0] == "no_of_staged_files"){
			if (data.rows[i].key[1] == reqid){
			    last_staged = data.rows[i].value;
			}
		    }
		}
		addStageData(last_staged, reqid);
		loadData3(reqid);
	    }
	});

    //for incomplete and failed we do not want the sum
    //we want the last value


}

function loadData3(reqid) {
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
		    if (data.rows[i].key[1] == reqid){
			if (data.rows[i].key[0] == "no_of_staged_files"){
			    ++index;
			    staged.push({'x':index, 'y':data.rows[i].value});
			}
		    }
		    if (data.rows[i].key[1] == reqid){
			if (data.rows[i].key[0] == "no_of_incomplete_files"){
			    ++index2;
			    incomplete.push({'x':index2, 'y':data.rows[i].value});
			}
		    }
		    if (data.rows[i].key[1] == reqid){
			if (data.rows[i].key[0] == "no_of_failed_files"){
			    ++index3;
			    failed.push({'x':index3, 'y':data.rows[i].value});
			}
		    }
		}
		var mystacked = [staged, incomplete, failed];
		stackedPlot(mystacked, reqid);

		//Want the last values of failed and incomplete files
		//The add values to stage stats pie chart
		ival = incomplete[(index2)-1].y;
		fval = failed[(index3)-1].y;
		addStageData(ival, reqid);
		addStageData(fval, reqid);
	    }
	});
}

function loadData4(reqid) {
    $db.view("statistics/stage_times_report_per_id", {
	    'reduce':false,
		success: function(data) {
		var element = $("#data");
		var index = 0;
		var index2 = 0;
		var stage_duration = [];
		var ave_stage_time = [];
		for (i in data.rows) {
		    //Only plot values for staged files
		    if (data.rows[i].key[1] == reqid){
			if (data.rows[i].key[0] == "stage_duration"){
			    ++index;
			    stage_duration.push({'x':index, 'y':data.rows[i].value});
			}
		    }
		    if (data.rows[i].key[1] == reqid){
			if (data.rows[i].key[0] == "ave_stage_time_per_file"){
			    ++index2;
			    ave_stage_time.push({'x':index2, 'y':data.rows[i].value});
			}
		    }
		}
		areaPlot(stage_duration, "area2_"+reqid);
		areaPlot(ave_stage_time, "area3_"+reqid);
	    }
	});
}


//=======================================================
//Main routine through which functions are called asynchronously
//=======================================================

$(document).ready(function() {
	run_through_views();
});
