$db = $.couch.db("t1_uk_ral/statistics");

id_to_dataset = [];// global, messy need to change

//===================================
//Functions to plot data
//===================================

function plot(data, reqid) {
    var id ="pie_"+reqid;
    pie(data, id, ['Staged (Gbytes)','Incomplete (Gbytes)','Failed (Gbytes)']);
}

function plot2(data, reqid) {
    var id = "pie2_" + reqid;
    pie(data, id, ['No. of staged files', 'No. of incomplete files', 'No. of failed files']);
}

function areaPlot(data, canvas){
    area(data, canvas, 'Total stage time (sec)');
}

function areaPlot2(data, canvas){
    area(data, canvas, 'Average stage time per file (sec)');
}

function stackedPlot(data, reqid){
    var id = "stacked_" + reqid;
    stacked(data, id);
}


//======================================
// function to get dataset name from id
//========================================

function getDatasetName(){
    $db2 = $.couch.db("t1_uk_ral/requests");
        $db2.view("requests/request_data", {
            async: false,  //make a synchronous call!
	    'reduce': false,
		success: function(data) {
                if (typeof id_to_dataset == 'undefined'){
                    id_to_dataset = []; // is now global. Messy, should change!
		}
		for (var i in data.rows) {
                     id_to_dataset[data.rows[i].key[0]] = data.rows[i].key[1][0];
		}
	    }
	});
}

//====================================
//Funtion to generate html template
//=====================================

function genDropdownList(value){
    $("#links").append("<li id='"+value+"'>"+id_to_dataset[value]+"</li>");
}

function genDivHtml(value,f){
//need to use the empty() method first to clear the page
    $("#container").empty().append("<center><h3 id='link"+value+"'>Dataset "+id_to_dataset[value]+"</h3></center>");
    $("#container").append("<div id='pie_"+value+"'>This is div is called 'pie_"+value+"', this text will be replaced by a plot.</div>");
    $("#container").append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
    $("#container").append("<div id='pie2_"+value+"'>This is div is called 'pie2_"+value+"', this text will be replaced by a plot</div><p><p>");
    $("#container").append("<div id='area2_"+value+"'>This is div is called 'area2_"+value+"' ,this text will be replaced by a plot </div>");
    $("#container").append("<div id='area3_"+value+"'>This is div is called 'area3_"+value+"' ,this text will be replaced by a plot </div><p><p>");
    $("#container").append("<div id='stacked_"+value+"'>This is div is called 'stacked_"+value+"' ,this text will be replaced by a plot </div><p><br><br>");
    delListVars(); //remove global variables list and list2
    if (typeof f == 'function'){
       f(value);
    }
}


//==================================================
// Functions to store stats data in global variables
//=================================================

//function to clean up global variables
function delListVars(){
    if (typeof list != 'undefined'){
        delete list;
    }
    if (typeof list2 != 'undefined'){
        delete list2;
    }
}

function addStageData(value, reqid){

    //signal to reset list. This is dirty. Should implement better!
    if (typeof list == 'undefined'){
	list = []; // variable pushed into global scope
    }

    if (typeof list[reqid] == 'undefined'){
	list[reqid] = [];
    }

    if (typeof value != 'undefined'){// should be typeof value
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

    if (typeof value != 'undefined'){
	list2[reqid].push(value);
    }

    //plot when staged, incomplete and failed data have been added
    if (list2[reqid].length == 3){
	plot(list2[reqid], reqid);
    }
}

//=================================================================
//function to run through views and collect request ids
// and populate dropdown list with this info
//
//(via other functions sequentially)
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
			genDropdownList(tmp_reqid);
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
		for (var i in data.rows) {
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
		for (var i in data.rows) {
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
		for (var i in data.rows) {
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
		for (var i in data.rows) {
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
		for (var i in data.rows) {
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
		areaPlot2(ave_stage_time, "area3_"+reqid);
	    }
	});
}


//===============================================================
//Main routine through which functions are called asynchronously
//===============================================================

$(document).ready(function() {
  getDatasetName(); //get the names of the datasets first. Synchrous call!
  run_through_views();
  $("li").live('click',function() {
    var li_id = $(this).attr('id');
    //genDivHtml(li_id);
    genDivHtml(li_id,loadData);// No need for callback here
    //loadData(li_id);
		loadData2(li_id);
		loadData4(li_id);
  });
});
