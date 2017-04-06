/*********************************************************************************************
 *
 *
 *                      VARIABLES
 *
 *
 ********************************************************************************************/
//Import mysql module
var mysql = require('mysql');
//Import exec module which will be used to execute the curl command
var exec = require('child_process').exec;
//Timestamp variable used to store the last timestamp to check for new records
var last_timestamp;
//Variable which stores the details for the mysql connection
var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : 'rootpass',
  database : 'test'
});

//Variables for the user defined column families - get assigned later
var USER_DEFINED_1_COL;
var USER_DEFINED_2_COL;
var USER_DEFINED_3_COL;
var USER_DEFINED_4_COL;
var USER_DEFINED_5_COL;

//URL which will be needed for the curl command
var url;

//Identifier in the mysql table
var task_id;
/********************************************************************************************
 *
 *
 *                      CONSTANTS
 *   Used to store the values of the column family: column qualifier's for HBase insertion
 *
 ********************************************************************************************/
//Create Column Family:Column Qualifier's
//Project Column Family
const PROJECT_NAME_COL=encode('Project:ProjectName');
//Task Column Family
const TASK_TYPE_COL= encode('Task:TaskType');
const TASK_NAME_COL= encode('Task:TaskName');
const TASK_ID_COL= encode('Task:TaskID');
//Action Column Family
const ACTION_TYPE_COL=encode('Action:ActionType');
const ACTION_TS_COL=encode('Action:ActionTimeStamp');
//Script Column Family
const SCRIPT_COL=encode('ScriptPath:ScriptPath');
//Error Column Family
const ERROR_CODE_COL=encode('Error:ErrorCode');
const ERROR_TEXT_COL=encode('Error:ErrorText');
const ERROR_LINK_COL=encode('Error:ErrorLink');
//Table Column Family
const TABLE_NAME_COL=encode('Table:TableName');
const TABLE_ROWS_AFFECTED_COL=encode('Table:RowsAffected');
const TABLE_DOLLAR_VALUE_COL=encode('Table:DollarValue');
//Sub Task Column Family
const SUB_TASK_ID_COL=encode('SubTask:SubTaskID');
const SUB_TASK_NAME_COL=encode('SubTask:SubTaskName');
/********************************************************************************************
 *
 *
 *                      FUNCTIONS/METHODS
 *
 *
 ********************************************************************************************/
/********************************************************************************************
 *
 *
 *                      encode
 *              Base64 encodes the data - necessary for insertion to HBase
 *
 ********************************************************************************************/
function encode(column){
        if (column == null) {
                return;
        }
        var encoded_string = new Buffer(column).toString('base64');
        return encoded_string;
};
/********************************************************************************************
 *
 *
 *                      getLastTS
 *  Retrieve the last timestamp from the timestamp table and store it in the variable last_timestamp
 *
 ********************************************************************************************/
function getLastTS(){
console.log('Getting the latest timestamp from the timestamp table');
connection.query("select last_timestamp from api_last_timestamp where Process_type = 'H'",function(err, rows, fields) {
        if(err){
            console.log('Couldnt retrieve the latest timestamp from the table');
        }
        else
        {
                        var timestamp_rows = rows[0];

                        for (var i in timestamp_rows){
                        last_timestamp = timestamp_rows[i];
                        console.log('The last timestamp is: ' + last_timestamp);
                        }
                        //Call the checkRows method
                        checkRows();
        }
});
}
/********************************************************************************************
 *
 *
 *                      checkRows
 *   Gather all rows which were inserted to mysql since last job, and insert into HBase
 *
 ********************************************************************************************/
function checkRows() {
console.log('Check for updated rows');
//Return all rows where the insert_ts is greater than the last timestamp
connection.query(`select * from alan_test where insert_timestamp >= ?`, [last_timestamp], function(err, results, fields) {
   if (err) {
      console.log('MySQL Error while checking for new records.');
   }
   else {
           //Check for updated records
           if (results.length < 1) {
                   console.log("No records to process");
		process.exit(0);
           }
		   else{
		//Go through each of the rows returned and store the results in a variables
        for (var i in results){
        var row = results[i];

        console.log("Project Name: " + row.api_project_name);
        console.log("Task Type: " + row.api_task_type);
        console.log("Task ID: " + row.api_task_id);
        console.log("Task Name: " + row.api_task_name);
        console.log("Action Type: " + row.api_action_type);
        console.log("Action Timestamp: " + row.api_action_timestamp);
        console.log("Script Full Path: " + row.api_script_full_path);
        console.log("Error Code: " + row.api_error_code);
        console.log("Error Text: " + row.api_error_text);
        console.log("Error Link: " + row.api_error_link);
        console.log("Table Name: " + row.api_table_name);
        console.log("Rows Affected: " + row.api_rows_affected);
        console.log("Dollar Value: " + row.api_dollar_value);
        console.log("Sub Task ID: " + row.api_sub_task_id);
        console.log("Sub Task Name: " + row.api_sub_task_name);
        console.log("App Defined Key 1: " + row.api_app_defined_key_1);
        console.log("App Defined Value 1: " + row.api_app_defined_value_1);
        console.log("App Defined Key 2: " + row.api_app_defined_key_2);
        console.log("App Defined Value 2: " + row.api_app_defined_value_2);
        console.log("App Defined Key 3: " + row.api_app_defined_key_3);
        console.log("App Defined Value 3: " + row.api_app_defined_value_3);
        console.log("App Defined Key 4: " + row.api_app_defined_key_4);
        console.log("App Defined Value 4: " + row.api_app_defined_value_4);
       console.log("App Defined Key 5: " + row.api_app_defined_key_5);
        console.log("App Defined Value 5: " + row.api_app_defined_value_5);

        //Store the relevant returned row into a variable and encode it
        var project_name                 = encode(row.api_project_name) ;
        var task_type                    = encode(row.api_task_type)           ;
        task_id                      = (row.api_task_id)             ;
        var task_name                    = encode(row.api_task_name)            ;
        var action_type                  = encode(row.api_action_type)          ;
        var action_timestamp             = encode(row.api_action_timestamp)     ;
        var script_full_path             = encode(row.api_script_full_path)     ;
       var error_code                   = encode(row.api_error_code)           ;
        var error_text                   = encode(row.api_error_text)           ;
        var error_link                   = encode(row.api_error_link)           ;
        var table_name                   = encode(row.api_table_name)           ;
        var rows_affected                = encode(row.api_rows_affected)        ;
        var dollar_value                 = encode(row.api_dollar_value)         ;
        var sub_task_id                  = encode(row.api_sub_task_id)          ;
        var sub_task_name                = encode(row.api_sub_task_name)        ;
        var app_defined_key_1            = (row.api_app_defined_key_1)    ;
        var app_defined_value_1          = encode(row.api_app_defined_value_1)  ;
        var app_defined_key_2            = (row.api_app_defined_key_2)    ;
        var app_defined_value_2          = encode(row.api_app_defined_value_2)  ;
        var app_defined_key_3            = (row.api_app_defined_key_3)    ;
        var app_defined_value_3          = encode(row.api_app_defined_value_3)  ;
        var app_defined_key_4            = (row.api_app_defined_key_4)    ;
        var app_defined_value_4          = encode(row.api_app_defined_value_4)  ;
       var app_defined_key_5            = (row.api_app_defined_key_5)    ;
        var app_defined_value_5          = encode(row.api_app_defined_value_5)  ;

	var insert_timestamp = (new Date(row.insert_timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, ''));

        //Assign the User Defined Column Family
        USER_DEFINED_1_COL=encode('UserDefined:' + app_defined_key_1);
        USER_DEFINED_2_COL=encode('UserDefined:' + app_defined_key_2);
        USER_DEFINED_3_COL=encode('UserDefined:' + app_defined_key_3);
       USER_DEFINED_4_COL=encode('UserDefined:' + app_defined_key_4);
       USER_DEFINED_5_COL=encode('UserDefined:' + app_defined_key_5);

        //Put the data into HBase using the runCurl command
        
	runCurl(task_id, insert_timestamp, PROJECT_NAME_COL, project_name);
        runCurl(task_id, insert_timestamp, TASK_TYPE_COL, task_type);
        runCurl(task_id, insert_timestamp, TASK_ID_COL, task_id);
        runCurl(task_id, insert_timestamp, TASK_NAME_COL, task_name);
        runCurl(task_id, insert_timestamp, ACTION_TYPE_COL, action_type);
        runCurl(task_id, insert_timestamp, ACTION_TS_COL, action_timestamp);
        runCurl(task_id, insert_timestamp, SCRIPT_COL, script_full_path);
        runCurl(task_id, insert_timestamp, ERROR_CODE_COL, error_code);

        runCurl(task_id, insert_timestamp, ERROR_CODE_COL, error_code);
        runCurl(task_id, insert_timestamp, ERROR_TEXT_COL, error_text);
        runCurl(task_id, insert_timestamp, ERROR_LINK_COL, error_link);
        runCurl(task_id, insert_timestamp, TABLE_NAME_COL, table_name);
        runCurl(task_id, insert_timestamp, TABLE_ROWS_AFFECTED_COL, rows_affected);
        runCurl(task_id, insert_timestamp, TABLE_DOLLAR_VALUE_COL, dollar_value);
        runCurl(task_id, insert_timestamp, SUB_TASK_ID_COL, sub_task_id);
        runCurl(task_id, insert_timestamp, SUB_TASK_NAME_COL, sub_task_name);

        runCurl(task_id, insert_timestamp, USER_DEFINED_1_COL, app_defined_value_1);
        runCurl(task_id, insert_timestamp, USER_DEFINED_2_COL, app_defined_value_2);
        runCurl(task_id, insert_timestamp, USER_DEFINED_3_COL, app_defined_value_3);



	runCurl(task_id, insert_timestamp, USER_DEFINED_4_COL, app_defined_value_4);
	runCurl(task_id, insert_timestamp, USER_DEFINED_5_COL, app_defined_value_5);

	//Update the hbase_flag in the mysql table
	updateColumn(row.insert_timestamp);
        }
        //Run the update table method
        updateTSTable();
		   }
        }
});
}
/********************************************************************************************
 *
 *
 *                      runCurl
 *   Executes the curl command, sending the data to HBase
 *
 ********************************************************************************************/
function runCurl(rowNumber,ts, columnFamily, data){
     var key = rowNumber;
        var column = columnFamily;
        var data = data;
	var ts = ts;
	var row_key = encode(key + " " + ts);
	console.log(row_key);

        //URL for the curl command where the results will be saved in HBase
        url = 'http://localhost:9090/alan_test/${task_id}';
        if (data == null){
        return;
        }
        else {
	var row = `{\"Row\":[{\"key\":\"${row_key}\", \"Cell\":[{\"column\":\"${column}\", \"$\":\"${data}\"}]}]}`;        
        var args = "-X PUT " + "'" + url + "'" +  " -H 'Accept:application/json' -H 'Content-Type:application/json' -d " + "'" + row + "'";
        exec('curl ' + args, function(error, stdout, stderr) {
        //console.log('stdout: ' + stdout);
        //console.log('stderr: ' + stderr);
        if (error !== null) {
                console.log('exec error: ' + error);
        }
      });
}
}
/********************************************************************************************
 *
 *
 *                      updateTSTable
 *   Updates the timestamp table with the latest current timestamp
 *
 ********************************************************************************************/
function updateTSTable(){
//Update the timestamp table
console.log('Updating Timestamp in table eap_api_hbase');
connection.query("UPDATE eap_api_last_timestamp SET Last_timestamp = CURRENT_TIMESTAMP WHERE Process_Type='H'", function(err, rows, fields){
        if(err){
        console.log('Error updating timestamp table');
}else{
        console.log('Succesfully updated timestamp table');
                process.exit(0);
}
});
}
/********************************************************************************************
 *
 *
 *                      updateColumn
 *   Updates the mysql table and changes the column hbase_flag to 'Y'
 *
 ********************************************************************************************/
  function updateColumn(timestamp){
  var row_number = timestamp;
//Update the hbase_flag column
console.log('Updating hbase_flag column in mysqltable');
connection.query(`UPDATE alan_test SET hbase_flag = 'Y' WHERE insert_timestamp = ?`,[timestamp], function(err, rows, fields){
        if(err){
        console.log('Error updating hbase_flag in mysql table');
}else{
        console.log('Succesfully updated hbase_flag column');
}
});
}

/********************************************************************************************
 *
 *
 *                      MAIN CODE
 *
 *
 ********************************************************************************************/
 //Connect to database and test connection
connection.connect(function(err){
if(!err) {
   console.log("Connection to database successful.");
   //Execute the getLastTS function
   getLastTS();

} else {
   console.log("MySQL Error on connection to database.  Process will exit.");
   process.exit(0);
}
});





