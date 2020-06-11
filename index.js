const Binance = require('binance-api-node').default;
var fs = require('fs');
const config = require('./config.json');
const snowflake = require('./snowflakeWrapper.js');
var dbConn = null;

// change this var to the number times you want to run quiries
var numberOfTimesToRunQuiries = 50;

// list of your quiries
var SQL = [` select * from "ANONDB"."CCM"."TBLSMUSER"; `, 
          ` select 'a' ` ,
          ` select 'b' ` ,
          ` select 'c' ` ];

// ---------------------------------------------------------

console.log('START', new Date());
return snowflake.connect()
.then((dbConnection)=>{
    dbConn = dbConnection;
    console.log('CONNECT', new Date());
    return snowflake.runSQL(dbConn, "ALTER SESSION SET QUERY_TAG = ED_CONCURENCY;").then((data)=>{
        console.log(Date.now(), data);
    })
})
.then(()=>{
    console.log('CONNECTED', new Date());
    var promises = [];
    for(i=0; i < numberOfTimesToRunQuiries; i++){
        var queryID = i % SQL.length;
        promises.push(snowflake.runSQL(dbConn, SQL[queryID]).then((data)=>{
            console.log(Date.now(), data);
        }))   
    }
    //console.log('RUNQUERY', new Date());
    return Promise.all( promises );

}).then(()=>{
    return snowflake.runSQL(dbConn, `ALTER WAREHOUSE ${config.snowflake.warehouse} SUSPEND;`).then((data)=>{
        console.log('DONE', new Date());
        console.log(Date.now(), data);
    })
})

    










