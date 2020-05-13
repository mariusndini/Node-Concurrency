const Binance = require('binance-api-node').default;
var fs = require('fs');
const config = require('./config.json');
const snowflake = require('./snowflakeWrapper.js');
var dbConn = null;


var SQL = ` SELECT ENDPROCESSINGDAYSTRING AS DAY, 
            COUNT(ID) - (SUM(IFF(PROCESSINGSTATUS= 'Successful',1,0)) + SUM(IFF(PROCESSINGSTATUS= 'Abandoned',1,0))) AS DIFF,
            SUM(IFF(PROCESSINGSTATUS= 'Successful',1,0)) as SUCCESSFUL,
            IFF(ENDPROCESSINGDAYSTRING= '2020-02-03',1,0) as THISDATE

            FROM DBO.QUEUEITEMS 
            WHERE DAY IS NOT NULL
            GROUP BY 1`;

return snowflake.connect()
.then((dbConnection)=>{
    dbConn = dbConnection;
    return snowflake.runSQL(dbConn, "ALTER SESSION SET QUERY_TAG = LARGE_CONCURRENCY_50;").then((data)=>{
        console.log(Date.now(), data);
    })
})
.then(()=>{
    var promises = [];
    for(i=0; i < 50; i++){
        promises.push(snowflake.runSQL(dbConn, SQL).then((data)=>{
            console.log(Date.now(), data);
        }))   
    }

    return Promise.all( promises );

}).then(()=>{

    return snowflake.runSQL(dbConn, `ALTER WAREHOUSE "CONCURRENCY" SUSPEND;`).then((data)=>{
        console.log(Date.now(), data);
    })
})

    










