// Import the Google Cloud client library
const credentials= require('./key.json');
const {BigQuery} = require('@google-cloud/bigquery');
const cron = require("node-cron");
const fs = require("fs");
const express = require('express');
const app = express();

//data variables
let newUsers= {};
let screenFlow= {};
let totalAccounts= {};
let conversionRate= {};

//dates variables
let nowDate;
let pastDate;
let period;

cron.schedule("0 0 0 * * *", function() {
    newUsers= {};
    screenFlow= {};
    totalAccounts= {};
    conversionRate= {};
    //retrieve data
    newUsers= queryNewUsers('month');
    screenFlow= queryScreenFlow('month');
    totalAccounts= queryTotalAccounts('month');
    conversionRate= queryConversionRate('month');
});

app.set('view engine', 'ejs');
app.listen(process.env.PORT);

app.use(function (req, res, next) {

    // Website you wish to allow to connect
    res.header('Access-Control-Allow-Origin', 'http://localhost:4200');

    // Request methods you wish to allow
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');

    // Request headers you wish to allow
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
    res.header('Access-Control-Allow-Credentials', true);

    next();
});

const bigQueryClient = new BigQuery(({projectId: 'empyrean-yeti-244822',credentials: credentials})
);

const asyncMiddleware = fn =>
    (req, res, next) => {
        Promise.resolve(fn(req, res, next))
            .catch(next);
    };

cron.schedule("0 0 0 * * *", asyncMiddleware(async (req, res, next) => {
    newUsers= {};
    screenFlow= {};
    totalAccounts= {};
    conversionRate= {};
    //retrieve data
    newUsers= await queryNewUsers('month');
    screenFlow= await queryScreenFlow('month');
    totalAccounts= await queryTotalAccounts('month');
    conversionRate= await queryConversionRate('month');
}));

const getNow= () => {
    let now = new Date();
    let month = now.getUTCMonth() + 1 < 10? '0' + (now.getUTCMonth() + 1): now.getUTCMonth() + 1;
    let day = now.getUTCDate() < 10? '0' + now.getUTCDate() : now.getUTCDate();
    let year = now.getUTCFullYear();
    let formatNow= year + '' + month + '' + day;
    nowDate= now;
    return formatNow;
}

const getPast= (period) => {
    let formatPast;
    let d = new Date();
    let past = d.getMonth();

    switch (period){
        case 'month':

            d.setMonth(d.getMonth() - 1);
            if (d.getMonth() === past) d.setDate(0);
            d.setHours(0, 0, 0);
            d.setMilliseconds(0);
            let monthPast = d.getUTCMonth() + 1 < 10? '0' + (d.getUTCMonth() + 1) : d.getUTCMonth() + 1;
            let dayPast = d.getUTCDate() < 10? '0' + d.getUTCDate() : d.getUTCDate();
            let yearPast = d.getUTCFullYear();
            formatPast= yearPast + '' + monthPast + '' + dayPast;
            break;
        case 'week':
            d.setDate(d.getDate() - 7);
            if (d.getMonth() === past) d.setDate(0);
            d.setHours(0, 0, 0);
            d.setMilliseconds(0);
            let monthPastW = d.getUTCMonth() + 1 < 10? '0' + (d.getUTCMonth() + 1) : d.getUTCMonth() + 1;
            let dayPastW = d.getUTCDate() < 10? '0' + d.getUTCDate() : d.getUTCDate();
            let yearPastW = d.getUTCFullYear();
            formatPast= yearPastW + '' + monthPastW + '' + dayPastW;
            break;
    }

    pastDate= d;

    return formatPast;
}

const insert= (str) => {
    let p1 = str.slice(0, 4);
    let p2 = str.slice(4, 6);
    let p3 = str.slice(6);
    return p1 + '/' + p2 + '/' + p3;
}

const getDateArray = function(start, end) {
    let arr = new Array();
    let dt = new Date(start);
    while (dt <= end) {
        let d= new Date(dt);
        let monthPast = d.getUTCMonth() + 1 < 10? '0' + (d.getUTCMonth() + 1) : d.getUTCMonth() + 1;
        let dayPast = d.getUTCDate() < 10? '0' + d.getUTCDate() : d.getUTCDate();
        let yearPast = d.getUTCFullYear();
        let formatDate= yearPast + '/' + monthPast + '/' + dayPast;
        arr.push(formatDate);
        dt.setDate(dt.getDate() + 1);
    }
    return arr;
}


const queryNewUsers= async (period, startPeriod, endPeriod) =>{
    let formatedDate= {};

    const query = `select event_date as date, COUNT(DISTINCT user_id) as count from \`empyrean-yeti-244822.analytics_204213165.events_*\`, UNNEST(event_params) AS param, UNNEST(event_params) AS param2 where (_TABLE_SUFFIX BETWEEN @past AND @now) and event_name='user_engagement' AND param.key = "firebase_screen" and param2.value.string_value = "ProfileInit" and user_id is not null group by event_date ORDER BY
  event_date ASC;`

    const options = {
        query: query,
        // Location must match that of the dataset(s) referenced in the query.
        location: 'US',
        params: {past: getPast(period), now: getNow()},
    };

    // Run the query as a job
    const [job] = await bigQueryClient.createQueryJob(options);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();



    getDateArray(pastDate, nowDate).forEach(key=> {
        formatedDate[key]= 0;
    });

    rows.forEach(value=> {
        let dateFormat= insert(value.date);
        formatedDate[dateFormat]= value.count;
    });

    return formatedDate;
}

const queryScreenFlow= async (period, startPeriod, endPeriod) =>{
    let totalEngagement= 0;
    let screenName= {};

    const query = `select param.value.string_value as screen, param2.value.int_value as time from \`empyrean-yeti-244822.analytics_204213165.events_*\`, UNNEST(event_params) AS param, UNNEST(event_params) AS param2 where event_name='user_engagement' AND param.key = "firebase_screen" and param2.key = "engagement_time_msec" and (_TABLE_SUFFIX BETWEEN @past AND @now) and user_id IS NOT NULL  ORDER BY screen ASC;`

    const options = {
        query: query,
        // Location must match that of the dataset(s) referenced in the query.
        location: 'US',
        params: {past: getPast(period), now: getNow()},
    };

    // Run the query as a job
    const [job] = await bigQueryClient.createQueryJob(options);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();


    rows.forEach((object, index)=> {
        if(!screenName[object.screen]){
            screenName[object.screen]=[];
        }

        switch(screenName[object.screen]){
            case 'RegisterOptions':
                screenName['Register'].push(object.time);
                break;
            default:
                screenName[object.screen].push(object.time);
                break
        }
    });

    Object.entries(screenName).forEach(
        ([key, value]) => {
            let sum= 0;
            value.forEach((val)=> {
                sum+=val;
            });

            const average= Math.round(sum/(value.length*1000));
            totalEngagement+= average;

            screenName[key]={average: average};

        }
    );

    Object.entries(screenName).forEach(
        ([key, value]) => {
            let percentage= Math.round(screenName[key].average/ totalEngagement *100);;

            screenName[key].percentage= percentage;

        }
    );

    console.log(screenName);

    return screenName;
};

const queryTotalAccounts= async (period, startPeriod, endPeriod) =>{
    let formatedDate= {};
    let totals= 0;

    const query = `select event_date as date, COUNT(DISTINCT user_id) as count from \`empyrean-yeti-244822.analytics_204213165.events_*\`, UNNEST(event_params) AS param, UNNEST(event_params) AS param2 where (_TABLE_SUFFIX BETWEEN @past AND @now) and event_name='user_engagement' AND param.key = "firebase_screen" and param2.value.string_value = "ProfileInit" and user_id is not null group by event_date ORDER BY
  event_date ASC;`

    const options = {
        query: query,
        // Location must match that of the dataset(s) referenced in the query.
        location: 'US',
        params: {past: getPast(period), now: getNow()},
    };

    // Run the query as a job
    const [job] = await bigQueryClient.createQueryJob(options);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();

    const queryTotal = `select event_date as date, COUNT(DISTINCT user_id) as count from \`empyrean-yeti-244822.analytics_204213165.events_*\`, UNNEST(event_params) AS param, UNNEST(event_params) AS param2 where (_TABLE_SUFFIX BETWEEN @past AND @now) and event_name='user_engagement' AND param.key = "firebase_screen" and param2.value.string_value = "ProfileInit" and user_id is not null group by event_date ORDER BY
  event_date ASC;`

    const optionsTotal = {
        query: queryTotal,
        // Location must match that of the dataset(s) referenced in the query.
        location: 'US',
        params: {past: '20190830', now: getPast(period)},
    };

    // Run the query as a job
    const [jobTotal] = await bigQueryClient.createQueryJob(optionsTotal);

    // Wait for the query to finish
    const [rowsTotal] = await jobTotal.getQueryResults();

    rowsTotal.forEach(value=> {
        totals+= value.count;
    });

    getDateArray(pastDate, nowDate).forEach(key=> {
        formatedDate[key]= 0;
    });

    rows.forEach(value=> {
        let dateFormat= insert(value.date);
        formatedDate[dateFormat]= value.count;
    });

    Object.entries(formatedDate).forEach(
        ([key, value]) => {
            totals+= value;
            formatedDate[key]= totals;
        }
    );

    return formatedDate;
};

const queryConversionRate= async (period, startPeriod, endPeriod) =>{
    let formatedDate= {};
    let formatedDate2= {};

    const query = `select event_date as date, COUNT(DISTINCT user_pseudo_id) as count from \`empyrean-yeti-244822.analytics_204213165.events_*\`, UNNEST(event_params) AS param, UNNEST(event_params) AS param2 where (_TABLE_SUFFIX BETWEEN @past AND @now) and event_name='first_open' group by event_date ORDER BY
  event_date ASC;`

    const options = {
        query: query,
        // Location must match that of the dataset(s) referenced in the query.
        location: 'US',
        params: {past: getPast(period), now: getNow()},
    };

    // Run the query as a job
    const [job] = await bigQueryClient.createQueryJob(options);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();

    const query2 = `select event_date as date, COUNT(DISTINCT user_id) as number from \`empyrean-yeti-244822.analytics_204213165.events_*\`, UNNEST(event_params) AS param, UNNEST(event_params) AS param2 where param.key = "firebase_screen" and param2.value.string_value = "ChallengeComplete" and (_TABLE_SUFFIX BETWEEN @past AND @now) and user_id IS NOT NULL  group by event_date ORDER BY event_date ASC;`

    const options2 = {
        query: query2,
        // Location must match that of the dataset(s) referenced in the query.
        location: 'US',
        params: {past: getPast(period), now: getNow()},
    };

    // Run the query as a job
    const [job2] = await bigQueryClient.createQueryJob(options2);

    // Wait for the query to finish
    const [rows2] = await job2.getQueryResults();

    getDateArray(pastDate, nowDate).forEach(key=> {
        formatedDate[key]= 0;
    });

    rows2.forEach(value=> {
        let dateFormat= insert(value.date);
        formatedDate2[dateFormat]= value.count;
    });

    rows.forEach(value=> {
        let dateFormat= insert(value.date);
        formatedDate[dateFormat]= Math.round(formatedDate2[dateFormat]/value.count*100);
    });

    return formatedDate;
};



app.get('/newusers', asyncMiddleware(async (req, res, next) => {
    let data;
    if((Object.entries(newUsers).length === 0 && newUsers.constructor === Object) || req.query.period !== period) {
        data = await queryNewUsers(req.query.period);
        newUsers= data;
    } else {
        data = newUsers;
    }

    period= req.query.period;

    res.json(data);
}));

app.get('/screenflow', asyncMiddleware(async (req, res, next) => {
    let data;

    if((Object.entries(screenFlow).length === 0 && screenFlow.constructor === Object) || req.query.period !== period) {
        data = await queryScreenFlow(req.query.period);
        screenFlow= data;
    } else {
        data = screenFlow;
    }

    period= req.query.period;

    res.json(data);
}));

app.get('/totalaccounts', asyncMiddleware(async (req, res, next) => {
    let data;

    if((Object.entries(totalAccounts).length === 0 && totalAccounts.constructor === Object) || req.query.period !== period) {
        data = await queryTotalAccounts(req.query.period);
        totalAccounts= data;
    } else {
        data = totalAccounts;
    }

    period= req.query.period;

    res.json(data);
}));

app.get('/conversionrate', asyncMiddleware(async (req, res, next) => {
    let data;

    if((Object.entries(conversionRate).length === 0 && conversionRate.constructor === Object) || req.query.period !== period) {
        data = await queryConversionRate(req.query.period);
        conversionRate= data;
    } else {
        data = conversionRate;
    }

    period= req.query.period;

    res.json(data);
}));
