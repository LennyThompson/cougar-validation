let mongoConn = new Mongo();
let db = mongoConn.getDB("cougar-record");
let dbResults = db.getSiblingDB("results-css-multi");

load("css-messages.js");
load("JTP_CSSDebitRequest_V2.js");
load("JTP_CSSCreditRequest_V2.js");


let strCollectionName = "messages2020-04-07T23-46";
let dateStart = ISODate("2020-04-07T23:51:32.988Z");
let dateEnd = ISODate("2020-04-08T03:17:30.078Z");

let mapCollections = {};
dbResults.getCollectionNames().forEach
(
    function(name)
    {
        mapCollections[name] = true;
    }
);

traceJTP_CSSDebitRequest_V2(db[strCollectionName], dateStart, dateEnd, dbResults, mapCollections);
traceJTP_CSSCreditRequest_V2(db[strCollectionName], dateStart, dateEnd, dbResults, mapCollections);