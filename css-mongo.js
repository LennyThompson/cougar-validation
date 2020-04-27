let mongoConn = new Mongo();
let db = mongoConn.getDB("cougar-record");
let dbResults = db.getSiblingDB("results-prod");

load("css-messages.js");
load("JTP_CSSDebitRequest_V2.js");
load("JTP_CSSCreditRequest_V2.js");
load("message-CSSCreditDebitRequestCompleted.js")
load("message-CSSTransactionStatus.js")
load("message-CSSTransactionStatus_V2.js")

let strCollectionName = "messages2020-04-08T22-37";
let dateStart = ISODate("2020-04-08T22:41:43.280Z");
let dateEnd = ISODate("2020-04-09T02:07:40.371Z");

let mapCollections = {};
dbResults.getCollectionNames().forEach
(
    function(name)
    {
        mapCollections[name] = true;
    }
);

// traceJTP_CSSDebitRequest_V2(db[strCollectionName], dateStart, dateEnd, dbResults, mapCollections);
// traceJTP_CSSCreditRequest_V2(db[strCollectionName], dateStart, dateEnd, dbResults, mapCollections);

// traceJTP_CSSCreditDebitRequestCompleted(db[strCollectionName], dateStart, dateEnd, dbResults, mapCollections);
traceJTP_CSSTransactionStatus(db[strCollectionName], dateStart, dateEnd, dbResults, mapCollections);
traceJTP_CSSTransactionStatus_V2(db[strCollectionName], dateStart, dateEnd, dbResults, mapCollections);
