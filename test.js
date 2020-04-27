let mongoConn = new Mongo();
let db = mongoConn.getDB("cougar-record");
let dbResults = db.getSiblingDB("results-prod");

let JTP_UpdatePatron = 7001;
let JTP_RemovePatron = 7004;
let JTP_UpdateEmployee = 7005;
let JTP_UpdateEmployee_V2 = 7680;
let JTP_UpdateCard = 7023;
let JTP_RemoveCard = 7024;
let JTP_UpdatePatronStatus = 7057;
let JTP_UpdateEmployeeStatus = 7356;
let JTP_UpdateCardStatus = 7056;
let JTP_CssConfigItemUpdated = 7137;
let JTP_UpdateCardRating = 7139;
let JTP_UpdatePatronRating = 7140;
let JTP_SiteOpened = 308;
let JTP_SiteClosed = 309;
let JTP_APECardInserted = 550;
let JTP_AwardMegaPoint = 828;
let JTP_CardDetailsRequest = 1250;
let JTP_CardValidateAttendantCardRequest = 1259;
let JTP_CardInserted = 5001;
let JTP_CardInserted_V2 = 5552;
let JTP_CardInserted_V3 = 5553;
let JTP_CSSHostGetBalance = 5220;
let JTP_CSSHostGetBalance_V2 = 5221;
let JTP_CSSGetActualBalance = 5240;
let JTP_CSSGetEstimatedBalance = 5241;
let JTP_CSSAccountTransferRequest = 5245;
let JTP_CSSDebitRequest = 5250;
let JTP_CSSDebitRequest_V2 = 5340;
let JTP_CSSCreditRequest = 5255;
let JTP_CSSCreditRequest_V2 = 5342;
let JTP_CSSCreditDebitRequestCompleted = 5527;
let JTP_CSSTransactionStatus = 5292;
let JTP_CSSTransactionStatus_V2 = 5344;
let JTP_CSSCashTransactionRequest = 5294;
let JTP_CSSCashTransactionStatus = 5297;
let JTP_CSSTransactionCombine = 5299;
let JTP_IsCSSActive = 5132;
let JTP_CSSFlushCardCache = 5263;
let JTP_CSSLogCardCreditSummary = 5274;
let JTP_CSSLogCardCreditSummary_V2 = 5337;
let JTP_CSSHostGetValidationDetails = 5277;
let JTP_CSSHostGetValidationDetails_V2 = 5407;
let JTP_CSSCardCreditSummarySiteLogged = 5534;
let JTP_CBGAccountLastValidationUpdated_V2 = 5424;
let JTP_CSSGetActualBalance_V2 = 5417;
let JTP_PurgeAccountPoints = 5411;
let JTP_CSSLogCardCreditSummary_V3 = 5420;
let JTP_CSSLogCardCreditSummary_V4 = 5443;
let JTP_LogPOSTransactionValue = 5438;
let JTP_SiteDataAdded_V2 = 10765;
let JTP_SiteDataUpdated_V2 = 10767;
let JTP_ConfigItemUpdated = 10481;

// All messages generated in response

let JTP_CSSDebitRequestGranted_V2 = 5341;
let JTP_CBGCardValidated_V2 = 5432;
let JTP_CSSHostCombineTransactionFailed = 5300;
let JTP_CSSInvalidCard = 5212;
let JTP_CSSDebitRequestDenied = 5251;
let JTP_CSSCardCreditSummaryLogged_V2 = 5338;
let JTP_CSSCardCreditSummaryLogged_V3 = 5421;
let JTP_CSSCardCreditSummaryLogged_V4 = 5444;
let JTP_CSSAccountBalance_V2 = 5410;
let JTP_CSSCashTransactionRequestGranted = 5296;
let JTP_CSSHostCombineTransactionSuccessful = 5301;
let JTP_AddCBGAudit = 7457;
let JTP_CSSAccountTransferRequestDenied = 5248;
let JTP_CSSCardValidated_V3 = 5211;
let JTP_PagerGenericMessage = 5302;
let JTP_CSSHostCashTransactionStatus = 5298;
let JTP_CSSCardValidated_V2 = 5406;
let JTP_CSSCardValidatedSpendReward = 5477;
let JTP_CSSHostTransactionStatus = 5293;
let JTP_UpdateCBGAccountExpiryDatetime = 5370;
let JTP_CSSCardValidatedSpend = 5476;
let JTP_CSSCashTransactionRequestDenied = 5295;
let JTP_CheckPatronRating = 5415;
let JTP_PagerGenericMessageOnPlayerRating = 5455;
let JTP_CSSCardValidated = 5210;
let JTP_AddCssEvent = 7109;
let JTP_InvalidatedCard = 5371;
let JTP_CSSAccountBalance = 5244;
let JTP_CSSHostAccountBalance_V3 = 5537;
let JTP_APECardValidated = 565;
let JTP_CSSHostAccountBalance_V2 = 5336;
let JTP_CBGAccountLastValidationUpdated = 5369;
let JTP_APEInvalidCard = 587;
let JTP_CSSDebitRequestGranted = 5252;
let JTP_CSSAccountTransferRequestGranted = 5249;
let JTP_CSSCardValidatedPatronRating = 5454;

let dbNames = [
    "site_1049",
    "site_1107",
    "site_1115",
    "site_1149",
    "site_1150",
    "site_1187",
    "site_1262",
    "site_1308",
    "site_1325",
    "site_1370",
    "site_1374",
    "site_1379",
    "site_140",
    "site_1404",
    "site_1448",
    "site_1534",
    "site_2004",
    "site_2040",
    "site_2060",
    "site_2283",
    "site_22985",
    "site_23001",
    "site_23014",
    "site_23020",
    "site_23022",
    "site_23029",
    "site_2321",
    "site_2334",
    "site_2335",
    "site_2387",
    "site_23998",
    "site_257",
    "site_274",
    "site_295",
    "site_3112",
    "site_347",
    "site_357",
    "site_376",
    "site_385",
    "site_397",
    "site_421",
    "site_466",
    "site_473",
    "site_474",
    "site_533",
    "site_565",
    "site_582",
    "site_618",
    "site_622",
    "site_66",
    "site_705",
    "site_781",
    "site_817",
    "site_822",
    "site_831",
    "site_847",
    "site_850",
    "site_851",
    "site_871",
    "site_939",
    "site_966",
    "site_968",
    "site_976",
        ];

function dropDatabases()
{
    dbNames.forEach
    (
        function(name)
        {
            print("Dropping database - " + name);
            let dbDrop = mongoConn.getDB(name);
            dbDrop.dropDatabase();
        }
    );
}


function traceJTP_CSSDebitRequest_V2(collectionSource, dateStart, dateEnd, dbDestination, mapSiteCollections)
{
    let timestampOffset = 500;
    print("Tracing JTP_CSSDebitRequest_V2");
    let cursor = collectionSource.find({ $and : [{ "_msgID" : JTP_CSSDebitRequest_V2 }, {_timestamp : {$gte : dateStart, $lte: dateEnd }}]});
    let nCount = cursor.count();
    let nIndex = 1;
    cursor.forEach
    (
        function(value)
        {
            let strSiteCollection = "site_" + value._siteID;
            delete value["_id"];
            let timestampFrom = new Date(value._timestamp.getTime() - timestampOffset);
            let listTrace = [];

            collectionSource.find
            (
                {
                    $and :
                    [
                        { "_msgID" : JTP_CSSCashTransactionRequestGranted },
                        { "_siteID" : value._siteID },
                        { "_timestamp" : { $gte : timestampFrom } },
                        { "CardID" : value.CardID },
                        { "TransID" : value.TransID }
                    ]
                }
            )
            .forEach
            (
                function(valGenerated)
                {
                    try
                    {
                        delete valGenerated["_id"];
                        listTrace.push(valGenerated);
                    }
                    catch(exc)
                    {
                        print(exc);
                    }
                }
            );
            collectionSource.find
            (
                {
                    $and :
                    [
                        { "_msgID" : JTP_CSSCashTransactionRequestDenied },
                        { "_siteID" : value._siteID },
                        { "_timestamp" : { $gte : timestampFrom } },
                        { "TransID" : value.TransID }
                    ]
                }
            )
            .forEach
            (
                function(valGenerated)
                {
                    try
                    {
                        delete valGenerated["_id"];
                        listTrace.push(valGenerated);
                    }
                    catch(exc)
                    {
                        print(exc);
                    }
                }
            );
            collectionSource.find
            (
                {
                    $and :
                    [
                        { "_msgID" : JTP_CSSDebitRequestGranted_V2 },
                        { "_siteID" : value._siteID },
                        { "_timestamp" : { $gte : timestampFrom } },
                        { "CardID" : value.CardID },
                        { "TransID" : value.TransID }
                    ]
                }
            )
            .forEach
            (
                function(valGenerated)
                {
                    try
                    {
                        delete valGenerated["_id"];
                        listTrace.push(valGenerated);
                    }
                    catch(exc)
                    {
                        print(exc);
                    }
                }
            );
            collectionSource.find
            (
                {
                    $and :
                    [
                        { "_msgID" : JTP_AddCssEvent },
                        { "_siteID" : value._siteID },
                        { "_timestamp" : { $gte : timestampFrom } },
                        { "CardID" : value.CardID }
                    ]
                }
            )
            .forEach
            (
                function(valGenerated)
                {
                    try
                    {
                        delete valGenerated["_id"];
                        listTrace.push(valGenerated);
                    }
                    catch(exc)
                    {
                        print(exc);
                    }
                }
            );
            collectionSource.find
            (
                {
                    $and :
                    [
                        { "_msgID" : JTP_CSSDebitRequestDenied },
                        { "_siteID" : value._siteID },
                        { "_timestamp" : { $gte : timestampFrom } },
                        { "TransID" : value.TransID }
                    ]
                }
            )
            .forEach
            (
                function(valGenerated)
                {
                    try
                    {
                        delete valGenerated["_id"];
                        listTrace.push(valGenerated);
                    }
                    catch(exc)
                    {
                        print(exc);
                    }
                }
            );

            if(!mapSiteCollections[strSiteCollection])
            {
                dbDestination[strSiteCollection].createIndex({_cardID : 1});
                dbDestination[strSiteCollection].insertOne({_cardID : value.CardID, JTP_CSSDebitRequest_V2 : { msgID : JTP_CSSDebitRequest_V2, input : [] }});
                mapSiteCollections[strSiteCollection] = true;
            }
            else if(dbDestination[strSiteCollection].find({_cardID : value.CardID}).count() == 0)
            {
                dbDestination[strSiteCollection].insertOne({_cardID : value.CardID, JTP_CSSDebitRequest_V2 : { msgID : JTP_CSSDebitRequest_V2, input : [] }});
            }
            else if(dbDestination[strSiteCollection].find({$and : [{_cardID : value.CardID}, { JTP_CSSDebitRequest_V2 : {$exists : true}}]}).count() == 0)
            {
                dbDestination[strSiteCollection].update({_cardID : value.CardID}, { $set : { JTP_CSSDebitRequest_V2 : []}});
            }
            dbDestination[strSiteCollection].update({_cardID : value.CardID}, { $push : { "JTP_CSSDebitRequest_V2.input" : { in : value, trace : listTrace }}});
            if(nIndex % 100 == 0)
            {
                print("Up to " + nIndex + " of " + nCount);
            }
            ++nIndex;
        }
    );
}


let strCollectionName = "messages2020-04-08T22-37";
let dateStart = ISODate("2020-04-08T22:41:43.280Z");
let dateEnd = ISODate("2020-04-09T02:07:40.371Z");
let mapCollections = {};

traceJTP_CSSDebitRequest_V2(db[strCollectionName], dateStart, dateEnd, dbResults, mapCollections);