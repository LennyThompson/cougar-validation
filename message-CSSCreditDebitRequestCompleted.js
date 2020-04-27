
// To run this stand alone uncomment this line.
// load("CSS_Host-messages.js");

function traceJTP_CSSCreditDebitRequestCompleted(collectionSource, dateStart, dateEnd, dbDestination, mapSiteCollections)
{
    let timestampOffset = 500;
    print("Tracing JTP_CSSCreditDebitRequestCompleted");
    let cursor = collectionSource.find({ $and : [{ "_msgID" : JTP_CSSCreditDebitRequestCompleted }, {_timestamp : {$gte : dateStart, $lte: dateEnd }}]});
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
                        { "_msgID" : JTP_CheckPatronRating },
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
            collectionSource.find
            (
                {
                    $and :
                    [
                        { "_msgID" : JTP_CSSAccountBalance },
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
                        { "_msgID" : JTP_CSSCreditRequestDenied },
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
                        { "_msgID" : JTP_CSSCreditDebitRequestCompleted },
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

            let emptyDoc = { msgID : JTP_CSSCreditDebitRequestCompleted, input : [] };
            if(!mapSiteCollections[strSiteCollection])
            {
                dbDestination[strSiteCollection].createIndex({_cardID : 1});
                dbDestination[strSiteCollection].insertOne({_cardID : value.CardID,  JTP_CSSCreditDebitRequestCompleted : emptyDoc });
                mapSiteCollections[strSiteCollection] = true;
            }
            else if(dbDestination[strSiteCollection].find({_cardID : value.CardID}).count() == 0)
            {
                dbDestination[strSiteCollection].insertOne({_cardID : value.CardID, JTP_CSSCreditDebitRequestCompleted : emptyDoc });
            }
            else if(dbDestination[strSiteCollection].find({$and : [{_cardID : value.CardID}, { JTP_CSSCreditDebitRequestCompleted : {$exists : true}}]}).count() == 0)
            {
                dbDestination[strSiteCollection].update({_cardID : value.CardID}, { $set : { JTP_CSSCreditDebitRequestCompleted : emptyDoc }});
            }
            dbDestination[strSiteCollection].update({_cardID : value.CardID}, { $push : { "JTP_CSSCreditDebitRequestCompleted.input" : { in : value, trace : listTrace }}});
            if(nIndex % 100 == 0)
            {
                print("Up to " + nIndex + " of " + nCount);
            }
            ++nIndex;
        }
    );
}
