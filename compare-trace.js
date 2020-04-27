load("css-messages.js");

let mongoConn = new Mongo();
let db = mongoConn.getDB("cougar-record");
let dbProdResults = db.getSiblingDB("results-prod");
let dbMTResults = db.getSiblingDB("results-css-multi");

function compareJTP_CSSDebitRequest_V2(cardTraceProd, cardTraceMulti, strCardId)
{
    print("  " + strCardId + " JTP_CSSDebitRequest_V2 messages");
    if(cardTraceProd.JTP_CSSDebitRequest_V2 && cardTraceMulti.JTP_CSSDebitRequest_V2)
    {
        if(cardTraceProd.JTP_CSSDebitRequest_V2.input.length == cardTraceMulti.JTP_CSSDebitRequest_V2.input.length)
        {
            if(cardTraceProd.JTP_CSSDebitRequest_V2.input.length == 0)
            {
                print("   " + strCardId + " has same no JTP_CSSDebitRequest_V2 input messages????");
            }
            else
            {
                cardTraceProd.JTP_CSSDebitRequest_V2.input
                .forEach
                (
                    function(traceProd)
                    {
                        print("   " + strCardId + " searching for equivalent transaction " + traceProd.in.TransID);
                        traceMulti = cardTraceMulti.JTP_CSSDebitRequest_V2.input
                        .find
                        (
                            function(traceMsg)
                            {
                                return traceProd.in.TransID == traceMsg.in.TransID;
                            }

                        );
                        if(traceMulti)
                        {
                            print("   " + strCardId + " has equivalent transaction " + traceProd.in.TransID);
                        }
                        else
                        {
                            print("   " + strCardId + " **** ERROR **** has no equivalent transaction " + traceProd.in.TransID);
                        }
                    }
                );
            }
        }
        else
        {
            print("   " + strCardId + " **** ERROR **** different number of JTP_CSSDebitRequest_V2 input messages");
        }
    }
    else
    {
        if(!cardTraceProd.JTP_CSSDebitRequest_V2)
        {
            print(" " + strCardId + " ^^^^ INFO ^^^^ card has no JTP_CSSDebitRequest_V2 input messages");
        }
        else
        {
            print("   " + strCardId + " **** ERROR **** different JTP_CSSDebitRequest_V2 input messages recorded (prod has none)");
        }
    }
}

function compareJTP_CSSCreditRequest_V2(cardTraceProd, cardTraceMulti, strCardId)
{
    print("  " + strCardId + " JTP_CSSCreditRequest_V2 messages");
    if(cardTraceProd.JTP_CSSCreditRequest_V2 && cardTraceMulti.JTP_CSSCreditRequest_V2)
    {
        if(cardTraceProd.JTP_CSSCreditRequest_V2.input.length == cardTraceMulti.JTP_CSSCreditRequest_V2.input.length)
        {
            if(cardTraceProd.JTP_CSSCreditRequest_V2.input.length == 0)
            {
                print("   " + strCardId + " has same no JTP_CSSCreditRequest_V2 input messages????");
            }
            else
            {
                cardTraceProd.JTP_CSSCreditRequest_V2.input
                .forEach
                (
                    function(traceProd)
                    {
                        print("   " + strCardId + " searching for equivalent transaction " + traceProd.in.TransID);
                        traceMulti = cardTraceMulti.JTP_CSSCreditRequest_V2.input
                        .find
                        (
                            function(traceMsg)
                            {
                                return traceProd.in.TransID == traceMsg.in.TransID;
                            }

                        );
                        if(traceMulti)
                        {
                            print("   " + strCardId + " has equivalent transaction " + traceProd.in.TransID);
                        }
                        else
                        {
                            print("   " + strCardId + " **** ERROR **** has no equivalent transaction " + traceProd.in.TransID);
                        }
                    }
                );
            }
        }
        else
        {
            print("   " + strCardId + " **** ERROR **** different number of JTP_CSSCreditRequest_V2 input messages");
        }
    }
    else
    {
        if(!cardTraceProd.JTP_CSSCreditRequest_V2)
        {
            print(" " + strCardId + " ^^^^ INFO ^^^^ card has no JTP_CSSCreditRequest_V2 input messages");
        }
        else
        {
            print("   " + strCardId + " **** ERROR **** different JTP_CSSCreditRequest_V2 input messages recorded (prod has none)");
        }
    }
}

dbProdResults.getCollectionNames()
.forEach
(
    function(siteCollection)
    {
        print("Collection: " + siteCollection);
        print();
        let collection = dbProdResults[siteCollection];
        let collectionMulti = dbMTResults[siteCollection];

        collection.find({})
        .forEach
        (
            function(cardTraceProd)
            {
                let strCardId = "Card ID: " + cardTraceProd._cardID;
                print(strCardId);
                let cardTraceMulti = collectionMulti.findOne({_cardID : cardTraceProd._cardID});
                if(cardTraceMulti)
                {
                    compareJTP_CSSDebitRequest_V2(cardTraceProd, cardTraceMulti, strCardId);
                    compareJTP_CSSCreditRequest_V2(cardTraceProd, cardTraceMulti, strCardId);
                }
                else
                {
                    print("  " + strCardId + " **** ERROR **** no card to compare to ****");
                }
            }
        );
        print();
    }
);
