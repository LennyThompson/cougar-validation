var mongoConn = new Mongo();
var db = mongoConn.getDB("cougar-record");

regExp = /Card_.+/;
db.getCollectionNames()
    .filter
    (
        function(name)
        {
            return name.match(regExp);
        }
    )
    .forEach
    (
        function(name)
        {
            db.getCollection(name).drop();
        }
    );

