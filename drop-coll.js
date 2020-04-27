var mongoConn = new Mongo();
var db = mongoConn.getDB("cougar-prod");

regExp = /site_.+/;
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
            db[name].find({_key : {$exists : true}})
                .forEach
                (
                    function(obj)
                    {
                        printJson(obj);
                        //db[name].delete(obj);
                    }
                );
        }
    );


db.getCollectionNames().forEach(function(name){  db[name].find({_key : {$exists : true}}).forEach( function(obj){ printJson(obj);});});
