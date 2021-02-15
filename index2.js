const MongoClient = require("mongodb");
const mongodburl = "mongodb+srv://mongodbcase:inserthere@cluster0-qlvde.gcp.mongodb.net?retryWrites=true&w=majority";
const numwatches = 2;
const numwatchestarget = 2;
const querytimems = 1000;
const collectionname = "entities";
let isQuerying = false;
let streams = [];
const watches = [
    { collection: "entities", aggregate: [{ "$match": { "fullDocument._type": "test" } }] },
    { collection: "users", aggregate: [{ "$match": { "fullDocument._type": "user" } }] },
    { collection: "users", aggregate: [{ "$match": { "fullDocument._type": "role" } }] },
    { collection: "audit", aggregate: [{ "$match": { "fullDocument._type": "jwtsignin" } }] }
]
MongoClient.connect(mongodburl, { autoReconnect: false, useNewUrlParser: true, useUnifiedTopology: true }).then(async (cli) => {
    const db = cli.db("demo5");
    console.error("Connected to mongodb");
    for (let i = 0; i < numwatches; i++) {
        await addwatch(db);
    }
    setInterval(async () => {
        if (isQuerying) return;
        isQuerying = true;
        try {
            var hrstart = process.hrtime();
            const result = arr = await db.collection(collectionname).find({ "_type": "test" }).limit(50).toArray();
            hrend = process.hrtime(hrstart)
            console.info('Execution time (hr): %ds %dms with ' + result.length + ' results and ' + streams.length + " watches", hrend[0], hrend[1] / 1000000)
            if (numwatchestarget > streams.length) await addwatch(db);
        } catch (error) {
            console.error(error);
        }
        isQuerying = false;
    }, querytimems);
}).catch((reason) => {
    console.error(reason);
});

async function addwatch(db) {
    const num = Math.floor(Math.random() * (watches.length));
    console.log("Adding watch " + num + " watching " + watches[num].collection)
    const stream = await db.collection(watches[num].collection).watch(watches[num].aggregate);
    streams.push(stream);
    const options = { fullDocument: "updateLookup" };
    stream.on("error", err => { console.log(err); });
    stream.on("change", next => { console.log("change key: " + next.documentKey); });
}