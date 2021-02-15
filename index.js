const MongoClient = require("mongodb");
const mongodburl = "mongodb+srv://mongodbcase:inserthere@cluster0-qlvde.gcp.mongodb.net?retryWrites=true&w=majority";
const numwatches = 1;
const numwatchestarget = 20;
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
        await ensureindexes(db);
    }, querytimems);
}).catch((reason) => {
    console.error(reason);
});


async function createIndex(col, name, keypath) {
    return new Promise((resolve, reject) => {
        console.log("Adding index " + name + " to " + col.namespace)
        var res = col.createIndex(keypath, (err, name) => {
            if (err) {
                reject(err);
                return;
            }
            resolve(name);
        })
    });
}
async function ensureindexes(db) {
    const collections = await toArray(db.listCollections());

    for (var i = 0; i < collections.length; i++) {
        const collection = collections[i];
        if (collection.type != "collection") continue;
        const indexes = await db.collection(collection.name).indexes();
        const indexnames = indexes.map(x => x.name);
        if (collection.name.endsWith("_hist")) {
            if (indexnames.indexOf("id_1__version_-1") == -1) {
                await createIndex(db.collection(collection.name), "id_1__version_-1", { "id": 1, "_version": -1 })
            }
        } else {
            switch (collection.name) {
                case "fs.files":
                    if (indexnames.indexOf("metadata.workflow_1") == -1) {
                        await createIndex(db.collection(collection.name), "metadata.workflow_1", { "metadata.workflow": 1 })
                    }
                    break;
                case "workflow":
                    if (indexnames.indexOf("queue_1") == -1) {
                        await createIndex(db.collection(collection.name), "queue_1", { "queue": 1 })
                    }
                    break;
                case "users":
                    if (indexnames.indexOf("workflowid_1") == -1) {
                        await createIndex(db.collection(collection.name), "workflowid_1", { "workflowid": 1 })
                    }
                    if (indexnames.indexOf("_rpaheartbeat_1") == -1) {
                        await createIndex(db.collection(collection.name), "_rpaheartbeat_1", { "_rpaheartbeat": 1 })
                    }
                    if (indexnames.indexOf("name_1") == -1) {
                        await createIndex(db.collection(collection.name), "name_1", { "name": 1 })
                    }
                    if (indexnames.indexOf("_type_1") == -1) {
                        await createIndex(db.collection(collection.name), "_type_1", { "_type": 1 })
                    }
                    if (indexnames.indexOf("_created_1") == -1) {
                        await createIndex(db.collection(collection.name), "_created_1", { "_created": 1 })
                    }
                    break;
                default:
                    if (indexnames.indexOf("_type_1") == -1) {
                        await createIndex(db.collection(collection.name), "_type_1", { "_type": 1 })
                    }
                    if (indexnames.indexOf("_created_1") == -1) {
                        await createIndex(db.collection(collection.name), "_created_1", { "_created": 1 })
                    }
                    break;
            }
        }
    }




}
function toArray(iterator) {
    return new Promise((resolve, reject) => {
        iterator.toArray((err, res) => {
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

async function addwatch(db) {
    // const num = Math.floor(Math.random() * (watches.length));
    // console.log("Adding watch " + num + " watching " + watches[num].collection)
    // const stream = await db.collection(watches[num].collection).watch(watches[num].aggregate);
    // streams.push(stream);
    // const options = { fullDocument: "updateLookup" };
    // stream.on("error", err => { console.log(err); });
    // stream.on("change", next => { console.log("change key: " + next.documentKey); });
    const num = Math.floor(Math.random() * (watches.length));
    console.log("Adding watch " + num + " watching " + watches[num].collection)
    const stream = await db.watch(watches[num].aggregate);
    streams.push(stream);
    const options = { fullDocument: "updateLookup" };
    stream.on("error", err => { console.log(err); });
    stream.on("change", next => { console.log("change key: " + next.documentKey); });
}