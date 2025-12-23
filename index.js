const express = require('express');
const { MongoClient } = require('mongodb');
const app = express();
app.use(express.json());

const url = 'mongodb://localhost:27017';
const dbName = 'listing_analytics_poc';
let listingsCollection;

const client = new MongoClient(url, {
    maxPoolSize: 100, // Increased to handle high-concurrency stress
    minPoolSize: 20
});

async function initDB() {
    await client.connect();
    const db = client.db(dbName);
    listingsCollection = db.collection('daily_metrics');
    
    console.log("Setting up high-cardinality indexes...");
    // Essential for your "District/Region" heatmap queries later
    await listingsCollection.createIndex({ listingId: 1, date: 1 }, { unique: true });
    await listingsCollection.createIndex({ "metadata.district": 1, date: 1 });
    console.log("DB Ready.");
}

app.post('/simulate-event', async (req, res) => {
    // HIGH CARDINALITY: Randomly pick one of 100,000 listings
    const randomId = Math.floor(Math.random() * 100000);
    const listingId = `listing_${randomId}`;
    
    // Use a fixed date for the POC so we build up a large volume for one day
    const dateStr = "2025-12-23"; 
    const currentHour = new Date().getHours();

    const filter = { _id: `${listingId}_${dateStr}` };
    const update = {
        $inc: {
            [`hourly.${currentHour}.views`]: 1,
            "daily_totals.views": 1
        },
        $set: {
            listingId: listingId,
            agentId: `agent_${Math.floor(randomId / 5)}`, // Simulate 1 agent per 5 listings
            date: dateStr,
            "metadata.district": req.body.district || "Petaling Jaya",
            "metadata.transactionType": req.body.transactionType || "SALE"
        }
    };

    try {
        await listingsCollection.updateOne(filter, update, { upsert: true });
        res.status(202).send(); 
    } catch (err) {
        res.status(500).send(err.message);
    }
});

initDB().then(() => {
    app.listen(3000, () => console.log(`Stress Test Server on :3000`));
});
