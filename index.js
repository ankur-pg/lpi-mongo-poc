const express = require('express');
const { MongoClient } = require('mongodb');
const app = express();
app.use(express.json());

const url = 'mongodb://localhost:27017';
const dbName = 'listing_analytics_poc';
let dailyColl, hourlyColl;

const client = new MongoClient(url, { maxPoolSize: 100 });

async function initDB() {
    await client.connect();
    const db = client.db(dbName);
    dailyColl = db.collection('daily_metrics');
    hourlyColl = db.collection('hourly_metrics');
    
    // --- Indexing Strategy ---
    // 1. Daily: District queries are your heavy hitters
    await dailyColl.createIndex({ "metadata.district": 1, date: 1 });
    // 2. Hourly: Optimized for the 30-day heatmap 
    // We add a TTL index to auto-expire data after 30 days (2592000 seconds)
    await hourlyColl.createIndex({ "createdAt": 1 }, { expireAfterSeconds: 2592000 });
    
    console.log("Indexes and TTL set. DB Ready.");
}

app.post('/simulate-event', async (req, res) => {
    const randomId = Math.floor(Math.random() * 100000);
    const listingId = `listing_${randomId}`;
    const dateStr = "2025-12-23"; 
    const currentHour = new Date().getHours();
    const docId = `${listingId}_${dateStr}`;

    // Update 1: Daily Collection (The long-term record)
    const dailyUpdate = dailyColl.updateOne(
        { _id: docId },
        { 
            $inc: { "daily_totals.views": 1 },
            $set: { 
                listingId, 
                date: dateStr,
                "metadata.district": "Petaling Jaya" 
            }
        },
        { upsert: true }
    );

    // Update 2: Hourly Collection (The 30-day rolling record)
    const hourlyUpdate = hourlyColl.updateOne(
        { _id: docId },
        { 
            $inc: { [`hourly.${currentHour}.views`]: 1 },
            $setOnInsert: { createdAt: new Date() }, // Needed for TTL
            $set: { listingId, date: dateStr }
        },
        { upsert: true }
    );

    try {
        // Execute both in parallel to simulate real-world fan-out
        await Promise.all([dailyUpdate, hourlyUpdate]);
        res.status(202).send(); 
    } catch (err) {
        res.status(500).send(err.message);
    }
});

initDB().then(() => app.listen(3000));