const { Client } = require('@elastic/elasticsearch');
const express = require('express');

const app = express();
app.use(express.json());

const esClient = new Client({ node: 'http://localhost:9200' });

app.post('/simulate-es-event', async (req, res) => {
    const randomId = Math.floor(Math.random() * 100000);
    const listingId = `listing_${randomId}`;
    const dateStr = "2025-12-23";
    const currentHour = new Date().getHours();

    try {
        await esClient.update({
            index: 'listing_performance',
            id: `${listingId}_${dateStr}`,
            refresh: false, // CRITICAL: ES is NRT; don't force refresh every write
            body: {
                script: {
                    source: `
                        if (ctx._source.daily_totals == null) { ctx._source.daily_totals = ['views': 0] }
                        ctx._source.daily_totals.views += params.count;
                        if (ctx._source.hourly == null) { ctx._source.hourly = [:] }
                        if (ctx._source.hourly[params.hour] == null) { ctx._source.hourly[params.hour] = ['views': 0] }
                        ctx._source.hourly[params.hour].views += params.count;
                    `,
                    params: { count: 1, hour: currentHour.toString() }
                },
                upsert: {
                    listingId: listingId,
                    date: dateStr,
                    daily_totals: { views: 1 },
                    metadata: { 
                        district: req.body.district || "Petaling Jaya",
                        transactionType: req.body.transactionType || "SALE"
                    }
                }
            }
        });
        res.status(202).send();
    } catch (err) {
        // Log the actual error to your terminal so you can see it
        console.error("ES Error:", err.meta ? err.meta.body : err.message);
        res.status(500).send(err.message);
    }
});

app.listen(3000, () => console.log(`ES Stress Test Server running on port 3000`));