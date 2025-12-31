const { MongoClient } = require('mongodb');

async function seedPOC() {
    const client = new MongoClient('mongodb://localhost:27017');
    await client.connect();
    const db = client.db('listing_analytics_poc');
    const dailyColl = db.collection('daily_metrics');
    const hourlyColl = db.collection('hourly_metrics');

    const TOTAL_LISTINGS = 100000;
    const DAYS_TO_SEED = 30;
    const BATCH_SIZE = 5000;

    const districts = ["Petaling Jaya", "Shah Alam", "Subang Jaya", "Kuala Lumpur", "Cheras"];

    console.log(`Starting seed: ${TOTAL_LISTINGS} listings over ${DAYS_TO_SEED} days...`);

    for (let d = 0; d < DAYS_TO_SEED; d++) {
        let dailyBatch = [];
        let hourlyBatch = [];
        
        // Generate a date for each day in the past
        const date = new Date();
        date.setDate(date.getDate() - d);
        const dateStr = date.toISOString().split('T')[0];

        for (let i = 0; i < TOTAL_LISTINGS; i++) {
            const listingId = `listing_${i}`;
            const district = districts[i % districts.length];
            const views = Math.floor(Math.random() * 50);

            // Daily Doc
            dailyBatch.push({
                _id: `${listingId}_${dateStr}`,
                listingId,
                date: dateStr,
                daily_totals: { views },
                metadata: { district, transactionType: "SALE" }
            });

            // Hourly Doc (Heatmap)
            const randomHour = Math.floor(Math.random() * 24);
            hourlyBatch.push({
                _id: `${listingId}_${dateStr}`,
                listingId,
                date: dateStr,
                hourly: { [randomHour]: { views } },
                createdAt: new Date()
            });

            // Execute batch insert to keep memory usage low
            if (dailyBatch.length >= BATCH_SIZE) {
                await dailyColl.insertMany(dailyBatch, { ordered: false });
                await hourlyColl.insertMany(hourlyBatch, { ordered: false });
                dailyBatch = [];
                hourlyBatch = [];
                console.log(`Inserted ${i + 1} listings for ${dateStr}`);
            }
        }
    }
    console.log("Seeding complete!");
    process.exit();
}

seedPOC();
