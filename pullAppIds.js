var gplay = require('google-play-scraper');


async function pullRank(category, collection, country, numApps) {
    try {
        let result = await gplay.list({ category: category, collection: collection, num: numApps, country: country });

        if (!Array.isArray(result)) {
            console.warn(`No results for Category: ${category}, Collection: ${collection}, Country: ${country}`);
            return [];
        }

        return result.map((item, index) => ({
            crawled_date: new Date().toISOString().split('T')[0],  // Gets the current date in "YYYY-mm-dd" format
            store: 1,
            country: country,
            collection: collection,
            category: category,
            rank: index + 1,  // Assuming the list starts from rank 1
            store_id: item.appId
        }));
    }
    catch (e) {
        console.error(e);
        return [];
    }
}


async function loopLists(categories, collections, country, numApps) {
    // Loop over each keys in categories and collections
    for (const categoryKey in categories) {
        let collectedAppRanks = [];
        const category = gplay.category[categoryKey]
        for (const collectionKey in collections) {

            const collection = gplay.collection[collectionKey]
            const logString = "Category:" + category + ", Collection: " + collection
            console.log(logString)

            let appRanks = await pullRank(category, collection, country, numApps)
            collectedAppRanks = collectedAppRanks.concat(appRanks);

        }
        appendToFile(collectedAppRanks)
    }
}

async function appendToFile(collectedAppRanks) {
    const fs = require('fs');

    // Convert each JSON object to a string and join them with newline characters
    const dataString = collectedAppRanks.map(rank => JSON.stringify(rank)).join('\n');

    // Append the data string to the file with an additional newline at the end
    fs.appendFile('/tmp/googleplay_json.txt', dataString + '\n', (err) => {
        if (err) throw err;
    });

    console.log('Appended %i Ids', collectedAppRanks.length);
}



// Apps pulled per category per collection
var numApps = 500
var country = "us"

async function main() {

    // 54 Categories: GAME_TRIVIA, EVENTS, TRAVEL
    var categories = gplay.category
    // 3 Collections: TOP_FREE, TOP_PAID, GROSSING
    var collections = gplay.collection
    console.log("Starting %i categories and %i collections", Object.keys(categories).length, Object.keys(collections).length)

    // nested for loop for two iterables
    loopLists(categories, collections, country, numApps)


}

main()