const yargs = require('yargs/yargs');
const fs = require('fs');
const path = require('path');

// Ensure the log directory exists
const logDir = path.join(process.env.HOME, '.config/adscrawler/logs');
if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
}

// Create a write stream for logging
const logFile = path.join(logDir, 'pulljs.log');
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

// Override console methods
console.log = function (message, ...optionalParams) {
    logStream.write(`[LOG ${new Date().toISOString()}] ${message} ${optionalParams.join(' ')}\n`);
    process.stdout.write(`[LOG ${new Date().toISOString()}] ${message} ${optionalParams.join(' ')}\n`);
};

console.error = function (message, ...optionalParams) {
    logStream.write(`[ERROR ${new Date().toISOString()}] ${message} ${optionalParams.join(' ')}\n`);
    process.stderr.write(`[ERROR ${new Date().toISOString()}] ${message} ${optionalParams.join(' ')}\n`);
};

console.warn = function (message, ...optionalParams) {
    logStream.write(`[WARN ${new Date().toISOString()}] ${message} ${optionalParams.join(' ')}\n`);
    process.stderr.write(`[WARN ${new Date().toISOString()}] ${message} ${optionalParams.join(' ')}\n`);
};

(async () => {
    const gplay = await import('google-play-scraper');


const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
    .option('developers', {
        alias: 'd',
        type: 'boolean',
        description: 'Set to true or false to include developers'
    })
    .option('country', {
        alias: 'c',
        type: 'string',
        default: 'us',
        description: 'Set the country to pull from'
    })
    .argv;

async function pullRank(category, collection, country, numApps) {
    try {
        let result = await gplay.default.list({ category: category, collection: collection, num: numApps, country: country });

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

async function loopDevelopers(country, numApps) {
    // Read the file synchronously
    const fileContent = fs.readFileSync('/tmp/googleplay_developers.txt', 'utf8');

    // Split the content by newlines to get an array of developer IDs
    const developerIds = fileContent.split('\n').filter(Boolean);

    let allAppIds = [];

    // Loop over the list of developer IDs
    for (const devId of developerIds) {
        try {
            console.info(`devId=${devId}: start`);
            const apps = await gplay.default.developer({ devId: devId, country: country, num: numApps });

            // Extract the appId from each app and add it to the allAppIds array
            const appIds = apps.map(app => app.appId);
            if (appIds.length > 1) {
                allAppIds = allAppIds.concat(appIds);
                console.info(`devId=${devId}: added ${appIds.length}`);
            }
            else {

            console.info(`devId=${devId}: no appIds found`);
            }
        } catch (error) {
            console.error(`Error fetching apps for developer ${devId}:`, error);
        }
    }
    if (allAppIds.length > 0) {
        // Save the list of appIds to a file separated by newlines
        fs.writeFileSync('/tmp/googleplay_developers_app_ids.txt', allAppIds.join('\n'));

    }
}

async function loopLists(categories, collections, country, numApps) {
    // Loop over each keys in categories and collections
    for (const categoryKey in categories) {
            let collectedAppRanks = [];
            const category = categories[categoryKey]
            for (const collectionKey in collections) {

                const collection = collections[collectionKey]
                const logString = "Category:" + category + ", Collection: " + collection
                console.log(logString)

                let appRanks = await pullRank(category, collection, country, numApps)
                collectedAppRanks = collectedAppRanks.concat(appRanks);

            }
            appendToFile(collectedAppRanks, country)
        }
}

async function appendToFile(collectedAppRanks, country) {
    const fs = require('fs');

    // Convert each JSON object to a string and join them with newline characters
    const dataString = collectedAppRanks.map(rank => JSON.stringify(rank)).join('\n');

    // Append the data string to the file with an additional newline at the end
    fs.appendFile('/tmp/googleplay_json_' + country + '.txt', dataString + '\n', (err) => {
        if (err) throw err;
    });

    console.log('Appended %i Ids', collectedAppRanks.length);
}



// Apps pulled per category per collection
var numApps = 500

async function main() {


    if (argv.developers) {
        loopDevelopers(argv.country, numApps = 60)
    }
    else {
        var categories = gplay.default.category;
        var collections = gplay.default.collection;
        var country = argv.country;
        // 54 Categories: GAME_TRIVIA, EVENTS, TRAVEL
        // var categories = gplay.category
        // 3 Collections: TOP_FREE, TOP_PAID, GROSSING
        // var collections = gplay.collection
        console.log(`Starting country=${country}, ${Object.keys(categories).length} categories and ${Object.keys(collections).length} collections`)

        // nested for loop for two iterables
        loopLists(categories, collections, country, numApps)
    }

}

main()

})();
