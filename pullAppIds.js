var gplay = require('google-play-scraper');

async function pullList(category, collection) {
    // Pull a list of Apps and limited details for a specific category and collection
    try {
        //var category = 'GAME_ADVENTURE'
        //var collection = 'TOP_FREE'
        //numApps = 2
        let result = await gplay.list({ category: category, collection: collection, num: numApps });
        let newAppIds = result.map(item => item.appId)
        return newAppIds
    }
    catch (e) {
        console.error(e)
        return []
    }
}

async function loopLists(categories, collections) {
    // Loop over each keys in categories and collections
    for (const categoryKey in categories) {
        let collectedAppIds = new Set();
        const category = gplay.category[categoryKey]
        for (const collectionKey in collections) {

            const collection = gplay.collection[collectionKey]
            const logString = "Category:" + category + ", Collection: " + collection

            console.log(logString)

            let newAppIds = await pullList(category, collection, collectedAppIds)

            let originalSetSize = collectedAppIds.size
            collectedAppIds = new Set([...collectedAppIds, ...newAppIds])
            let newSetSize = collectedAppIds.size - originalSetSize

            console.log(logString + " pulled %i apps, %i new", newAppIds.length, newSetSize)

        }
        appendToFile(collectedAppIds)
    }
}
async function appendToFile(collectedAppIds) {

    // Change Set to Array to use .join
    let collectedAppIdsArray = Array.from(collectedAppIds)

    // Requiring fs module in which
    const fs = require('fs')

    // Write data in 'Output.txt' .
    fs.appendFile('/tmp/googleplay_ids.txt', collectedAppIdsArray.join('\n') + '\n', (err) => {
        // In case of a error throw err.
        if (err) throw err;
    })

    console.log('Appeneded %i Ids', collectedAppIdsArray.length)

}


// Apps pulled per category per collection
var numApps = 500

async function main() {

    // 54 Categories: GAME_TRIVIA, EVENTS, TRAVEL
    var categories = gplay.category
    // 3 Collections: TOP_FREE, TOP_PAID, GROSSING
    var collections = gplay.collection
    console.log("Starting %i categories and %i collections", Object.keys(categories).length, Object.keys(collections).length)

    // nested for loop for two iterables
    loopLists(categories, collections, numApps)


}

main()