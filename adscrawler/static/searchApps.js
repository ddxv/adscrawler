// searchApps.js
(async () => {
  try {
    const gplay = await import('google-play-scraper');

    const searchTerm = process.argv[2];
    const numResults = parseInt(process.argv[3], 10) || 5;

    const results = await gplay.default.search({
      term: searchTerm,
      num: numResults
    });

    console.log(JSON.stringify(results));
  } catch (error) {
    console.error('Error fetching data from Google Play:', error);
  }
})();

