// searchApps.js
(async () => {
  try {
    const gplay = await import('google-play-scraper');

    const searchTerm = process.argv[2];
    const numResults = parseInt(process.argv[3], 10) || 5;
    const country = process.argv[4];
    const language = process.argv[5];

    const results = await gplay.default.search({
      term: searchTerm,
      num: numResults,
      country: country,
      lang: language,
    });

    console.log(JSON.stringify(results));
  } catch (error) {
    console.error('Error fetching data from Google Play:', error);
  }
})();

