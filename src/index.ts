export default {
  async fetch(request, env) {
    return await handleRequest(request, env)
  }
}

async function handleRequest(request, env) {
  const url = new URL(request.url);
  const urlParams = new URLSearchParams(url.search);

  let searchParams = '';
  if (urlParams.has('q')) {
    searchParams += `&input=${urlParams.get('q')}`;
  }

  const CACHE_TTL = 86400;
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  };

  if (!urlParams.has('refresh')) {
    const cachedResult = await env.SEARCH_RESULTS_CACHE.get(searchParams, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers});
    }
  }

  const tinybird = 'https://api.us-east.tinybird.co/v0/pipes/sc2_search.json';
  const search = `${tinybird}?token=${env.TINYBIRD_API_KEY}${searchParams}`;
  const apiResponse = await fetch(search);
  const searchResponse = await apiResponse.json();

  const searchResults = searchResponse.data.map((record) => ({
    ...record,
    builds: JSON.parse(record.builds),
    players: JSON.parse(record.players),
  }));
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await env.SEARCH_RESULTS_CACHE.put(searchParams, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers});
}
