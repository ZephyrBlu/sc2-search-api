type Env = {
  TINYBIRD_API_KEY: string,
  SEARCH_RESULTS_CACHE: KVNamespace,
};

type SearchResponse = {
  meta: any[],
  data: any[],
  rows: number,
  statistics: {
    elapsed: number,
    rows_read: number,
    bytes_read: number,
  },
};

const HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
};
const CACHE_TTL = 86400;

export default {
  async fetch(request: Request, env: Env) {
    return await handleRequest(request, env);
  }
}

async function handleRequest(request: Request, env: Env) {
  const url = new URL(request.url);
  const params = new URLSearchParams(url.search);

  switch(url.pathname) {
    case '/search':
      return search(params, env);

    case '/player':
      return searchPlayers(params, env);
  }
}

async function search(params: URLSearchParams, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  let searchParams = '';
  if (params.has('q')) {
    searchParams += `&input=${params.get('q')}`;
  }

  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_search.json';
  const url = `${endpoint}?token=${TINYBIRD_API_KEY}${searchParams}`;

  if (!params.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(url);
  const searchResponse: SearchResponse = await apiResponse.json();

  const searchResults = searchResponse.data.map((record) => ({
    ...record,
    builds: JSON.parse(record.builds),
    players: JSON.parse(record.players),
  }));
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(searchParams, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS});
}

async function searchPlayers(params: URLSearchParams, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  let searchParams = '';
  if (params.has('q')) {
    searchParams += `&input=${params.get('q')}`;
  }

  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_player_search.json';
  const url = `${endpoint}?token=${TINYBIRD_API_KEY}${searchParams}`;

  if (!params.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(url);
  const searchResults: SearchResponse = await apiResponse.json();
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(searchParams, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS});

}
