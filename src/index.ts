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

async function handleRequest(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const params = new URLSearchParams(url.search);

  if (!params.has('q')) {
    return new Response('missing parameter: q',  {headers: HEADERS, status: 400});
  }

  const searchParams = `&input=${params.get('q')}`;

  switch(url.pathname) {
    case '/games':
      return await searchGames(params, searchParams, env);

    case '/players':
      return await searchPlayers(params, searchParams, env);

    case '/maps':
      return await searchMaps(params, searchParams, env);

    case '/events':
      return await searchEvents(params, searchParams, env);

    default:
      return new Response(`invalid path: ${url.pathname}`, {headers: HEADERS, status: 400});
  }
}

async function searchGames(requestParams: URLSearchParams, searchParams: string, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_search.json';
  const url = `${endpoint}?token=${TINYBIRD_API_KEY}${searchParams}`;

  if (!requestParams.has('refresh')) {
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

  return new Response(serializedSearchResults, {headers: HEADERS, status: apiResponse.status});
}

async function searchPlayers(requestParams: URLSearchParams, searchParams: string, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_player_search.json';
  const url = `${endpoint}?token=${TINYBIRD_API_KEY}${searchParams}`;

  if (!requestParams.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(url);
  const searchResponse: SearchResponse = await apiResponse.json();
  const searchResults = searchResponse.data;
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(searchParams, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS, status: apiResponse.status});
}

async function searchMaps(requestParams: URLSearchParams, searchParams: string, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_map_search.json';
  const url = `${endpoint}?token=${TINYBIRD_API_KEY}${searchParams}`;

  if (!requestParams.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(url);
  const searchResponse: SearchResponse = await apiResponse.json();
  const searchResults = searchResponse.data;
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(searchParams, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS, status: apiResponse.status});
}

async function searchEvents(requestParams: URLSearchParams, searchParams: string, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_event_search.json';
  const url = `${endpoint}?token=${TINYBIRD_API_KEY}${searchParams}`;

  if (!requestParams.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(url);
  const searchResponse: SearchResponse = await apiResponse.json();
  const searchResults = searchResponse.data;
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(searchParams, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS, status: apiResponse.status});
}
