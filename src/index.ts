type Env = {
  TINYBIRD_API_KEY: string,
  SEARCH_RESULTS_CACHE: KVNamespace,
};

type TinybirdResponse = {
  meta: object[],
  data: object[],
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

  const searchParams = new URLSearchParams(params.toString());
  searchParams.delete('refresh');
  searchParams.delete('q');

  if (params.get('q')) {
    searchParams.set('input', params.get('q')!);
  }

  switch(url.pathname) {
    case '/games':
      return await searchGames(params, searchParams, env);

    case '/players':
      return await searchPlayers(params, searchParams, env);

    case '/maps':
      return await searchMaps(params, searchParams, env);

    case '/events':
      return await searchEvents(params, searchParams, env);

    case '/recent':
      return await fetchRecent(params, env);

    default:
      return new Response(`invalid path: ${url.pathname}`, {headers: HEADERS, status: 400});
  }
}

function compare(a: string, b: string) {
  return a.toLowerCase() === b.toLowerCase();
}

async function searchGames(requestParams: URLSearchParams, searchParams: URLSearchParams, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  
  let endpoint = 'https://api.us-east.tinybird.co/v0/pipes/';
  if (requestParams.has('fuzzy')) {
    endpoint += 'sc2_fuzzy_game_search';
    searchParams.delete('fuzzy');
  } else {
    endpoint += 'sc2_game_search';
  }
  endpoint += '.json';
  const url = `${endpoint}?${searchParams.toString()}`;
  const authorizedUrl = `${url}&token=${TINYBIRD_API_KEY}`;

  if (!requestParams.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(authorizedUrl);
  const searchResponse: TinybirdResponse = await apiResponse.json();

  const searchResults = searchResponse.data.map((record) => ({
    ...record,
    builds: JSON.parse(record.builds),
    players: JSON.parse(record.players),
  }));

  let orderedSearchResults = searchResults;
  if (searchParams.get('input')) {
    const exactMatches: any[] = [];
    const otherMatches: any[] = [];
    const terms = searchParams.get('input')!.split('+');
    searchResults.forEach((replay) => {
      let exact = false;
      replay.players.forEach((player) => {
        // any exact name match should rank replay higher
        const exactMatch = terms.some((term: string) => compare(player.name, term));
        if (!exact && exactMatch) {
          exactMatches.push(replay);
          exact = true;
        }
      });

      if (!exact) {
        otherMatches.push(replay);
      }
    });

    orderedSearchResults = [...exactMatches, ...otherMatches];
  }

  const serializedSearchResults = JSON.stringify(orderedSearchResults.slice(0, 20));

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(url, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS, status: apiResponse.status});
}

async function searchPlayers(requestParams: URLSearchParams, searchParams: URLSearchParams, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_player_search.json';
  const url = `${endpoint}?${searchParams.toString()}`;
  const authorizedUrl = `${url}&token=${TINYBIRD_API_KEY}`;

  if (!requestParams.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(authorizedUrl);
  const searchResponse: TinybirdResponse = await apiResponse.json();
  const searchResults = searchResponse.data;
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(url, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS, status: apiResponse.status});
}

async function searchMaps(requestParams: URLSearchParams, searchParams: URLSearchParams, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_map_search.json';
  const url = `${endpoint}?${searchParams.toString()}`;
  const authorizedUrl = `${url}&token=${TINYBIRD_API_KEY}`;

  if (!requestParams.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(authorizedUrl);
  const searchResponse: TinybirdResponse = await apiResponse.json();
  const searchResults = searchResponse.data;
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(url, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS, status: apiResponse.status});
}

async function searchEvents(requestParams: URLSearchParams, searchParams: URLSearchParams, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_event_search.json';
  const url = `${endpoint}?${searchParams.toString()}`;
  const authorizedUrl = `${url}&token=${TINYBIRD_API_KEY}`;

  if (!requestParams.has('refresh')) {
    const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {cacheTtl: CACHE_TTL});

    if (cachedResult) {
      return new Response(cachedResult, {headers: HEADERS});
    }
  }

  const apiResponse = await fetch(authorizedUrl);
  const searchResponse: TinybirdResponse = await apiResponse.json();
  const searchResults = searchResponse.data;
  const serializedSearchResults = JSON.stringify(searchResults);

  if (apiResponse.ok) {
    await SEARCH_RESULTS_CACHE.put(url, serializedSearchResults, {
      expirationTtl: CACHE_TTL,
    });
  }

  return new Response(serializedSearchResults, {headers: HEADERS, status: apiResponse.status});
}

async function fetchRecent(requestParams: URLSearchParams, env: Env) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/';
  const pipes = [
    'sc2_recent_games',
    'sc2_recent_players',
    'sc2_recent_maps',
    'sc2_recent_events',
  ];

  const responses: any[] = await Promise.all(pipes.map(async (pipe) => {
    const url = `${endpoint}${pipe}.json`;
    const authorizedUrl = `${url}?token=${TINYBIRD_API_KEY}`;

    let dataType = pipe.split('_').slice(-1)[0];
    if (dataType === 'games') {
      dataType = 'replays';
    }
  
    if (!requestParams.has('refresh')) {
      const cachedResult = await SEARCH_RESULTS_CACHE.get(url, {
        type: 'json',
        cacheTtl: CACHE_TTL,
      });
  
      if (cachedResult) {
        return {[dataType]: cachedResult};
      }
    }

    const response = await fetch(authorizedUrl);
    const results: TinybirdResponse = await response.json();
    
    let resultData = results.data;
    if (dataType === 'replays') {
      resultData = results.data.map((record) => ({
        ...record,
        builds: JSON.parse(record.builds),
        players: JSON.parse(record.players),
      }));
    }

    const serializedResults = JSON.stringify(results.data);
    
    if (response.ok) {
      await SEARCH_RESULTS_CACHE.put(url, serializedResults, {
        expirationTtl: CACHE_TTL,
      });
    }

    return {[dataType]: resultData};
  }));

  const recentResults = responses.reduce((allResults, currentResults) => ({
    ...allResults,
    ...currentResults,
  }), {});
  const serializedRecentResults = JSON.stringify(recentResults);

  return new Response(serializedRecentResults, {headers: HEADERS});
}
