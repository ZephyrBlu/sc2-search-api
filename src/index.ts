import {KVNamespace} from '@cloudflare/workers-types';

type Env = {
  TINYBIRD_API_KEY: string,
  SEARCH_RESULTS_CACHE: KVNamespace,
};

type TinybirdGamePipe =
  'sc2_game_search' |
  'sc2_fuzzy_game_search' |
  'sc2_player_search' |
  'sc2_map_search' |
  'sc2_event_search' |
  'sc2_player_builds';

type TinybirdTimelinePipe =
  'sc2_timeline_game_length' |
  'sc2_timeline_collection_rate' |
  'sc2_timeline_army_value' |
  'sc2_timeline_workers_active';

type TinybirdPipe = TinybirdGamePipe | TinybirdTimelinePipe;

interface TinybirdResponse {
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
      return await searchAllGames(params, searchParams, env);

    case '/players':
      return await searchGames('sc2_player_search', params, searchParams, env);

    case '/maps':
      return await searchGames('sc2_map_search', params, searchParams, env);

    case '/events':
      return await searchGames('sc2_event_search', params, searchParams, env);

    case '/builds':
      return await searchPlayerBuilds(params, searchParams, env);

    case '/timeline/game-length':
      return await searchTimelines('sc2_timeline_game_length', params, searchParams, env);

    case '/timeline/army-value':
      return await searchTimelines('sc2_timeline_army_value', params, searchParams, env);

    case '/timeline/collection-rate':
      return await searchTimelines('sc2_timeline_collection_rate', params, searchParams, env);

    case '/timeline/workers-active':
      return await searchTimelines('sc2_timeline_workers_active', params, searchParams, env);

    case '/timeline/quantiles/game-length':
      return await searchTimelines('sc2_timeline_game_length', params, searchParams, env, true);

    case '/timeline/quantiles/army-value':
      return await searchTimelines('sc2_timeline_army_value', params, searchParams, env, true);

    case '/timeline/quantiles/collection-rate':
      return await searchTimelines('sc2_timeline_collection_rate', params, searchParams, env, true);

    case '/timeline/quantiles/workers-active':
      return await searchTimelines('sc2_timeline_workers_active', params, searchParams, env, true);

    case '/recent':
      return await fetchRecent(params, env);

    default:
      return new Response(`invalid path: ${url.pathname}`, {headers: HEADERS, status: 400});
  }
}

function compare(a: string, b: string) {
  return a.toLowerCase() === b.toLowerCase();
}

async function searchAllGames(requestParams: URLSearchParams, searchParams: URLSearchParams, env: Env) {
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

async function searchGames(
  pipe: TinybirdGamePipe,
  requestParams: URLSearchParams,
  searchParams: URLSearchParams,
  env: Env,
) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = `https://api.us-east.tinybird.co/v0/pipes/${pipe}.json`;
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

async function searchPlayerBuilds(
  requestParams: URLSearchParams,
  searchParams: URLSearchParams,
  env: Env,
) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  const endpoint = 'https://api.us-east.tinybird.co/v0/pipes/sc2_player_builds.json';
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

async function searchTimelines(
  pipe: TinybirdTimelinePipe,
  requestParams: URLSearchParams,
  searchParams: URLSearchParams,
  env: Env,
  quantiles: boolean = false,
) {
  const {TINYBIRD_API_KEY, SEARCH_RESULTS_CACHE} = env;
  let endpoint = `https://api.us-east.tinybird.co/v0/pipes/${pipe}`;

  if (quantiles) {
    endpoint += '_quantiles';
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
