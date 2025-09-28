export default {
  async fetch(request, env, ctx) {
      const url = new URL(request.url);
      const path = url.pathname;
      const proxySettings = await getProxySettings(env.WUYA);
      const upgradeHeader = request.headers.get("Upgrade");

      if (proxySettings.enableWsReverseProxy && upgradeHeader && upgradeHeader.toLowerCase() === "websocket") {
          try {
              const targetUrl = new URL(proxySettings.wsReverseProxyUrl);
              const wsRequest = new Request(targetUrl.origin + url.pathname + url.search, request);
              return fetch(wsRequest);
          } catch (e) {
              return new Response("无效的 WebSocket 反向代理 URL。", { status: 502 });
          }
      }
      
      try {
          await initializeAndMigrateDatabase(env);
      } catch (e) {
          console.error("Database initialization failed:", e.stack);
          return new Response("严重错误：数据库初始化失败，请检查Worker的D1数据库绑定是否正确配置为'WUYA'。", { status: 500 });
      }
      
      const subMatch = path.match(/^\/([a-zA-Z0-9]+)\/(xray|clash|singbox|surge)$/);
      if (subMatch) {
          return handleSubscriptionRequest(request, env, subMatch[1], subMatch[2]);
      }

      const nodeListMatch = path.match(/^\/([a-zA-Z0-9]+)$/);
      if (nodeListMatch && !path.startsWith('/api/') && !path.startsWith('/admin') && path.length > 1) {
          return handleNodeListRequest(request, env, nodeListMatch[1]);
      }

      try {
          if (path.startsWith('/api/')) {
              return await handleApiRequest(request, env);
          }
          if (path.length > 1 && !path.startsWith('/api/') && !['/login', '/admin'].includes(path)) {
              const fileName = path.substring(1);
              return await handleGitHubFileProxy(fileName, env, ctx);
          }
          return await handleUiRequest(request, env);
      } catch (e) {
          console.error("Global Catch:", e.stack);
          const errorResponse = { error: "发生意外的服务器错误。", details: e.message };
          const status = e.status || 500;
          if (path.startsWith('/api/')) {
              return jsonResponse(errorResponse, status);
          }
          return new Response(`错误: ${e.message}\n${e.stack}`, { status });
      }
  },
  async scheduled(controller, env, ctx) {
      console.log("Scheduled task started: Initializing...");
      await initializeAndMigrateDatabase(env);

      const db = env.WUYA;
      const nextTask = await getSetting(db, 'next_sync_task') || 'domains';

      switch (nextTask) {
          case 'domains':
          console.log("Scheduled task: Syncing a batch of DNS records (failure-first)...");
          await syncScheduledDomains(env);
          await setSetting(db, 'next_sync_task', 'ip_sources');
          break;
          case 'ip_sources':
          console.log("Scheduled task: Syncing a batch of IP sources to GitHub (failure-first)...");
          await syncScheduledIpSources(env);
          await setSetting(db, 'next_sync_task', 'external_subs');
          break;
          case 'external_subs':
          console.log("Scheduled task: Syncing an external subscription...");
          await syncScheduledExternalSubs(env);
          await setSetting(db, 'next_sync_task', 'domains');
          break;
          default:
          await setSetting(db, 'next_sync_task', 'domains');
          break;
      }

      console.log("Scheduled task for this cycle finished.");
  },
};

async function handleSubscriptionRequest(request, env, id, type) {
  const proxySettings = await getProxySettings(env.WUYA);
  const sublinkWorkerUrl = proxySettings.sublinkWorkerUrl;
  if (!sublinkWorkerUrl) {
      return new Response("订阅转换服务地址未配置。", { status: 503 });
  }
  
  const url = new URL(request.url);
  const nodeUrl = `${url.origin}/${id}`;
  const targetUrl = `${sublinkWorkerUrl.replace(/\/$/, '')}/${type}?config=${encodeURIComponent(nodeUrl)}`;

  try {
      const subResponse = await fetch(targetUrl, { headers: { 'User-Agent': request.headers.get('User-Agent') || 'CF-DNS-Clon/1.0' } });
      const responseHeaders = new Headers(subResponse.headers);
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      return new Response(subResponse.body, { status: subResponse.status, statusText: subResponse.statusText, headers: responseHeaders });
  } catch (e) {
      return new Response(`获取上游订阅失败: ${e.message}`, { status: 502 });
  }
}

async function handleNodeListRequest(request, env, id) {
  try {
      const { results: domains } = await env.WUYA.prepare('SELECT id, target_domain, notes, is_single_resolve, single_resolve_limit, last_synced_records, single_resolve_node_names FROM domains WHERE is_enabled = 1 ORDER BY is_system DESC, id').all();
      const proxySettings = await getProxySettings(env.WUYA);

      const internalNodes = generateInternalNodes(domains, proxySettings, request);
      const customNodes = parseCustomNodes(proxySettings.customNodes, proxySettings, request);
      
      const enabledSubUrls = (proxySettings.externalSubscriptions || []).filter(sub => sub.enabled && sub.url).map(sub => sub.url);
      let externalNodes = [];
      if (enabledSubUrls.length > 0) {
          const placeholders = enabledSubUrls.map(() => '?').join(',');
          const { results: externalNodeRecords } = await env.WUYA.prepare(`SELECT content FROM external_nodes WHERE content IS NOT NULL AND content != '' AND url IN (${placeholders})`).bind(...enabledSubUrls).all();
          const externalNodeContent = externalNodeRecords.map(r => r.content).join('\n');
          externalNodes = parseExternalNodes(externalNodeContent, proxySettings, request);
      }
      
      let allNodes = [...internalNodes, ...customNodes, ...externalNodes];

      if (proxySettings.filterMode === 'global' && proxySettings.globalFilters) {
          const rules = parseFilterRules(proxySettings.globalFilters);
          if (rules.length > 0) {
              allNodes = allNodes.map(node => {
                  try {
                      const url = new URL(node);
                      let newAddress = url.hostname;
                      let newRemarks = decodeURIComponent(url.hash.substring(1));

                      for (const rule of rules) {
                          switch (rule.type) {
                              case '#H:':
                                  if (newAddress.includes(rule.match)) {
                                      newAddress = newAddress.replace(new RegExp(rule.match, 'g'), rule.replacement);
                                  }
                                  break;
                              case '#M:':
                                  if (newRemarks.includes(rule.match)) {
                                      newRemarks = newRemarks.replace(new RegExp(rule.match, 'g'), rule.replacement);
                                  }
                                  break;
                              case '#T:':
                                  newRemarks = rule.replacement + newRemarks;
                                  break;
                              case '#W:':
                                  newRemarks = newRemarks + rule.replacement;
                                  break;
                          }
                      }

                      url.hostname = newAddress;
                      url.hash = encodeURIComponent(newRemarks);
                      return url.toString();
                  } catch (e) {
                      return node;
                  }
              });
          }
      }

      if (allNodes.length === 0) {
          return new Response("没有可用的节点。", { status: 404 });
      }
      
      const numberedNodes = allNodes.map((node, index) => {
          try {
              const url = new URL(node);
              const remarks = decodeURIComponent(url.hash.substring(1));
              url.hash = encodeURIComponent(`${index + 1} ~ ${remarks}`);
              return url.toString();
          } catch (e) {
              console.warn(`Skipping invalid node URL: ${node}`);
              return null;
          }
      }).filter(Boolean);

      const vlessLinksText = numberedNodes.join('\n');
      const base64EncodedLinks = btoa(vlessLinksText.trim());
      return new Response(base64EncodedLinks, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });

  } catch (e) {
      console.error(`生成节点列表失败: ${e.stack}`);
      return new Response(`生成节点列表失败: ${e.message}`, { status: 500 });
  }
}

function generateInternalNodes(domains, proxySettings, request) {
  if (!proxySettings.enableWsReverseProxy) return [];

  const requestHostname = new URL(request.url).hostname;
  let sniHost = requestHostname;
  if (proxySettings.useProxyUrlForSni) {
      try {
          sniHost = new URL(proxySettings.wsReverseProxyUrl).hostname;
      } catch (e) {
          console.warn("Invalid wsReverseProxyUrl, fallback to self url for SNI/Host", e.message);
      }
  }
  
  const path = proxySettings.wsReverseProxyPath || '/';
  const useRandomUuid = proxySettings.wsReverseProxyUseRandomUuid;
  const specificUuid = proxySettings.wsReverseProxySpecificUuid;

  return domains.flatMap(domain => {
      const nodes = [];
      
      const mainNodeName = domain.notes || domain.target_domain;
      const mainUuid = useRandomUuid ? crypto.randomUUID() : specificUuid;
      const mainEncodedPath = encodeURIComponent(encodeURIComponent(path));
      nodes.push(`vless://${mainUuid}@${domain.target_domain}:443?encryption=none&security=tls&sni=${sniHost}&fp=random&type=ws&host=${sniHost}&path=${mainEncodedPath}#${encodeURIComponent(mainNodeName)}`);

      if (domain.is_single_resolve) {
          try {
              const records = JSON.parse(domain.last_synced_records || '[]');
              const limit = domain.single_resolve_limit || 5;
              const finalRecords = records.slice(0, limit);
              const customNames = JSON.parse(domain.single_resolve_node_names || '[]');

              const targetParts = domain.target_domain.split('.');
              if(targetParts.length > 1){
                  const targetPrefix = targetParts[0];
                  const zone = targetParts.slice(1).join('.');
                  
                  finalRecords.forEach((record, index) => {
                      const singleDomain = `${targetPrefix}.${index + 1}.${zone}`;
                      const nodeName = customNames[index] || (domain.notes ? `${domain.notes} #${index + 1}` : `${singleDomain}`);
                      const uuid = useRandomUuid ? crypto.randomUUID() : specificUuid;
                      const encodedPath = encodeURIComponent(encodeURIComponent(path));

                      nodes.push(`vless://${uuid}@${singleDomain}:443?encryption=none&security=tls&sni=${sniHost}&fp=random&type=ws&host=${sniHost}&path=${encodedPath}#${encodeURIComponent(nodeName)}`);
                  });
              }
          } catch(e) {
              console.error(`Error processing single resolve for ${domain.target_domain}: ${e.message}`);
          }
      }
      return nodes;
  });
}


async function syncScheduledDomains(env) {
  const BATCH_SIZE = 5;
  const db = env.WUYA;
  const log = (msg) => console.log(beijingTimeLog(msg));

  const { token, zoneId } = await getCfApiSettings(db);
  if (!token || !zoneId) {
      log("Cannot run scheduled domain sync: Cloudflare settings are missing.");
      return;
  }

  const query = `
      SELECT * FROM domains 
      WHERE is_enabled = 1 
      ORDER BY 
          CASE last_sync_status WHEN 'failed' THEN 0 ELSE 1 END, 
          last_synced_time ASC
      LIMIT ?`;
  
  const { results: domainsToSync } = await db.prepare(query).bind(BATCH_SIZE).all();

  if (domainsToSync.length === 0) {
      log("No domains to sync in this batch.");
      return;
  }

  log(`Found ${domainsToSync.length} domains for this sync batch (failure-first).`);
  
  const allZoneRecords = await listAllDnsRecords(token, zoneId);
  const syncContext = { allZoneRecords };

  for (const domain of domainsToSync) {
      try {
          await syncDomainLogic(domain, token, zoneId, db, log, syncContext);
      } catch (e) {
          log(`Error processing domain ${domain.target_domain} in batch: ${e.message}`);
      }
  }
}

async function syncScheduledIpSources(env) {
  const BATCH_SIZE = 5;
  const db = env.WUYA;
  const log = (msg) => console.log(beijingTimeLog(msg));

  const githubSettings = await getGitHubSettings(db);
  if (!githubSettings.token || !githubSettings.owner || !githubSettings.repo) {
      log("Cannot run scheduled IP source sync: GitHub settings are missing.");
      return;
  }

  const query = `
      SELECT id FROM ip_sources 
      WHERE is_enabled = 1 
      ORDER BY 
          CASE last_sync_status WHEN 'failed' THEN 0 ELSE 1 END, 
          last_synced_time ASC
      LIMIT ?`;

  const { results: sourcesToSync } = await db.prepare(query).bind(BATCH_SIZE).all();

  if (sourcesToSync.length === 0) {
      log("No IP sources to sync in this batch.");
      return;
  }

  log(`Found ${sourcesToSync.length} IP sources for this sync batch (failure-first).`);
  for (const source of sourcesToSync) {
      await syncSingleIpSource(source.id, env, false).catch(e => {
          log(`Error processing IP source ID ${source.id} in batch: ${e.message}`);
      });
  }
}

async function syncScheduledExternalSubs(env) {
  const db = env.WUYA;
  const log = (msg) => console.log(beijingTimeLog(msg));
  const proxySettings = await getProxySettings(db);

  const subsToSync = proxySettings.externalSubscriptions?.filter(sub => sub.enabled && sub.url) || [];
  if (subsToSync.length === 0) {
      log("No enabled external subscriptions to sync.");
      return;
  }
  
  const lastSyncedIndex = parseInt(await getSetting(db, 'last_ext_sub_synced_index') || '-1', 10);
  const nextIndex = (lastSyncedIndex + 1) % subsToSync.length;
  const subToSync = subsToSync[nextIndex];
  
  log(`Syncing external subscription: ${subToSync.url}`);
  await syncExternalSubscription(subToSync, db, log);
  
  await setSetting(db, 'last_ext_sub_synced_index', nextIndex.toString());
}


async function initializeAndMigrateDatabase(env) {
if (!env.WUYA) {
  throw new Error("D1 database binding 'WUYA' not found. Please configure it in your Worker settings.");
}
const db = env.WUYA;
const expectedSchemas = {
  settings: ['key TEXT PRIMARY KEY NOT NULL', 'value TEXT NOT NULL'],
  domains: [
      'id INTEGER PRIMARY KEY AUTOINCREMENT',
      'source_domain TEXT NOT NULL',
      'target_domain TEXT NOT NULL',
      'zone_id TEXT NOT NULL',
      'is_deep_resolve INTEGER NOT NULL DEFAULT 1',
      'ttl INTEGER NOT NULL DEFAULT 60',
      'notes TEXT',
      'is_single_resolve INTEGER NOT NULL DEFAULT 0',
      'single_resolve_limit INTEGER NOT NULL DEFAULT 5',
      'single_resolve_node_names TEXT',
      'last_synced_records TEXT DEFAULT \'[]\'',
      'displayed_records TEXT DEFAULT \'[]\'',
      'last_synced_time TIMESTAMP',
      'last_sync_status TEXT DEFAULT \'pending\'',
      'last_sync_error TEXT',
      'is_enabled INTEGER DEFAULT 1 NOT NULL',
      'is_system INTEGER NOT NULL DEFAULT 0',
      'UNIQUE(target_domain)'
  ],
  sessions: ['token TEXT PRIMARY KEY NOT NULL', 'expires_at TIMESTAMP NOT NULL'],
  ip_sources: [
      'id INTEGER PRIMARY KEY AUTOINCREMENT',
      'url TEXT NOT NULL UNIQUE',
      'github_path TEXT NOT NULL UNIQUE',
      'commit_message TEXT NOT NULL',
      'fetch_strategy TEXT',
      'last_synced_time TIMESTAMP',
      'last_sync_status TEXT DEFAULT \'pending\'',
      'last_sync_error TEXT',
      'is_enabled INTEGER DEFAULT 1 NOT NULL'
  ],
  external_nodes: [
      'url TEXT PRIMARY KEY NOT NULL',
      'content TEXT',
      'status TEXT NOT NULL',
      'last_updated TIMESTAMP NOT NULL',
      'error TEXT'
  ]
};

const createStmts = Object.keys(expectedSchemas).map(tableName =>
  db.prepare(`CREATE TABLE IF NOT EXISTS ${tableName} (${expectedSchemas[tableName].join(', ')});`)
);
await db.batch(createStmts);

for (const tableName in expectedSchemas) {
  const { results: existingColumns } = await db.prepare(`PRAGMA table_info(${tableName})`).all();
  const existingColumnNames = existingColumns.map(c => c.name);
  const expectedColumnDefs = expectedSchemas[tableName].filter(def => !def.startsWith('UNIQUE'));

  for (const columnDef of expectedColumnDefs) {
      const columnName = columnDef.split(' ')[0];
      if (!existingColumnNames.includes(columnName)) {
          try {
              await db.prepare(`ALTER TABLE ${tableName} ADD COLUMN ${columnDef}`).run();
          } catch (e) { console.error(`Failed to add column '${columnName}' to '${tableName}':`, e.message); }
      }
  }
}

const { token, zoneId } = await getCfApiSettings(db);
if (!token || !zoneId) return;

const { results: invalidDomains } = await db.prepare("SELECT id, target_domain FROM domains WHERE target_domain LIKE '%.'").all();
if (invalidDomains.length > 0) {
  try {
      const zoneName = (await getZoneName(token, zoneId)).replace(/\.$/, '');
      const fixStmts = [];
      for (const domain of invalidDomains) {
          const prefix = domain.target_domain.replace(/\.$/, '');
          const correctedDomain = (prefix === '' || prefix === '@') ? zoneName : `${prefix}.${zoneName}`;
          fixStmts.push(db.prepare("UPDATE domains SET target_domain = ? WHERE id = ?").bind(correctedDomain, domain.id));
      }
      await db.batch(fixStmts);
  } catch (e) { console.error("Failed to fix invalid domain entries:", e.message); }
}
}

async function ensureInitialData(db, zoneId, zoneName) {
  if (!zoneId || !zoneName) return;
  
  await setSetting(db, 'THREE_NETWORK_SOURCE', await getSetting(db, 'THREE_NETWORK_SOURCE') || 'api.uouin.com');

  const initialIpSources = [
      { url: 'https://ipdb.api.030101.xyz/?type=bestcf&country=true', path: '030101-bestcf.txt', msg: 'Update BestCF IPs from 030101.xyz', strategy: 'phantomjs_cloud' },
      { url: 'https://ipdb.api.030101.xyz/?type=bestproxy&country=true', path: '030101-bestproxy.txt', msg: 'Update BestProxy IPs from 030101.xyz', strategy: 'phantomjs_cloud' },
      { url: 'https://ip.164746.xyz', path: '164746.txt', msg: 'Update IPs from 164746.xyz', strategy: 'direct_regex' },
      { url: 'https://stock.hostmonit.com/CloudFlareYes', path: 'CloudFlareYes.txt', msg: 'Update CloudFlareYes IPs', strategy: 'phantomjs_cloud' },
      { url: 'https://ip.haogege.xyz', path: 'haogege.txt', msg: 'Update IPs from haogege.xyz', strategy: 'direct_regex' },
      { url: 'https://api.uouin.com/cloudflare.html', path: 'uouin-cloudflare.txt', msg: 'Update IPs from uouin.com', strategy: 'direct_regex' },
      { url: 'https://www.wetest.vip/page/cloudflare/address_v4.html', path: 'wetest-cloudflare-v4.txt', msg: 'Update Cloudflare v4 IPs from wetest.vip', strategy: 'direct_regex' },
      { url: 'https://www.wetest.vip/page/edgeone/address_v4.html', path: 'wetest-edgeone-v4.txt', msg: 'Update EdgeOne v4 IPs from wetest.vip', strategy: 'direct_regex' },
  ];
  const ipSourceStmts = initialIpSources.map(s => 
      db.prepare('INSERT INTO ip_sources (url, github_path, commit_message, fetch_strategy) VALUES (?, ?, ?, ?) ON CONFLICT(url) DO NOTHING')
      .bind(s.url, s.path, s.msg, s.strategy)
  );
  
  const initialDomains = [
      { source: 'internal:hostmonit:yd', prefix: 'yd', notes: '移动', is_system: 1, deep_resolve: 1 },
      { source: 'internal:hostmonit:dx', prefix: 'dx', notes: '电信', is_system: 1, deep_resolve: 1 },
      { source: 'internal:hostmonit:lt', prefix: 'lt', notes: '联通', is_system: 1, deep_resolve: 1 },
      { source: 'www.wto.org', prefix: 'wto', notes: 'wto', is_system: 0, deep_resolve: 1 },
      { source: 'www.visa.com.sg', prefix: 'visasg', notes: 'visasg', is_system: 0, deep_resolve: 1 },
      { source: 'www.shopify.com', prefix: 'shopify', notes: 'shopify', is_system: 0, deep_resolve: 1 },
      { source: 'openai.com', prefix: 'openai', notes: 'openai', is_system: 0, deep_resolve: 1 },
      { source: 'snipaste5.speedip.eu.org', prefix: 'bp5', notes: 'bp5', is_system: 0, deep_resolve: 1 },
      { source: 'cf.090227.xyz', prefix: 'cm', notes: 'cm', is_system: 0, deep_resolve: 1 },
      { source: 'cf.877774.xyz', prefix: 'qms', notes: 'qms', is_system: 0, deep_resolve: 1 },
  ];
  const domainStmts = initialDomains.map(d => {
      const targetDomain = d.prefix === '@' ? zoneName : `${d.prefix}.${zoneName}`;
      return db.prepare('INSERT INTO domains (source_domain, target_domain, zone_id, is_deep_resolve, notes, is_system) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(target_domain) DO NOTHING')
              .bind(d.source, targetDomain, zoneId, d.deep_resolve, d.notes, d.is_system);
  });

  await db.batch([...ipSourceStmts, ...domainStmts]);
}

async function getSetting(db, key) { return db.prepare("SELECT value FROM settings WHERE key = ?").bind(key).first("value"); }
async function setSetting(db, key, value) { 
  if (value === undefined || value === null) {
      await db.prepare("DELETE FROM settings WHERE key = ?").bind(key).run();
  } else {
      await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)").bind(key, value).run();
  }
}

async function getFullSettings(db) {
  const { results } = await db.prepare("SELECT key, value FROM settings").all();
  const settings = {};
  for (const row of results) {
      settings[row.key] = row.value;
  }
  return settings;
}

async function getProxySettings(db) {
  const proxySettingsStr = await getSetting(db, 'PROXY_SETTINGS');
  const storedSettings = proxySettingsStr ? JSON.parse(proxySettingsStr) : {};
  const defaultSettings = {
      enableWsReverseProxy: false,
      wsReverseProxyUrl: '',
      wsReverseProxyUseRandomUuid: true,
      wsReverseProxySpecificUuid: '',
      wsReverseProxyPath: '/',
      useSelfUrlForSni: true,
      useProxyUrlForSni: false,
      sublinkWorkerUrl: 'https://nnmm.eu.org',
      publicSubscription: false,
      subUseRandomId: true,
      subIdLength: 12,
      subCustomId: '',
      subIdCharset: 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789',
      externalSubscriptions: [],
      customNodes: '',
      filterMode: 'none', 
      globalFilters: '',
      APP_THEME: 'default'
  };
  return { ...defaultSettings, ...storedSettings };
}


async function getCfApiSettings(db) {
const [token, zoneId] = await Promise.all([
  getSetting(db, 'CF_API_TOKEN'),
  getSetting(db, 'CF_ZONE_ID'),
]);
return { token: token || '', zoneId: zoneId || '' };
}

async function getGitHubSettings(db) {
  const [token, owner, repo] = await Promise.all([
      getSetting(db, 'GITHUB_TOKEN'),
      getSetting(db, 'GITHUB_OWNER'),
      getSetting(db, 'GITHUB_REPO'),
  ]);
  return { token, owner, repo };
}

async function handleApiRequest(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;
  const db = env.WUYA;

  if (path === '/api/status' && method === 'GET') {
    const passwordSet = await getSetting(db, 'ADMIN_PASSWORD_HASH');
    return jsonResponse({ isInitialized: !!passwordSet });
  }
  if (path === '/api/setup' && method === 'POST') return await apiSetup(request, db);
  if (path === '/api/login' && method === 'POST') return await apiLogin(request, db);

  if (!await isAuthenticated(request, db)) return jsonResponse({ error: '未授权' }, 401);

  if (method === 'POST' && path === '/api/logout') return await apiLogout(request, db);
  if (method === 'GET' && path === '/api/settings') return await apiGetSettings(request, db);
  if (method === 'POST' && path === '/api/settings') return await apiSetSettings(request, env);
  if (method === 'GET' && path === '/api/domains') return await apiGetDomains(request, db);
  if (method === 'POST' && path === '/api/domains') return await apiAddDomain(request, db);
  if (method === 'POST' && path === '/api/sync') return syncAllDomains(env, true);
  if (method === 'POST' && path === '/api/domains/sync_system') return syncSystemDomains(env, true);
  if (method === 'POST' && path === '/api/proxy/test_subscription') return await apiTestExternalSubscription(request, env);

  const domainMatch = path.match(/^\/api\/domains\/(\d+)$/);
  if (domainMatch) {
    const id = domainMatch[1];
    if (method === 'PUT') return await apiUpdateDomain(request, db, id);
    if (method === 'DELETE') return await apiDeleteDomain(request, db, id);
  }
  
  const liveResolveMatch = path.match(/^\/api\/domains\/(\d+)\/resolve$/);
  if (liveResolveMatch && method === 'POST') {
      return await apiLiveResolveDomain(liveResolveMatch[1], env);
  }

  const syncMatch = path.match(/^\/api\/domains\/(\d+)\/sync$/);
  if (syncMatch && method === 'POST') {
    const id = syncMatch[1];
    return syncSingleDomain(id, env, true);
  }

  if (method === 'GET' && path === '/api/ip_sources') return await apiGetIpSources(db);
  if (method === 'POST' && path === '/api/ip_sources') return await apiAddIpSource(request, db);
  if (method === 'POST' && path === '/api/ip_sources/probe') return await apiProbeIpSource(request);
  if (method === 'POST' && path === '/api/ip_sources/sync_all') return syncAllIpSources(env, true);

  const ipSourceMatch = path.match(/^\/api\/ip_sources\/(\d+)$/);
  if (ipSourceMatch) {
    const id = ipSourceMatch[1];
    if (method === 'PUT') return await apiUpdateIpSource(request, db, id);
    if (method === 'DELETE') return await apiDeleteIpSource(db, id);
  }

  const ipSourceSyncMatch = path.match(/^\/api\/ip_sources\/(\d+)\/sync$/);
  if (ipSourceSyncMatch && method === 'POST') {
    const id = ipSourceSyncMatch[1];
    return syncSingleIpSource(id, env, true);
  }

  return jsonResponse({ error: 'API 端点未找到' }, 404);
}

async function apiTestExternalSubscription(request, env) {
  try {
      const { url, filters } = await request.json();
      if (!url) {
          return jsonResponse({ success: false, error: 'URL is required.' }, 400);
      }
      const proxySettings = await getProxySettings(env.WUYA);

      let testContent = await fetchAndParseExternalSubscription(url);
      if (testContent === null) {
          return jsonResponse({ success: false, error: '无法获取或解析订阅内容。' });
      }
      
      let effectiveFilters = '';
      if (proxySettings.filterMode === 'global') {
          effectiveFilters = proxySettings.globalFilters;
      } else if (proxySettings.filterMode === 'individual') {
          effectiveFilters = filters;
      }
      
      const filteredContent = applyContentFilters(testContent, effectiveFilters);
      const nodeCount = filteredContent.trim().split('\n').filter(Boolean).length;
      
      return jsonResponse({ success: true, nodeCount: nodeCount });

  } catch (e) {
      return jsonResponse({ success: false, error: e.message }, 500);
  }
}

async function apiSetup(request, db) {
const passwordSet = await getSetting(db, 'ADMIN_PASSWORD_HASH');
if (passwordSet) return jsonResponse({ error: '应用已经初始化。' }, 403);
const { password } = await request.json();
if (!password || password.length < 8) return jsonResponse({ error: '密码长度必须至少为8个字符。' }, 400);
const salt = crypto.randomUUID();
const hash = await hashPassword(password, salt);
await db.batch([
  db.prepare("INSERT INTO settings (key, value) VALUES ('ADMIN_PASSWORD_HASH', ?)").bind(hash),
  db.prepare("INSERT INTO settings (key, value) VALUES ('PASSWORD_SALT', ?)").bind(salt),
]);
return jsonResponse({ success: true });
}

async function apiLogin(request, db) {
const { password } = await request.json();
const [storedHash, salt] = await Promise.all([getSetting(db, 'ADMIN_PASSWORD_HASH'), getSetting(db, 'PASSWORD_SALT')]);
if (!storedHash || !salt) return jsonResponse({ error: '应用尚未初始化。' }, 400);
const inputHash = await hashPassword(password, salt);
if (inputHash === storedHash) {
  const token = crypto.randomUUID();
  const expires = new Date(Date.now() + 24 * 60 * 60 * 1000);
  await db.prepare("INSERT INTO sessions (token, expires_at) VALUES (?, ?)").bind(token, expires.toISOString()).run();
  const sessionCookie = `session=${token}; HttpOnly; Secure; Path=/; SameSite=Strict; Max-Age=86400`;
  return jsonResponse({ success: true }, 200, { 'Set-Cookie': sessionCookie });
}
return jsonResponse({ error: '密码无效。' }, 401);
}

async function apiLogout(request, db) {
const token = getCookie(request, 'session');
if (token) await db.prepare("DELETE FROM sessions WHERE token = ?").bind(token).run();
const expiryCookie = 'session=; HttpOnly; Secure; Path=/; SameSite=Strict; Expires=Thu, 01 Jan 1970 00:00:00 GMT';
return jsonResponse({ success: true }, 200, { 'Set-Cookie': expiryCookie });
}

async function apiGetSettings(request, db) {
  const settings = await getFullSettings(db);
  const proxySettings = await getProxySettings(db);
  
  const combinedSettings = {
      CF_API_TOKEN: settings.CF_API_TOKEN || '',
      CF_ZONE_ID: settings.CF_ZONE_ID || '',
      THREE_NETWORK_SOURCE: settings.THREE_NETWORK_SOURCE || 'CloudFlareYes',
      GITHUB_TOKEN: settings.GITHUB_TOKEN || '',
      GITHUB_OWNER: settings.GITHUB_OWNER || '',
      GITHUB_REPO: settings.GITHUB_REPO || '',
      proxySettings: proxySettings
  };

  if (combinedSettings.CF_API_TOKEN && combinedSettings.CF_ZONE_ID) {
      try { 
          combinedSettings.zoneName = await getZoneName(combinedSettings.CF_API_TOKEN, combinedSettings.CF_ZONE_ID);
      } catch (e) {
          console.warn("Could not fetch zone name for settings endpoint");
      }
  }
  return jsonResponse(combinedSettings);
}

async function apiSetSettings(request, env) {
  const db = env.WUYA;
  const settings = await request.json();
  const { CF_API_TOKEN, CF_ZONE_ID, proxySettings } = settings;
  
  if (CF_API_TOKEN && CF_ZONE_ID) {
      const seeded = await getSetting(db, 'INITIAL_DATA_SEEDED');
      if (!seeded) {
          try {
              const zoneName = await getZoneName(CF_API_TOKEN, CF_ZONE_ID);
              await ensureInitialData(db, CF_ZONE_ID, zoneName);
              await setSetting(db, 'INITIAL_DATA_SEEDED', 'true');
          } catch (e) {
              return jsonResponse({ error: `Cloudflare API 验证失败: ${e.message}` }, 400);
          }
      }
  }
  
  const settingsToSave = { ...settings };
  delete settingsToSave.proxySettings; 

  const oldProxySettings = await getProxySettings(db);
  const setPromises = Object.entries(settingsToSave).map(([key, value]) => setSetting(db, key, value));
  
  if (proxySettings) {
      setPromises.push(setSetting(db, 'PROXY_SETTINGS', JSON.stringify(proxySettings)));
  }
  
  await Promise.all(setPromises);

  if (proxySettings) {
      const oldSubs = oldProxySettings.externalSubscriptions || [];
      const newSubs = proxySettings.externalSubscriptions || [];

      const disabledSubs = oldSubs
          .filter(oldSub => oldSub.enabled && !newSubs.some(newSub => newSub.url === oldSub.url && newSub.enabled))
          .map(sub => sub.url);
      
      if (disabledSubs.length > 0) {
          console.log("Deleting disabled subscriptions from DB:", disabledSubs);
          const placeholders = disabledSubs.map(() => '?').join(',');
          await db.prepare(`DELETE FROM external_nodes WHERE url IN (${placeholders})`).bind(...disabledSubs).run();
      }

      const reEnabledSubs = newSubs.filter(newSub => 
          newSub.enabled && 
          newSub.url &&
          !oldSubs.some(oldSub => oldSub.url === newSub.url && oldSub.enabled)
      );

      if (reEnabledSubs.length > 0) {
          console.log("Re-syncing newly enabled subscriptions:", reEnabledSubs.map(s => s.url));
          const syncPromises = reEnabledSubs.map(sub => syncExternalSubscription(sub, db, (msg) => console.log(msg)));
          await Promise.all(syncPromises);
      }
  }
  
  return jsonResponse({ success: true, message: '设置已成功保存。' });
}


async function apiGetDomains(request, db) {
  const query = `
    SELECT id, source_domain, target_domain, zone_id, is_deep_resolve, ttl, notes, 
            is_single_resolve, single_resolve_limit, single_resolve_node_names,
            strftime('%Y-%m-%dT%H:%M:%SZ', last_synced_time) as last_synced_time, 
            last_sync_status, last_sync_error, is_enabled, is_system,
            displayed_records
    FROM domains ORDER BY is_system DESC, id`;
  const { results } = await db.prepare(query).all();
  return jsonResponse(results);
}

async function handleDomainMutation(request, db, isUpdate = false, id = null) {
  const { source_domain, target_domain_prefix, is_deep_resolve, ttl, notes, is_single_resolve, single_resolve_limit, single_resolve_node_names } = await request.json();
  const { token, zoneId } = await getCfApiSettings(db);
  if (!zoneId) return jsonResponse({ error: '尚未在设置中配置区域 ID。' }, 400);

  const commonValues = [is_deep_resolve ? 1 : 0, ttl || 60, notes || null, is_single_resolve ? 1 : 0, single_resolve_limit || 5, JSON.stringify(single_resolve_node_names || [])];

  try {
      if (isUpdate) {
          const domainInfo = await db.prepare("SELECT is_system, target_domain FROM domains WHERE id = ?").bind(id).first();
          if (!domainInfo) return jsonResponse({ error: "目标不存在。" }, 404);

          if (domainInfo.is_system) {
              await db.prepare('UPDATE domains SET is_deep_resolve=?, ttl=?, notes=?, is_single_resolve=?, single_resolve_limit=?, single_resolve_node_names=? WHERE id = ?')
                  .bind(...commonValues, id).run();
              return jsonResponse({ success: true, message: "系统目标部分设置更新成功。" });
          } else {
              if (!source_domain || !target_domain_prefix) return jsonResponse({ error: '缺少必填字段。' }, 400);
              const zoneName = (await getZoneName(token, zoneId)).replace(/\.$/, '');
              const target_domain = (target_domain_prefix.trim() === '@' || target_domain_prefix.trim() === '')
                  ? zoneName : `${target_domain_prefix.trim()}.${zoneName}`;

              await db.prepare('UPDATE domains SET source_domain=?, target_domain=?, zone_id=?, is_deep_resolve=?, ttl=?, notes=?, is_single_resolve=?, single_resolve_limit=?, single_resolve_node_names=? WHERE id = ?')
                  .bind(source_domain, target_domain, zoneId, ...commonValues, id).run();
              return jsonResponse({ success: true, message: "目标更新成功。" });
          }
      } else {
          if (!source_domain || !target_domain_prefix) return jsonResponse({ error: '缺少必填字段。' }, 400);
          const zoneName = (await getZoneName(token, zoneId)).replace(/\.$/, '');
          const target_domain = (target_domain_prefix.trim() === '@' || target_domain_prefix.trim() === '')
              ? zoneName : `${target_domain_prefix.trim()}.${zoneName}`;

          await db.prepare('INSERT INTO domains (source_domain, target_domain, zone_id, is_deep_resolve, ttl, notes, is_system, is_single_resolve, single_resolve_limit, single_resolve_node_names) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)')
              .bind(source_domain, target_domain, zoneId, ...commonValues).run();
          return jsonResponse({ success: true, message: "目标添加成功。" });
      }
  } catch (e) {
      if (e.message.includes('UNIQUE constraint failed')) return jsonResponse({ error: '该目标域名已存在。' }, 409);
      throw e;
  }
}


async function apiAddDomain(request, db) { return handleDomainMutation(request, db, false); }
async function apiUpdateDomain(request, db, id) { return handleDomainMutation(request, db, true, id); }
async function apiDeleteDomain(request, db, id) {
  const { changes } = await db.prepare('DELETE FROM domains WHERE id = ? AND is_system = 0').bind(id).run();
  if (changes === 0) {
      return jsonResponse({ error: "删除失败，目标为系统预设或不存在。" }, 403);
  }
  return jsonResponse({ success: true, message: "目标删除成功。" });
}

async function apiLiveResolveDomain(id, env) {
    try {
        const db = env.WUYA;
        const domain = await db.prepare("SELECT source_domain, is_deep_resolve FROM domains WHERE id = ?").bind(id).first();
        if (!domain) {
            return jsonResponse({ error: "目标未找到" }, 404);
        }
        
        let records;
        const noOpLog = () => {};

        if (domain.is_deep_resolve) {
            records = await resolveRecursively(domain.source_domain, noOpLog);
        } else {
            const cnames = await getDnsFromDoh(domain.source_domain, 'CNAME');
            if (cnames.length > 0) {
                records = [{ type: 'CNAME', content: cnames[0].replace(/\.$/, "") }];
            } else {
                records = [];
            }
        }
        return jsonResponse(records);
    } catch(e) {
        return jsonResponse({ error: `实时解析失败: ${e.message}`}, 500);
    }
}


async function handleUiRequest(request, env) {
const url = new URL(request.url);
const path = url.pathname;
const db = env.WUYA;

const isInitialized = !!(await getSetting(db, 'ADMIN_PASSWORD_HASH'));
const loggedIn = await isAuthenticated(request, db);
let pageContent, pageTitle;

if (!isInitialized) {
  pageTitle = '系统初始化';
  pageContent = getSetupPage();
} else if (path === '/admin' && loggedIn) {
  pageTitle = 'DNS Clone Dashboard';
  const settingsPromise = getFullSettings(db);
  const settings = await settingsPromise;
  
  const { token, zoneId } = await getCfApiSettings(db);
  if (token && zoneId) {
      try {
          settings.zoneName = await getZoneName(token, zoneId);
      } catch(e) { console.warn("Could not fetch zone name.", e.message); }
  }

  const domainsPromise = db.prepare("SELECT id, source_domain, target_domain, zone_id, is_deep_resolve, ttl, notes, is_single_resolve, single_resolve_limit, single_resolve_node_names, strftime('%Y-%m-%dT%H:%M:%SZ', last_synced_time) as last_synced_time, last_sync_status, last_sync_error, is_enabled, is_system, displayed_records FROM domains ORDER BY is_system DESC, id").all();
  const ipSourcesPromise = db.prepare("SELECT id, url, github_path, commit_message, fetch_strategy, strftime('%Y-%m-%dT%H:%M:%SZ', last_synced_time) as last_synced_time, last_sync_status, last_sync_error, is_enabled FROM ip_sources ORDER BY github_path").all();
  
  const [{ results: domains }, { results: ipSources }] = await Promise.all([domainsPromise, ipSourcesPromise]);

  pageContent = await getDashboardPage(domains, ipSources, settings);
} else if (path === '/admin' && !loggedIn) {
  return new Response(null, { status: 302, headers: { 'Location': '/' } });
} else {
  pageTitle = 'CF-DNS-Clon';
  const domainsPromise = db.prepare("SELECT source_domain, target_domain, notes, last_synced_time, is_system, is_single_resolve, single_resolve_limit, last_synced_records FROM domains WHERE is_enabled = 1 ORDER BY is_system DESC, id").all();
  const ipSourcesPromise = db.prepare("SELECT url, github_path, last_synced_time FROM ip_sources WHERE is_enabled = 1 ORDER BY github_path").all();
  const threeNetworkSourcePromise = getSetting(db, 'THREE_NETWORK_SOURCE');
  const proxySettingsPromise = getProxySettings(db);

  const [{ results: domains }, { results: ipSources }, threeNetworkSource, proxySettings] = await Promise.all([domainsPromise, ipSourcesPromise, threeNetworkSourcePromise, proxySettingsPromise]);
  
  const sourceNameMap = { CloudFlareYes: 'CloudFlareYes', 'api.uouin.com': 'UoUin', 'wetest.vip': 'Wetest' };
  const sourceDisplayName = sourceNameMap[threeNetworkSource] || '未知';

  pageContent = getPublicHomepage(request.url, domains, ipSources, sourceDisplayName, loggedIn, proxySettings);
}
return new Response(getHtmlLayout(pageTitle, pageContent, { proxySettings: await getProxySettings(env.WUYA) }), { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
}

function getHtmlLayout(title, content, options = {}) { 
  const { proxySettings = {} } = options;
  return `<!DOCTYPE html><html lang="zh-CN" class="${proxySettings.APP_THEME || 'default'}"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>${title}</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css"><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css"><style>
:root {
  --sidebar-width: 250px;
  --pico-font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --pico-font-size: 16px;
  --pico-line-height: 1.6;
  --pico-border-radius: 12px;
  --pico-form-element-spacing-vertical: 1rem;
  --pico-form-element-spacing-horizontal: 1.25rem;
  --pico-shadow-sm: 0 2px 4px rgba(0,0,0,0.05);
  --pico-shadow-md: 0 4px 12px rgba(0,0,0,0.1);
  --pico-shadow-lg: 0 10px 30px rgba(0,0,0,0.1);
  --c-primary: #007aff;
  --c-primary-hover: #0056b3;
  --c-bg: #f0f2f5;
  --c-bg-blur: rgba(248, 249, 250, 0.7);
  --c-card-bg: rgba(255, 255, 255, 0.6);
  --c-card-border: rgba(255, 255, 255, 0.9);
  --c-text: #212529;
  --c-text-muted: #6c757d;
  --c-text-accent: var(--c-primary);
  --c-icon-bg: #e9ecef;
  --c-button-bg: var(--c-primary);
  --c-button-text: #ffffff;
  --noise-bg: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 800"><filter id="n"><feTurbulence type="fractalNoise" baseFrequency="0.65" numOctaves="3" stitchTiles="stitch"/></filter><rect width="100%" height="100%" filter="url(%23n)" opacity="0.1"/></svg>');
}
html.aurora-dark {
  --c-primary: #5e5ce6;
  --c-primary-hover: #7d7aff;
  --c-bg: #1c1c1e;
  --c-bg-blur: rgba(28, 28, 30, 0.7);
  --c-card-bg: rgba(44, 44, 46, 0.5);
  --c-card-border: rgba(255, 255, 255, 0.1);
  --c-text: #f2f2f7;
  --c-text-muted: #8e8e93;
  --c-text-accent: var(--c-primary);
  --c-icon-bg: #3a3a3c;
}
html.serene-light {
  --c-primary: #32ADEA;
  --c-primary-hover: #298cb8;
  --c-bg: #f7f9fc;
  --c-bg-blur: rgba(247, 249, 252, 0.7);
  --c-card-bg: rgba(255, 255, 255, 0.8);
  --c-card-border: rgba(0, 0, 0, 0.05);
  --c-text: #333;
  --c-text-muted: #6a737d;
  --pico-shadow-sm: 0 3px 6px rgba(0,0,0,0.04);
  --pico-shadow-md: 0 5px 15px rgba(0,0,0,0.06);
  --pico-shadow-lg: 0 10px 30px rgba(0,0,0,0.08);
}
body {
  font-family: var(--pico-font-family);
  margin: 0;
  background-color: var(--c-bg);
  color: var(--c-text);
  transition: background-color 0.3s ease, color 0.3s ease;
  overflow-x: hidden;
}
.public-body-wrapper {
  position: relative;
  z-index: 1;
  width: 100%;
  min-height: 100vh;
}
.background-blurs {
  position: fixed;
  top: 0; left: 0;
  width: 100%; height: 100%;
  overflow: hidden;
  z-index: 0;
}
html.aurora-dark .background-blurs::before { content:''; position:absolute; width:100%; height:100%; background:var(--noise-bg); opacity: 0.2; }
.blur-orb {
  position: absolute;
  border-radius: 50%;
  filter: blur(100px);
  opacity: 0.2;
  animation: move 20s infinite alternate;
}
.blur-orb-1 { width: 500px; height: 500px; top: 10%; left: 10%; background-color: #007aff; }
html.aurora-dark .blur-orb-1 { background-color: #5e5ce6; opacity: 0.4; }
html.serene-light .blur-orb-1 { background-color: #89CFF0; opacity: 0.3; }
.blur-orb-2 { width: 400px; height: 400px; bottom: 10%; right: 10%; background-color: #ff3b30; animation-delay: -10s;}
html.aurora-dark .blur-orb-2 { background-color: #ff375f; opacity: 0.4; }
html.serene-light .blur-orb-2 { background-color: #FFD580; opacity: 0.3; }

@keyframes move {
  from { transform: translate(-50px, -50px) rotate(0deg); }
  to { transform: translate(50px, 50px) rotate(360deg); }
}
main.container { max-width: none; padding: 0; display: flex; }
.sidebar { width: var(--sidebar-width); flex-shrink: 0; background: var(--pico-card-background-color); height: 100vh; position: sticky; top: 0; border-right: 1px solid var(--pico-card-border-color); display: flex; flex-direction: column; }
.sidebar-header { padding: 1.5rem; text-align: left; border-bottom: 1px solid var(--pico-card-border-color); }
.sidebar-header h3 { margin: 0; font-size: 1.75rem; font-weight: 700; color: var(--pico-primary); }
.sidebar-nav { padding: 1rem 0; flex-grow: 1; }
.sidebar-nav a { display: flex; align-items: center; gap: .85rem; padding: .85rem 1.5rem; color: #495057; text-decoration: none; margin: 0 .75rem; border-radius: 6px; transition: background-color .2s ease, color .2s ease; font-weight: 500; }
.sidebar-nav a:hover { background: #e9ecef; color: #212529; }
.sidebar-nav a.active { color: #fff; background-color: var(--pico-primary); font-weight: 600; }
.main-content { flex-grow: 1; padding: 2.5rem; }
.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; }
.page-header h2 { margin: 0; font-size: 2rem; font-weight: 700; }
.page { display: none; }
.page.active { display: block; animation: fadeIn .3s ease-out forwards; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
article, fieldset { border-radius: var(--pico-border-radius); border: 1px solid var(--pico-card-border-color); background: var(--pico-card-background-color); box-shadow: var(--pico-shadow-sm); padding: 2rem; }
dialog article { box-shadow: var(--pico-shadow-lg); }
legend { font-size: 1.25rem; font-weight: 600; padding: 0 .5rem; }
.domain-card { background-color: var(--pico-card-background-color); border: 1px solid var(--pico-card-border-color); border-radius: var(--pico-border-radius); padding: 1.5rem; display: grid; grid-template-columns: 2fr 1.5fr 1fr auto; gap: 1.5rem; align-items: center; transition: box-shadow .2s ease, transform .2s ease; margin-bottom: 1rem; }
.card-col { display: flex; flex-direction: column; gap: 0.2rem; min-width: 0; }
.card-col strong { display: flex; align-items: center; font-size: 0.75rem; color: #6c757d; margin-bottom: .25rem; text-transform: uppercase; font-weight: 600; letter-spacing: 0.05em; }
.card-col .domain-cell { font-size: 1rem; font-weight: 500; color: #212529; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; cursor: pointer; }
.card-col small.domain-cell { color: #6c757d; font-weight: 400; }
.card-actions { display: flex; justify-content: flex-end; gap: .5rem; }
.record-details summary { display: inline-flex; align-items: center; cursor: pointer; user-select: none; list-style: none; font-weight: 500; }
.record-details ul { margin: 8px 0 0; padding-left: 20px; font-size: 0.9em; }
.refresh-icon { cursor: pointer; margin-left: 8px; color: var(--pico-primary-light); transition: color .2s; }
.refresh-icon:hover { color: var(--pico-primary); }
.status-success { color: #198754; } .status-failed { color: #dc3545; } .status-no_change { color: #6c757d; }
.notifications, .home-toast { z-index: 1050; }
.public-nav {
  position: sticky;
  top: 1rem;
  max-width: 1200px;
  margin: 0 auto 2rem auto;
  padding: 0.75rem 1rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: var(--c-bg-blur);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  border: 1px solid var(--c-card-border);
  border-radius: var(--pico-border-radius);
  box-shadow: var(--pico-shadow-md);
  z-index: 100;
}
.public-nav-title { font-size: 1.25rem; font-weight: 600; color: var(--c-text); }
.public-nav-actions { display: flex; align-items: center; gap: 1rem; }
.public-nav-actions a, .public-nav-actions button {
  text-decoration: none;
  padding: 0.5rem 1rem;
  border-radius: 8px;
  font-weight: 500;
  transition: all 0.2s ease;
  border: none;
}
.public-nav-actions a { background-color: var(--c-button-bg); color: var(--c-button-text); }
.public-nav-actions a:hover { filter: brightness(1.1); }
.subscription-buttons { display: flex; gap: 0.5rem; }
.subscription-buttons button { padding: 0.5rem 0.8rem; font-size: 0.9rem; background-color: transparent; border: 1px solid var(--c-button-bg); color: var(--c-text-accent); }
.subscription-buttons button:hover { background-color: var(--c-button-bg); color: var(--c-button-text); }
.public-container { max-width: 1200px; margin: 0 auto; padding: 1.5rem; }
.public-section h2 { font-size: 1.75rem; margin-bottom: 2rem; display: flex; align-items: center; gap: 0.75rem; color: var(--c-text); border: none; padding: 0;}
.public-section h2::before { content: ''; display: block; width: 5px; height: 1.25rem; background-color: var(--c-primary); border-radius: 3px; }
.public-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 1.5rem; }
.public-card {
  background: var(--c-card-bg);
  backdrop-filter: blur(5px);
  -webkit-backdrop-filter: blur(5px);
  border: 1px solid var(--c-card-border);
  border-radius: var(--pico-border-radius);
  box-shadow: var(--pico-shadow-md);
  padding: 1.5rem;
  cursor: pointer;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
  display: flex;
  flex-direction: column;
  gap: 1rem;
}
.public-card:hover { transform: translateY(-5px); box-shadow: var(--pico-shadow-lg); }
.public-card-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; }
.public-card-title { font-size: 1.1rem; font-weight: 600; color: var(--c-text); margin: 0; }
.public-card-meta { font-size: 0.8rem; color: var(--c-text-muted); white-space: nowrap; }
.public-card-content { font-family: "SF Mono", "Consolas", "Menlo", monospace; font-size: 1rem; color: var(--c-text-accent); word-break: break-all; }
.public-card-footer { font-size: 0.85rem; color: var(--c-text-muted); display: flex; align-items: center; gap: 0.5rem; }
.home-toast { position: fixed; top: 80px; right: 20px; background-color: var(--c-primary); color: #fff; padding: 12px 20px; border-radius: 6px; z-index: 1000; font-weight: 500; box-shadow: var(--pico-shadow-lg); transition: transform 0.4s ease-in-out, opacity 0.4s ease-in-out; transform: translateY(-100px); opacity: 0; }
.home-toast.show { transform: translateY(0); opacity: 1; }
.home-toast.hide { transform: translateY(50px); opacity: 0; }
.snippets-help-btn { margin-left: .5rem; cursor: pointer; font-size: .8em; font-weight: normal; }
.code-block { background-color: #f5f5f5; border: 1px solid #ddd; border-radius: 4px; padding: 1rem; margin-top: 1rem; position: relative; }
.code-block-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: .5rem; }
.code-block-title { font-weight: bold; }
.copy-btn { padding: .25rem .5rem; font-size: .8rem; }
.line-numbers { float: left; text-align: right; margin-right: 1rem; padding-right: 1rem; border-right: 1px solid #ddd; user-select: none; color: #999; }

@media (max-width: 768px) {
  .public-nav { top: 0; left: 0; right: 0; border-radius: 0; width: 100%; flex-wrap: wrap; padding-bottom: 0; }
  .public-nav-title, .public-nav-actions { width: 50%; }
  .public-nav-actions { justify-content: flex-end; }
  .subscription-buttons-container-mobile { padding: 0.75rem 1rem; width: 100%; order: 3; }
  .subscription-buttons { justify-content: center; }
  .public-container { padding: 1rem; }
  .public-grid { grid-template-columns: 1fr; }
  main.container {
      display: block;
  }
  .sidebar {
      width: 100%;
      height: auto;
      position: static;
      border-right: none;
      border-bottom: 1px solid var(--pico-card-border-color);
  }
  .main-content {
      padding: 1.5rem 1rem;
  }
  .page-header {
      flex-direction: column;
      gap: 1rem;
      align-items: flex-start;
  }
  .page-header h2 {
      font-size: 1.75rem;
  }
  .domain-card {
      grid-template-columns: 1fr;
      gap: 1rem;
      padding: 1rem;
  }
  .card-actions {
      flex-direction: row;
      justify-content: flex-start;
      flex-wrap: wrap;
  }
}
</style></head><body><main class="container">${content}</main><div id="notifications" class="notifications"></div></body></html>`; }

function getSetupPage() { return `<div class="auth-container"><article id="setupForm"><h1>系统初始化</h1><p>请设置一个安全的管理员密码以保护您的应用。</p><form><label for="password">管理员密码 (最少8位)</label><input type="password" id="password" required minlength="8"><button type="submit">设置密码</button></form><p id="error-msg" style="color:red"></p></article></div><script>document.querySelector('#setupForm form').addEventListener('submit',async function(e){e.preventDefault();const password=document.getElementById('password').value;document.getElementById('error-msg').textContent='';try{const res=await fetch('/api/setup',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password})});if(!res.ok){const err=await res.json();throw new Error(err.error||'设置失败')}alert('设置成功，页面即将刷新...');setTimeout(()=>window.location.reload(),1000)}catch(e){document.getElementById('error-msg').textContent=e.message}});</script>`;}

function getPublicHomepage(requestUrl, domains, ipSources, threeNetworkSourceName, loggedIn, proxySettings) {
  const origin = new URL(requestUrl).origin;
  const formatTime = (isoStr) => {
      if (!isoStr) return 'N/A';
      const date = new Date(isoStr);
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      return `${year}-${month}-${day}`;
  };

  const domainCards = domains.flatMap(d => {
      let sourceHost;
      if (d.is_system) {
          sourceHost = threeNetworkSourceName;
      } else {
          try {
              let urlCompatibleSource = d.source_domain;
              if (!urlCompatibleSource.startsWith('http://') && !urlCompatibleSource.startsWith('https://')) {
                  urlCompatibleSource = 'https://' + urlCompatibleSource;
              }
              sourceHost = new URL(urlCompatibleSource).hostname;
          } catch (e) {
              sourceHost = d.source_domain || '解析错误';
          }
      }
      
      const mainCard = `
      <div class="public-card" data-copy-content="${d.target_domain}">
          <div class="public-card-header">
              <h3 class="public-card-title">${d.notes || '未知线路'}</h3>
              <span class="public-card-meta">${formatTime(d.last_synced_time)}</span>
          </div>
          <div class="public-card-content">${d.target_domain}</div>
          <div class="public-card-footer">
              <i class="fa-solid fa-link fa-xs"></i> <span>来源: ${sourceHost}</span>
          </div>
      </div>`;
      
      const cards = [mainCard];

      if (d.is_single_resolve) {
          try {
              const targetParts = d.target_domain.split('.');
              if (targetParts.length > 1) {
                  const targetPrefix = targetParts[0];
                  const zone = targetParts.slice(1).join('.');
                  const records = JSON.parse(d.last_synced_records || '[]');
                  const limit = d.single_resolve_limit || 5;
                  const finalRecords = records.slice(0, limit);

                  finalRecords.forEach((record, index) => {
                      const singleDomain = `${targetPrefix}.${index + 1}.${zone}`;
                      const singleCard = `
                      <div class="public-card" data-copy-content="${singleDomain}">
                          <div class="public-card-header">
                              <h3 class="public-card-title">${d.notes || '未知线路'} #${index + 1}</h3>
                              <span class="public-card-meta">${formatTime(d.last_synced_time)}</span>
                          </div>
                          <div class="public-card-content">${singleDomain}</div>
                          <div class="public-card-footer">
                              <i class="fa-solid fa-link fa-xs"></i> <span>来源: ${sourceHost}</span>
                          </div>
                      </div>`;
                      cards.push(singleCard);
                  });
              }
          } catch (e) {
              console.error("Error generating single-resolve cards:", e);
          }
      }
      return cards;
  }).join('');

  const ipSourceCards = ipSources.map(s => {
      const fullUrl = `${origin}/${s.github_path}`;
      return `
      <div class="public-card" data-copy-content="${fullUrl}">
          <div class="public-card-header">
              <h3 class="public-card-title">${s.github_path}</h3>
              <span class="public-card-meta">${formatTime(s.last_synced_time)}</span>
          </div>
          <div class="public-card-content">${fullUrl}</div>
          <div class="public-card-footer">
              <i class="fa-solid fa-link fa-xs"></i> <span>来源: ${new URL(s.url).hostname}</span>
          </div>
      </div>`;
  }).join('');

  const authButton = loggedIn 
      ? `<a href="/admin" role="button">进入后台</a>`
      : `<button class="outline" onclick="document.getElementById('login-modal').showModal()">登录</button>`;
  
  const subscriptionButtonsHTML = (proxySettings.publicSubscription && proxySettings.enableWsReverseProxy) ? `
      <div class="subscription-buttons" id="sub-buttons-desktop">
          <button data-sub-type="xray">Xray</button>
          <button data-sub-type="clash">Clash</button>
          <button data-sub-type="singbox">Sing-Box</button>
          <button data-sub-type="surge">Surge</button>
      </div>
      <div class="subscription-buttons-container-mobile" id="sub-buttons-mobile-container" style="display: none;">
          <div class="subscription-buttons" id="sub-buttons-mobile">
              <button data-sub-type="xray">Xray</button>
              <button data-sub-type="clash">Clash</button>
              <button data-sub-type="singbox">Sing-Box</button>
              <button data-sub-type="surge">Surge</button>
          </div>
      </div>
  ` : '';

  return `
  <div class="public-body-wrapper">
      <div class="background-blurs">
          <div class="blur-orb blur-orb-1"></div>
          <div class="blur-orb blur-orb-2"></div>
      </div>
      <nav class="public-nav">
          <div class="public-nav-title">CF-DNS-Clon</div>
          ${subscriptionButtonsHTML}
          <div class="public-nav-actions">
              ${authButton}
          </div>
      </nav>
      <div class="public-container">
          <section class="public-section">
              <h2><i class="fa-solid fa-globe"></i> 优选域名</h2>
              <div class="public-grid">${domainCards || '<p>暂无可用数据</p>'}</div>
          </section>
          <section class="public-section">
              <h2><i class="fa-solid fa-server"></i> 优选API</h2>
              <div class="public-grid">${ipSourceCards || '<p>暂无可用数据</p>'}</div>
          </section>
      </div>
  </div>
  <div id="copy-toast" class="home-toast"></div>
  
  <dialog id="login-modal">
  <article>
      <header>
      <a href="#close" aria-label="Close" class="close" onclick="document.getElementById('login-modal').close()"></a>
      <h3>管理员登录</h3>
      </header>
      <p>请输入密码以继续。</p>
      <form id="modal-login-form">
      <label for="modal-password">密码</label>
      <input type="password" id="modal-password" name="password" required>
      <p id="modal-error-msg" style="color: var(--pico-color-red-500); height: 1em;"></p>
      <button type="submit">登录</button>
      </form>
  </article>
  </dialog>

  <script>
      let currentSettings = ${JSON.stringify({proxySettings})};
      const toast = document.getElementById('copy-toast');
      
      function showToast(message) {
          toast.textContent = message;
          toast.classList.add('show');
          setTimeout(() => {
              toast.classList.remove('show');
              toast.classList.add('hide');
              setTimeout(() => toast.classList.remove('hide'), 400);
          }, 2000);
      }

      document.querySelector('.public-container').addEventListener('click', (event) => {
          const card = event.target.closest('.public-card');
          if (card && card.dataset.copyContent) {
              const content = card.dataset.copyContent;
              navigator.clipboard.writeText(content).then(() => {
                  showToast('已复制: ' + content);
              }, () => {
                  showToast('复制失败！');
              });
          }
      });
      
      document.getElementById('modal-login-form').addEventListener('submit', async (e) => {
          e.preventDefault();
          const password = document.getElementById('modal-password').value;
          const errorMsg = document.getElementById('modal-error-msg');
          const submitBtn = e.target.querySelector('button');
          errorMsg.textContent = '';
          submitBtn.setAttribute('aria-busy', 'true');
          try {
              const res = await fetch('/api/login', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ password })
              });
              if (!res.ok) {
                  const err = await res.json();
                  throw new Error(err.error || '登录失败');
              }
              window.location.href = '/admin';
          } catch (e) {
              errorMsg.textContent = e.message;
          } finally {
              submitBtn.removeAttribute('aria-busy');
          }
      });

      function setupSubscriptionButtons() {
          function generateAndCopySubLink(subType) {
              const ps = currentSettings.proxySettings || {};
              let subId;
              if (ps.subUseRandomId) {
                  const len = ps.subIdLength || 12;
                  const charset = ps.subIdCharset || 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
                  subId = Array.from({length: len}, () => charset.charAt(Math.floor(Math.random() * charset.length))).join('');
              } else {
                  subId = ps.subCustomId || '';
                  if (!subId) {
                      showToast("请在后台设置自定义ID");
                      return;
                  }
              }
              const finalUrl = \`\${window.location.origin}/\${subId}/\${subType}\`;
              navigator.clipboard.writeText(finalUrl).then(() => {
                  showToast(\`\${subType.charAt(0).toUpperCase() + subType.slice(1)} 订阅已复制\`);
              }).catch(err => {
                  showToast('复制失败!');
              });
          }
          document.querySelectorAll('.subscription-buttons button').forEach(btn => {
              btn.addEventListener('click', (e) => {
                  generateAndCopySubLink(e.target.dataset.subType);
              });
          });

          function handleResize() {
              const mobileContainer = document.getElementById('sub-buttons-mobile-container');
              const desktopContainer = document.getElementById('sub-buttons-desktop');
              if (window.innerWidth <= 768) {
                  if (mobileContainer) mobileContainer.style.display = 'block';
                  if (desktopContainer) desktopContainer.style.display = 'none';
              } else {
                  if (mobileContainer) mobileContainer.style.display = 'none';
                  if (desktopContainer) desktopContainer.style.display = 'flex';
              }
          }
          window.addEventListener('resize', handleResize);
          handleResize();
      }
      if (document.querySelector('.subscription-buttons')) {
          setupSubscriptionButtons();
      }

  </script>
  `;
}

function getProxySettingsPageHTML() {
  return `
      <div id="proxy-settings-content">
          <div style="margin-bottom: 2rem; display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem;">
              <button class="outline" id="copyXraySub">复制 Xray 订阅</button>
              <button class="outline" id="copyClashSub">复制 Clash 订阅</button>
              <button class="outline" id="copySingboxSub">复制 Sing-Box 订阅</button>
              <button class="outline" id="copySurgeSub">复制 Surge 订阅</button>
          </div>

          <fieldset>
              <legend>
                  <i class="fa-solid fa-bolt"></i> Websocket 反向代理
                  <a class="snippets-help-btn" onclick="window.openModal('snippetsModal')">( 如何部署 Snippets 呢？)</a>
              </legend>
              <label for="enableWsReverseProxy">
                  <input type="checkbox" id="enableWsReverseProxy" name="enableWsReverseProxy" role="switch">
                  <strong>启用 Websocket 反代理</strong>
              </label>
              <div id="wsReverseProxyConfig" style="display: none; margin-top: 1.5rem; border-left: 2px solid var(--pico-primary); padding-left: 1.5rem;">
                  <div class="form-group">
                      <label for="wsReverseProxyUrl">反代目标 WebSocket 地址:</label>
                      <input type="text" id="wsReverseProxyUrl" name="wsReverseProxyUrl" placeholder="例如: wss://example.com">
                      <small>您的 VLESS 节点的 WebSocket 流量将被转发到此地址。</small>
                  </div>
                  <div class="form-group">
                      <label for="wsReverseProxyPath">Path:</label>
                      <input type="text" id="wsReverseProxyPath" name="wsReverseProxyPath" placeholder="例如: /proxyip=proxyip.sg.cmliussss.net">
                      <small>VLESS 客户端中配置的 WebSocket 路径。</small>
                  </div>
                  <div class="form-group">
                      <label for="proxyIpRegionSelector">设置你的proxyip地区 (快速设置Path):</label>
                      <select id="proxyIpRegionSelector">
                          <option value="">---请选择地区---</option>
                          <option value="proxyip.us.cmliussss.net">美国</option>
                          <option value="proxyip.sg.cmliussss.net">新加坡</option>
                          <option value="proxyip.jp.cmliussss.net">日本</option>
                          <option value="proxyip.hk.cmliussss.net">香港</option>
                          <option value="proxyip.kr.cmliussss.net">韩国</option>
                          <option value="proxyip.se.cmliussss.net">瑞典</option>
                          <option value="proxyip.nl.cmliussss.net">荷兰</option>
                          <option value="proxyip.fi.cmliussss.net">芬兰</option>
                          <option value="proxyip.gb.cmliussss.net">英国</option>
                          <option value="proxyip.oracle.cmliussss.net">美国 (Oracle)</option>
                          <option value="proxyip.digitalocean.cmliussss.net">美国 (DigitalOcean)</option>
                          <option value="proxyip.vultr.cmliussss.net">美国 (Vultr)</option>
                          <option value="proxyip.multacom.cmliussss.net">美国 (Multacom)</option>
                          <option value="sjc.o00o.ooo">美国 (sjc)</option>
                          <option value="tw.ttzxw.cf">台湾</option>
                          <option value="cdn-akamai-jp.tgdaosheng.v6.rocks">日本 (Akamai)</option>
                      </select>
                  </div>
                  <div class="grid">
                      <div>
                          <label>
                              <input type="radio" name="proxyUuidOption" id="wsReverseProxyUseRandomUuid" value="random"> 随机UUID
                          </label>
                      </div>
                      <div>
                          <label>
                              <input type="radio" name="proxyUuidOption" id="wsReverseProxyUseSpecificUuid" value="specific"> 指定UUID
                          </label>
                      </div>
                  </div>
                  <div class="form-group" id="wsReverseProxySpecificUuidContainer" style="display:none;">
                      <label for="wsReverseProxyUuidValue">UUID:</label>
                      <input type="text" id="wsReverseProxyUuidValue" name="wsReverseProxySpecificUuid" placeholder="请输入有效的 UUID">
                  </div>
                  <hr>
                  <label><strong>SNI & Host 设置</strong></label>
                  <div class="grid">
                  <div>
                      <label>
                          <input type="radio" name="sniOption" id="useSelfUrlForSni" value="self">
                          使用自身的URL
                      </label>
                  </div>
                  <div>
                      <label>
                          <input type="radio" name="sniOption" id="useProxyUrlForSni" value="proxy">
                          使用反代的URL
                      </label>
                  </div>
              </div>
              </div>
          </fieldset>

          <fieldset>
              <legend><i class="fa-solid fa-pencil"></i> 自定义节点</legend>
              <div class="form-group">
                  <label for="customNodes">每行一个节点 (地址:端口#名称@路径)</label>
                  <textarea id="customNodes" name="customNodes" rows="4" placeholder="1.1.1.1:10086#乌鸦@/?ed=2560\nexample.com:443#示例"></textarea>
                  <small>@路径 部分为可选，如果留空则使用上方 Websocket 设置中的全局 Path。</small>
              </div>
          </fieldset>

          <fieldset>
              <legend><i class="fa-solid fa-cloud-download"></i> 外部订阅</legend>
              <div class="grid">
                  <div>
                      <label>
                          <input type="radio" name="filterMode" id="filterModeNone" value="none">
                          不过滤
                      </label>
                  </div>
                  <div>
                      <label>
                          <input type="radio" name="filterMode" id="filterModeGlobal" value="global">
                          过滤规则-全部
                      </label>
                  </div>
                  <div>
                      <label>
                          <input type="radio" name="filterMode" id="filterModeIndividual" value="individual">
                          过滤规则-单个
                      </label>
                  </div>
              </div>
              <div id="global-filters-container" style="display:none; margin-top:1rem;">
                  <label for="globalFilters">全局过滤规则:</label>
                  <textarea id="globalFilters" name="globalFilters" rows="3" placeholder="#M:名称=新名称\n#H:地址=新地址\n#T:前缀\n#W:后缀"></textarea>
              </div>
              <div id="external-subs-container" style="margin-top: 1rem;"></div>
              <button id="add-sub-btn" class="outline" style="margin-top: 1rem;">+ 添加订阅</button>
          </fieldset>

          <fieldset>
              <legend><i class="fa-solid fa-link"></i> 订阅区域</legend>
              <div class="form-group">
                  <label for="sublinkWorkerUrl">订阅转换 Worker 地址:</label>
                  <input type="text" id="sublinkWorkerUrl" name="sublinkWorkerUrl" placeholder="例如: https://sub.example.com">
              </div>
              <div class="grid">
                  <div>
                      <label for="publicSubscriptionToggle">
                          <input type="checkbox" id="publicSubscriptionToggle" name="publicSubscription" role="switch">
                          首页显示订阅
                      </label>
                  </div>
              </div>
              <div class="grid">
                  <div>
                      <label><input type="radio" name="subIdOption" id="subUseRandomId" value="random"> 随机ID</label>
                      <label for="subIdLength">长度:</label>
                      <input type="number" id="subIdLength" name="subIdLength" min="1" max="32" value="12">
                  </div>
                  <div>
                      <label><input type="radio" name="subIdOption" id="subUseCustomId" value="custom"> 自定义ID</label>
                      <label for="subCustomId" class="visually-hidden">自定义ID内容</label>
                      <input type="text" id="subCustomId" name="subCustomId" placeholder="自定义内容">
                  </div>
              </div>
              <div class="form-group">
                  <label for="subIdCharset">ID使用字符集:</label>
                  <textarea id="subIdCharset" name="subIdCharset" rows="2"></textarea>
              </div>
          </fieldset>
      </div>
  `;
}


async function getDashboardPage(domains, ipSources, settings) { 
  const githubSettingsComplete = settings.GITHUB_TOKEN && settings.GITHUB_OWNER && settings.GITHUB_REPO;

  const snippetsUrl = 'https://raw.githubusercontent.com/crow1874/CF-DNS-Clone/refs/heads/main/SRC/snippets.js';
  let snippetsCode = '/* Failed to fetch snippets code from GitHub. */';
  try {
      const response = await fetch(snippetsUrl);
      if (response.ok) {
          snippetsCode = await response.text();
      } else {
          snippetsCode = `/* Failed to fetch snippets: ${response.status} ${response.statusText} */`;
      }
  } catch (e) {
      console.error("Failed to fetch snippets code:", e);
      snippetsCode = `/* Error fetching snippets: ${e.message} */`;
  }

  return `<aside class="sidebar">
      <div class="sidebar-header"><h3>DNS Clone</h3></div>
      <nav class="sidebar-nav">
          <a href="#page-dns-clone" class="nav-link active" data-target="page-dns-clone"><i class="fa-solid fa-clone fa-fw"></i> 域名克隆</a>
          <a href="#page-github-upload" class="nav-link" data-target="page-github-upload"><i class="fa-brands fa-github fa-fw"></i> GitHub 上传</a>
          <a href="#page-proxy-settings" class="nav-link" data-target="page-proxy-settings"><i class="fa-solid fa-server fa-fw"></i> 代理设置</a>
          <a href="#page-settings" class="nav-link" data-target="page-settings"><i class="fa-solid fa-gear fa-fw"></i> 系统设置</a>
      </nav>
  </aside>
  <div class="main-content">
      <div id="page-dns-clone" class="page active">
          <div class="page-header"><h2>域名克隆列表</h2><button id="addDomainBtn" ${settings.CF_ZONE_ID ? '' : 'disabled'}>＋ 添加克隆目标</button></div>
          <div id="domain-list-container"></div>
          <article><h3>手动操作</h3><p>点击下方按钮，可以立即为所有已启用的目标执行一次同步任务。</p><button id="manualSyncBtn">手动同步所有目标</button><pre id="logOutput" style="display:none;"></pre></article>
      </div>
      <div id="page-github-upload" class="page">
          <div class="page-header"><h2>GitHub IP源列表</h2><button id="addIpSourceBtn" ${githubSettingsComplete ? '' : 'disabled'}>＋ 添加IP源</button></div>
          <div id="ip-source-list-container"></div>
          <article><h3>手动操作</h3><p>点击下方按钮，可以立即为所有已启用的IP源执行一次同步并上传到GitHub。</p><button id="manualSyncIpSourcesBtn">同步所有IP源</button><pre id="ipLogOutput" style="display:none;"></pre></article>
      </div>
      <div id="page-proxy-settings" class="page">
          <div class="page-header"><h2>代理设置</h2></div>
          ${getProxySettingsPageHTML()}
      </div>
      <div id="page-settings" class="page">
          <div class="page-header"><h2>系统设置</h2></div>
          <form id="settingsForm">
              <fieldset>
                  <legend><i class="fa-solid fa-palette"></i> 外观</legend>
                  <label for="appThemeSelector">主题选择</label>
                  <select id="appThemeSelector" name="APP_THEME">
                      <option value="default">默认 (Pico)</option>
                      <option value="aurora-dark">极光暗色 (Aurora Dark)</option>
                      <option value="serene-light">静谧浅色 (Serene Light)</option>
                  </select>
              </fieldset>
              <fieldset><legend><i class="fa-brands fa-cloudflare"></i> Cloudflare API 设置</legend><label for="cfToken">API 令牌 (Token)</label><input type="password" id="cfToken" value="${settings.CF_API_TOKEN||''}"><label for="cfZoneId">区域 (Zone) ID</label><input type="text" id="cfZoneId" value="${settings.CF_ZONE_ID||''}">
                  <details class="tutorial-details"><summary>如何获取 API 令牌和区域 ID？</summary><div class="tutorial-content"><ol><li><strong>获取 API 令牌 (Token):</strong><ol type="a"><li>登录 <a href="https://dash.cloudflare.com/" target="_blank">Cloudflare</a>，进入 <strong>“我的个人资料”</strong> &rarr; <strong>“API 令牌”</strong>。</li><li>点击 <strong>“创建令牌”</strong>，然后选择 <strong>“编辑区域 DNS”</strong> 模板并点击“使用模板”。</li><li>在 <strong>“区域资源”</strong> 部分，选择您需要操作的具体域名区域。</li><li>点击“继续以显示摘要”和“创建令牌”，复制生成的令牌。<strong>注意：令牌仅显示一次，请妥善保管。</strong></li></ol></li><li><strong>获取区域 ID (Zone ID):</strong><ol type="a"><li>在 Cloudflare 仪表板主页，点击您需要操作的域名。</li><li>在域名的“概述”页面，您可以在右下角找到 <strong>“区域 ID”</strong>，点击即可复制。</li></ol></li></ol></div></details>
              </fieldset>
              <fieldset><legend><i class="fa-solid fa-network-wired"></i> 三网优选IP源设置</legend><label for="threeNetworkSource">三网采集源</label><select id="threeNetworkSource"><option value="CloudFlareYes" ${settings.THREE_NETWORK_SOURCE === 'CloudFlareYes' ? 'selected' : ''}>CloudFlareYes</option><option value="api.uouin.com" ${settings.THREE_NETWORK_SOURCE === 'api.uouin.com' ? 'selected' : ''}>api.uouin.com</option><option value="wetest.vip" ${settings.THREE_NETWORK_SOURCE === 'wetest.vip' ? 'selected' : ''}>wetest.vip</option></select><small>为系统预设的电信/移动/联通域名选择IP来源。更改后保存设置将自动同步一次系统域名。</small>
              </fieldset>
              <fieldset><legend><i class="fa-brands fa-github"></i> GitHub API 设置</legend><label for="githubToken">GitHub Token</label><input type="password" id="githubToken" value="${settings.GITHUB_TOKEN||''}" placeholder="具有 repo 权限的 Personal Access Token"><label for="githubOwner">GitHub 用户名/组织名</label><input type="text" id="githubOwner" value="${settings.GITHUB_OWNER||''}" placeholder="例如: my-username"><label for="githubRepo">仓库名称</label><input type="text" id="githubRepo" value="${settings.GITHUB_REPO||''}" placeholder="例如: my-dns-records">
                  <details class="tutorial-details"><summary>如何获取 GitHub API 信息？</summary><div class="tutorial-content"><ol><li><strong>获取 GitHub Token:</strong><ol type="a"><li>登录 <a href="https://github.com/" target="_blank">GitHub</a>，点击右上角头像，进入 <strong>“Settings”</strong>。</li><li>在左侧菜单中，选择 <strong>“Developer settings”</strong> &rarr; <strong>“Personal access tokens”</strong> &rarr; <strong>“Tokens (classic)”</strong>。</li><li>点击 <strong>“Generate new token”</strong>，并选择 <strong>“Generate new token (classic)”</strong>。</li><li>为令牌添加描述（Note），设置合适的过期时间（Expiration）。</li><li>在 <strong>“Select scopes”</strong> 部分，勾选 <code>repo</code> 权限。</li><li>点击页面底部的 <strong>“Generate token”</strong>，并复制生成的令牌。<strong>注意：令牌仅显示一次，请妥善保管。</strong></li></ol></li><li><strong>获取用户名/组织名 和 仓库名称:</strong><ol type="a"><li><strong>用户名/组织名</strong> 就是您的GitHub个人主页URL中，github.com后面的那部分，或者您组织的主页URL。</li><li><strong>仓库名称</strong> 是您在GitHub上创建的，用来存储IP文件的仓库的名字。如果仓库不存在，系统将在第一次同步时自动为您创建为私有仓库。</li></ol></li></ol></div></details>
              </fieldset>
              <button type="submit">保存设置</button>
          </form>
      </div>
  </div>
  <dialog id="domainModal"><article><header><a href="#close" aria-label="Close" class="close" onclick="window.closeModal('domainModal')"></a><h3 id="modalTitle"></h3></header><form id="domainForm"><input type="hidden" id="domainId"><label for="source_domain">克隆域名</label><input type="text" id="source_domain" placeholder="example-source.com" required><label for="target_domain_prefix">我的域名前缀</label><div class="grid"><input type="text" id="target_domain_prefix" placeholder="subdomain or @" required><span id="zoneNameSuffix" style="line-height:var(--pico-form-element-height);font-weight:700">.your-zone.com</span></div><div class="grid"><div><label for="is_deep_resolve">深度 <span class="tooltip">(?)<span class="tooltip-text">开启后，如果克隆域名是CNAME，系统将递归查找最终的IP地址进行解析。关闭则直接克隆CNAME记录本身。</span></span></label><input type="checkbox" id="is_deep_resolve" role="switch" checked></div><div><label for="ttl">TTL (秒)</label><input type="number" id="ttl" min="60" max="86400" value="60" required></div></div><label for="notes">备注 (可选)</label><textarea id="notes" rows="2" placeholder="例如：主力CDN"></textarea><div class="grid"><div><label for="is_single_resolve">单个解析<span class="tooltip">(?)<span class="tooltip-text">开启后，将为每个解析出的IP（根据数量上限）创建单独的子域名。例如 bp1 -> bp1.1, bp1.2 ...</span></span></label><input type="checkbox" id="is_single_resolve" role="switch"></div><div id="single_resolve_limit_container" style="display: none;"><label for="single_resolve_limit">数量上限</label><input type="number" id="single_resolve_limit" min="1" max="50" value="5" required></div></div><div id="single_node_names_container" style="display:none; margin-top: 1rem;"><label><strong>节点名称 (可选)</strong></label></div><footer><button type="button" class="secondary" onclick="window.closeModal('domainModal')">取消</button><button type="submit" id="saveBtn">保存</button></footer></form></article></dialog>
  <dialog id="ipSourceModal"><article><header><a href="#close" aria-label="Close" class="close" onclick="window.closeModal('ipSourceModal')"></a><h3 id="ipSourceModalTitle"></h3></header><form id="ipSourceForm"><input type="hidden" id="ipSourceId"><div class="grid"><label for="ip_source_url">IP源地址</label><button type="button" class="outline" id="probeBtn" style="width:auto;padding:0 1rem;">探测方案</button></div><input type="text" id="ip_source_url" placeholder="https://example.com/ip_list.txt" required><progress id="probeProgress" style="display:none;"></progress><p id="probeResult" style="font-size:0.9em;"></p><label for="github_path">GitHub 文件路径</label><input type="text" id="github_path" placeholder="IP/Cloudflare.txt" required><label for="commit_message">Commit 信息</label><input type="text" id="commit_message" placeholder="Update Cloudflare IPs" required><footer><button type="button" class="secondary" onclick="window.closeModal('ipSourceModal')">取消</button><button type="submit" id="saveIpSourceBtn">保存</button></footer></form></article></dialog>
  <dialog id="snippetsModal"><article>
      <header><a href="#close" aria-label="Close" class="close" onclick="window.closeModal('snippetsModal')"></a><h3>如何部署 Snippets 反代？</h3></header>
      <p>Snippets 是 Cloudflare 提供的一项功能，可以在边缘节点执行轻量级代码，非常适合用于反向代理。</p>
      <ol>
          <li><strong>检查权限</strong>：登录您的 Cloudflare 账户，选择一个已绑定的域名。在左侧菜单中点击 <strong>规则 → Snippets</strong>。如果您能看到创建和管理界面，说明您拥有使用权限。</li>
          <li><strong>备用方案</strong>：如果您的账户没有 Snippets 权限，使用 Workers 部署也能达到相同的效果。请参考 <a href="https://github.com/cmliu/WorkerVless2sub" target="_blank">CMliu</a> 或 <a href="https://github.com/6Kmfi6HP/Sp" target="_blank">6Kmfi6HP</a> 的项目获取 Workers 版本的代码。</li>
          <li><strong>部署 Snippets 代码</strong>：
              <div class="code-block">
                  <div class="code-block-header">
                      <span class="code-block-title">Snippets 代码</span>
                      <button class="secondary outline copy-btn" onclick="window.copyCode(this, 'snippets-code-content')">复制</button>
                  </div>
                  <pre id="snippets-code-content"><code>${snippetsCode.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</code></pre>
              </div>
          </li>
      </ol>
  </article></dialog>
  <script>${getDashboardScript(domains, ipSources, settings)}</script>`;
}
function getDashboardScript(domains, ipSources, settings) { return `
function showNotification(message, type = 'info', duration = 5000) {
  const container = document.getElementById('notifications');
  const toast = document.createElement('div');
  toast.className = \`toast toast-\${type}\`;
  toast.innerHTML = \`<div>\${message}</div>\`;
  container.appendChild(toast);
  setTimeout(() => toast.classList.add('show'), 10);
  setTimeout(() => {
      toast.classList.add('hide');
      toast.addEventListener('transitionend', e => {
          if (e.target === toast) toast.remove();
      }, { once: true });
  }, duration);
}
async function apiFetch(url, options = {}) { const res = await fetch(url, { headers: { "Content-Type": "application/json", ...options.headers }, ...options }); if (!res.ok) { const errData = await res.json().catch(() => ({ error: \`HTTP 错误: \${res.status}\` })); if (res.status === 401) { showNotification('会话已过期，请重新登录。', 'error'); setTimeout(() => window.location.href = '/login', 2000); } throw new Error(errData.error); } try { return await res.json(); } catch (e) { return {}; } }
let currentDomains = ${JSON.stringify(domains)};
let currentIpSources = ${JSON.stringify(ipSources)};
let currentSettings = ${JSON.stringify(settings)};
let zoneName = currentSettings.zoneName || '';
let successfulProbeStrategy = null;
let saveProxySettingsTimeout;

const formatBeijingTime = (isoStr) => { if (!isoStr) return '从未'; const d = new Date(isoStr); return new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(d).replace(/\\//g, '-'); };

function renderLiveRecords(records) {
  if (!records || records.length === 0) return '无记录';
  const first = records[0];
  if (records.length === 1) return \`<b>\${first.type}:</b> \${first.content}\`;
  return \`<details class="record-details"><summary><b>\${first.type}:</b> (共 \${records.length} 条)</summary><ul>\${records.map(r => \`<li><b>\${r.type}:</b> \${r.content}</li>\`).join('')}</ul></details>\`;
}

function renderStatus(item) { switch (item.last_sync_status) { case 'success': return \`<span class="status-success">✔ 同步成功</span>\`; case 'failed': return \`<span class="status-failed" title="\${item.last_sync_error || '未知错误'}">✖ 同步失败</span>\`; case 'no_change': return \`<span class="status-no_change">✔ 内容一致</span>\`; default: return '○ 待定'; } }

function renderDomainCard(domain) {
  let prefix = domain.target_domain;
  if (zoneName && domain.target_domain.endsWith('.' + zoneName)) {
      prefix = domain.target_domain.substring(0, domain.target_domain.length - (zoneName.length + 1));
  } else if (zoneName && domain.target_domain === zoneName) {
      prefix = '@';
  }
  const displayContent = domain.notes ? \`<strong>\${domain.notes}</strong>\` : \`<span class="domain-cell" title="\${domain.target_domain}">\${prefix}</span>\`;
  const isSystem = domain.is_system;
  const systemClass = isSystem ? 'system-domain' : '';
  const sourceDisplay = isSystem ? '系统内置' : domain.source_domain;
  const displayedRecords = domain.displayed_records ? JSON.parse(domain.displayed_records) : [];

  return \`
  <div class="domain-card \${systemClass}" id="domain-card-\${domain.id}">
      <div class="card-col"><strong>我的域名 → 克隆源</strong><span class="domain-cell" title="\${domain.target_domain}" onclick="window.copyToClipboard('\${domain.target_domain}')">\${displayContent}</span><small class="domain-cell" title="\${domain.source_domain}">\${sourceDisplay}</small></div>
      <div class="card-col"><strong>当前解析 <i class="fa-solid fa-arrows-rotate refresh-icon" title="实时查询解析" onclick="window.refreshSingleDomainRecords(\${domain.id})"></i></strong><div id="records-container-\${domain.id}">\${renderLiveRecords(displayedRecords)}</div></div>
      <div class="card-col"><strong>上次同步</strong><div>\${renderStatus(domain)}</div><small>\${formatBeijingTime(domain.last_synced_time)}</small></div>
      <div class="card-actions"><button class="outline" onclick="window.individualSync(\${domain.id})">同步</button><button class="secondary outline" onclick="window.openModal('domainModal', \${domain.id})">编辑</button><button class="contrast outline" onclick="window.deleteDomain(\${domain.id})" \${isSystem ? 'disabled' : ''}>删除</button></div>
  </div>\`;
}
function renderDomainList() { 
  const container = document.getElementById('domain-list-container');
  if (currentDomains.length > 0) {
      container.innerHTML = currentDomains.map(renderDomainCard).join(''); 
  } else {
      container.innerHTML = '<div class="empty-state"><p>暂无域名克隆目标，请点击右上角按钮添加。</p></div>';
  }
}

function renderIpSourceCard(source) {
  const fileUrl = \`\${window.location.origin}/\${source.github_path}\`;
  return \`
  <div class="domain-card" id="ip-source-card-\${source.id}">
      <div class="card-col" style="flex-grow: 2;"><strong>GitHub 文件路径</strong><a href="\${fileUrl}" target="_blank" class="domain-cell" onclick="event.stopPropagation();">\${source.github_path}</a><small class="domain-cell" title="\${source.url}">源: \${source.url}</small></div>
      <div class="card-col"><strong>抓取策略</strong><span>\${source.fetch_strategy || '尚未探测'}</span></div>
      <div class="card-col"><strong>上次同步</strong><small>\${renderStatus(source)} @ \${formatBeijingTime(source.last_synced_time)}</small></div>
      <div class="card-actions"><button class="outline" onclick="window.syncSingleIpSource(\${source.id})">同步</button><button class="secondary outline" onclick="window.openModal('ipSourceModal', \${source.id})">编辑</button><button class="contrast outline" onclick="window.deleteIpSource(\${source.id})">删除</button></div>
  </div>\`;
}
function renderIpSourceList() { 
  const container = document.getElementById('ip-source-list-container');
  if (currentIpSources.length > 0) {
      container.innerHTML = currentIpSources.map(renderIpSourceCard).join('');
  } else {
      container.innerHTML = '<div class="empty-state"><p>暂无IP源，请点击右上角按钮添加。</p></div>';
  }
}

window.copyToClipboard = (text) => { navigator.clipboard.writeText(text).then(() => { showNotification(\`已复制: \${text}\`, 'success', 3000); }, () => { showNotification(\`复制失败，请检查浏览器权限。\`, 'error'); }); };

window.copyCode = (button, contentId) => {
  const content = document.getElementById(contentId).textContent;
  navigator.clipboard.writeText(content).then(() => {
      button.textContent = '已复制!';
      setTimeout(() => { button.textContent = '复制'; }, 2000);
  }, () => { showNotification('复制失败', 'error'); });
};

function updateSingleNodeNameInputs(limit, names = []) {
  const container = document.getElementById('single_node_names_container');
  container.innerHTML = '<label><strong>节点名称 (可选)</strong></label>';
  for (let i = 0; i < limit; i++) {
      const input = document.createElement('input');
      input.type = 'text';
      input.className = 'single-node-name-input';
      input.placeholder = \`节点 \${i + 1} 名称\`;
      input.value = names[i] || '';
      input.style.marginBottom = '0.5rem';
      container.appendChild(input);
  }
}

window.openModal = (modalId, id = null) => {
  const modal = document.getElementById(modalId);
  if (modalId === 'domainModal') {
      const form = document.getElementById('domainForm'); form.reset();
      document.getElementById('modalTitle').textContent = id ? '编辑克隆目标' : '添加新克隆目标';
      document.getElementById('zoneNameSuffix').textContent = zoneName ? '.' + zoneName : '(请先保存设置)';
      
      const singleResolveSwitch = document.getElementById('is_single_resolve');
      const limitContainer = document.getElementById('single_resolve_limit_container');
      const limitInput = document.getElementById('single_resolve_limit');
      const namesContainer = document.getElementById('single_node_names_container');

      function toggleSingleResolveUI() {
          const show = singleResolveSwitch.checked;
          limitContainer.style.display = show ? 'block' : 'none';
          namesContainer.style.display = show ? 'block' : 'none';
          if(show) {
              const domain = id ? currentDomains.find(d => d.id === id) : null;
              const names = domain && domain.single_resolve_node_names ? JSON.parse(domain.single_resolve_node_names) : [];
              updateSingleNodeNameInputs(parseInt(limitInput.value, 10), names);
          }
      }
      
      singleResolveSwitch.onchange = toggleSingleResolveUI;
      limitInput.oninput = () => {
          if (singleResolveSwitch.checked) {
              const currentNames = Array.from(document.querySelectorAll('.single-node-name-input')).map(input => input.value);
              updateSingleNodeNameInputs(parseInt(limitInput.value, 10) || 0, currentNames);
          }
      };

      const domain = id ? currentDomains.find(d => d.id === id) : null;
      const isSystem = domain ? !!domain.is_system : false;
      document.getElementById('source_domain').disabled = isSystem;
      document.getElementById('target_domain_prefix').disabled = isSystem;

      if (domain) {
          document.getElementById('domainId').value = domain.id;
          let prefix = domain.target_domain;
          if (zoneName) {
              const suffix = '.' + zoneName;
              if (domain.target_domain === zoneName) { prefix = '@'; }
              else if (domain.target_domain.endsWith(suffix)) { prefix = domain.target_domain.substring(0, domain.target_domain.length - suffix.length); }
          }
          document.getElementById('target_domain_prefix').value = prefix;
          document.getElementById('source_domain').value = domain.source_domain;
          document.getElementById('is_deep_resolve').checked = !!domain.is_deep_resolve;
          document.getElementById('ttl').value = domain.ttl;
          document.getElementById('notes').value = domain.notes;
          singleResolveSwitch.checked = !!domain.is_single_resolve;
          limitInput.value = domain.single_resolve_limit || 5;
          
          const showSingleUI = !!domain.is_single_resolve;
          limitContainer.style.display = showSingleUI ? 'block' : 'none';
          namesContainer.style.display = showSingleUI ? 'block' : 'none';
          if(showSingleUI) {
              const names = domain.single_resolve_node_names ? JSON.parse(domain.single_resolve_node_names) : [];
              updateSingleNodeNameInputs(domain.single_resolve_limit || 5, names);
          }

      } else { 
          document.getElementById('domainId').value = ''; 
          toggleSingleResolveUI();
      }
  } else if (modalId === 'ipSourceModal') {
      const form = document.getElementById('ipSourceForm'); form.reset();
      successfulProbeStrategy = null;
      document.getElementById('probeProgress').style.display = 'none';
      document.getElementById('probeResult').textContent = '';
      document.getElementById('saveIpSourceBtn').disabled = true;
      document.getElementById('probeBtn').disabled = false;
      document.getElementById('ipSourceModalTitle').textContent = id ? '编辑IP源' : '添加新IP源';
      if (id) {
          const source = currentIpSources.find(s => s.id === id);
          document.getElementById('ipSourceId').value = source.id;
          document.getElementById('ip_source_url').value = source.url;
          document.getElementById('github_path').value = source.github_path;
          document.getElementById('commit_message').value = source.commit_message;
          if (source.fetch_strategy) {
              successfulProbeStrategy = source.fetch_strategy;
              document.getElementById('probeResult').textContent = \`已缓存策略: \${successfulProbeStrategy}\`;
              document.getElementById('saveIpSourceBtn').disabled = false;
          }
      } else {
          document.getElementById('ipSourceId').value = '';
      }
  }
  modal.showModal();
};
window.closeModal = (modalId) => { document.getElementById(modalId).close(); };

async function saveDomain() {
  const id = document.getElementById('domainId').value;
  const single_resolve_node_names = Array.from(document.querySelectorAll('.single-node-name-input')).map(input => input.value);
  const payload = { 
      source_domain: document.getElementById('source_domain').value, 
      target_domain_prefix: document.getElementById('target_domain_prefix').value.trim(), 
      is_deep_resolve: document.getElementById('is_deep_resolve').checked, 
      ttl: parseInt(document.getElementById('ttl').value), 
      notes: document.getElementById('notes').value,
      is_single_resolve: document.getElementById('is_single_resolve').checked,
      single_resolve_limit: parseInt(document.getElementById('single_resolve_limit').value),
      single_resolve_node_names: single_resolve_node_names
  };
  const url = id ? '/api/domains/' + id : '/api/domains'; const method = id ? 'PUT' : 'POST';
  try { const result = await apiFetch(url, { method, body: JSON.stringify(payload) }); showNotification(result.message, 'success'); closeModal('domainModal'); await refreshDomains(); } catch (e) { showNotification(\`保存失败: <code>\${e.message}</code>\`, 'error'); }
}
window.deleteDomain = async (id) => { if (!confirm('确定要删除这个目标吗？此操作不可逆转。')) return; try { const result = await apiFetch('/api/domains/' + id, { method: 'DELETE' }); showNotification(result.message, 'success'); await refreshDomains(); } catch (e) { showNotification(\`错误: <code>\${e.message}</code>\`, 'error'); } }

async function refreshDomains() {
  try {
      currentDomains = await apiFetch('/api/domains');
      renderDomainList();
  } catch (e) {
      showNotification(\`更新列表失败: <code>\${e.message}</code>\`, 'error');
  }
}

window.refreshSingleDomainRecords = async (id) => {
    const container = document.getElementById(\`records-container-\${id}\`);
    if (!container) return;
    container.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> 正在查询...';
    try {
        const records = await apiFetch('/api/domains/' + id + '/resolve', { method: 'POST' });
        container.innerHTML = renderLiveRecords(records);
    } catch(e) {
        container.innerHTML = '<span class="status-failed">实时查询失败</span>';
        showNotification(\`查询失败: \${e.message}\`, 'error');
    }
};

async function saveIpSource() {
  const id = document.getElementById('ipSourceId').value;
  const payload = { url: document.getElementById('ip_source_url').value, github_path: document.getElementById('github_path').value, commit_message: document.getElementById('commit_message').value, fetch_strategy: successfulProbeStrategy };
  const apiUrl = id ? \`/api/ip_sources/\${id}\` : '/api/ip_sources';
  const method = id ? 'PUT' : 'POST';
  try { const result = await apiFetch(apiUrl, { method, body: JSON.stringify(payload) }); showNotification(result.message, 'success'); closeModal('ipSourceModal'); await refreshIpSources(); } catch (e) { showNotification(\`保存失败: <code>\${e.message}</code>\`, 'error'); }
}
  window.deleteIpSource = async (id) => { if (!confirm('确定要删除这个IP源吗？')) return; try { await apiFetch(\`/api/ip_sources/\${id}\`, { method: 'DELETE' }); showNotification('IP源已删除', 'success'); await refreshIpSources(); } catch(e) { showNotification(\`删除失败: code>\${e.message}</code>\`, 'error'); } }
  async function refreshIpSources() { try { currentIpSources = await apiFetch('/api/ip_sources'); renderIpSourceList(); } catch (e) { showNotification(\`更新IP源列表失败: <code>\${e.message}</code>\`, 'error'); } }

async function handleStreamingRequest(url, btn, logOutputElem) {
  const allButtons = document.querySelectorAll('button'); allButtons.forEach(b => b.disabled = true); const originalBtnText = btn ? btn.textContent : ''; if (btn) { btn.innerHTML = \`<i class="fa-solid fa-spinner fa-spin"></i> 同步中\`; btn.setAttribute('aria-busy', 'true'); }
  logOutputElem.style.display = 'block';
  logOutputElem.textContent = '开始同步任务...\\n';
  try { 
      const response = await fetch(url, { method: 'POST' });
      if (!response.ok || !response.body) throw new Error(\`服务器错误: \${response.status}\`);
      const reader = response.body.getReader(); 
      const decoder = new TextDecoder();
      while (true) { 
          const { done, value } = await reader.read(); 
          if (done) break; 
          const chunk = decoder.decode(value, { stream: true }); 
          const lines = chunk.split('\\n\\n').filter(line => line.startsWith('data: ')); 
          for (const line of lines) { logOutputElem.textContent += line.substring(6) + '\\n'; logOutputElem.scrollTop = logOutputElem.scrollHeight; } 
      }
      logOutputElem.textContent += '\\n同步完成，正在更新列表。';
      if(logOutputElem.id === 'logOutput') await refreshDomains();
      if(logOutputElem.id === 'ipLogOutput') await refreshIpSources();
  } catch (e) { 
      logOutputElem.textContent += '\\n发生严重错误：\\n' + e.message; 
      showNotification('同步任务发生错误', 'error');
  } finally { 
      allButtons.forEach(b => { if (!b.closest('dialog')) b.disabled = false; });
      if (btn) { btn.innerHTML = originalBtnText; btn.removeAttribute('aria-busy'); } 
  }
}

window.individualSync = (id) => {
  const btn = document.querySelector(\`#domain-card-\${id} .card-actions button:first-child\`);
  handleStreamingRequest(\`/api/domains/\${id}/sync\`, btn, document.getElementById('logOutput'));
};
window.syncSingleIpSource = (id) => {
  const btn = document.querySelector(\`#ip-source-card-\${id} .card-actions button:first-child\`);
  handleStreamingRequest(\`/api/ip_sources/\${id}/sync\`, btn, document.getElementById('ipLogOutput'));
};

  function setupProxyPageListeners() {
      const page = document.getElementById('page-proxy-settings');
      if (!page) return;
      
      page.querySelectorAll('input, select, textarea').forEach(el => {
          el.addEventListener('input', saveProxySettings);
          el.addEventListener('change', saveProxySettings);
      });

      const enableWsProxy = document.getElementById('enableWsReverseProxy');
      const wsConfig = document.getElementById('wsReverseProxyConfig');
      const uuidSpecificRadio = document.getElementById('wsReverseProxyUseSpecificUuid');
      const uuidSpecificContainer = document.getElementById('wsReverseProxySpecificUuidContainer');

      function toggleWsConfig() {
          wsConfig.style.display = enableWsProxy.checked ? 'block' : 'none';
      }
      function toggleUuidInput() {
          uuidSpecificContainer.style.display = uuidSpecificRadio.checked ? 'block' : 'none';
      }

      enableWsProxy.addEventListener('change', toggleWsConfig);
      page.querySelectorAll('input[name="proxyUuidOption"]').forEach(radio => radio.addEventListener('change', toggleUuidInput));
      
      const subIdCustomRadio = document.getElementById('subUseCustomId');
      const subIdRandomRadio = document.getElementById('subUseRandomId');
      const subIdLengthInput = document.getElementById('subIdLength');
      const subCustomIdInput = document.getElementById('subCustomId');
      const subIdCharsetInput = document.getElementById('subIdCharset');

      function toggleSubIdInputs() {
          const isRandom = subIdRandomRadio.checked;
          subIdLengthInput.disabled = !isRandom;
          subIdCharsetInput.disabled = !isRandom;
          subCustomIdInput.disabled = isRandom;
      }
      page.querySelectorAll('input[name="subIdOption"]').forEach(radio => radio.addEventListener('change', toggleSubIdInputs));
      
      const proxyIpSelector = document.getElementById('proxyIpRegionSelector');
      proxyIpSelector.addEventListener('change', (e) => {
          if (e.target.value) {
              const pathInput = document.getElementById('wsReverseProxyPath');
              pathInput.value = \`/?proxyip=\${e.target.value}\`;
              saveProxySettings();
          }
      });

      const addSubBtn = document.getElementById('add-sub-btn');
      addSubBtn.addEventListener('click', () => {
          const subs = currentSettings.proxySettings.externalSubscriptions || [];
          subs.push({url: '', enabled: true, filters: ''});
          renderExternalSubscriptions(subs);
          saveProxySettings();
      });
      
      const subsContainer = document.getElementById('external-subs-container');
      subsContainer.addEventListener('change', (e) => {
          if(e.target.classList.contains('ext-sub-url') || e.target.classList.contains('ext-sub-enabled') || e.target.classList.contains('ext-sub-filters')) {
              saveProxySettings();
          }
      });
      subsContainer.addEventListener('click', async (e) => {
          if (e.target.classList.contains('delete-sub-btn')) {
              const index = parseInt(e.target.dataset.index, 10);
              currentSettings.proxySettings.externalSubscriptions.splice(index, 1);
              renderExternalSubscriptions(currentSettings.proxySettings.externalSubscriptions);
              saveProxySettings();
          }
          if (e.target.classList.contains('test-sub-btn')) {
              const index = parseInt(e.target.dataset.index, 10);
              const subItemRow = e.target.closest('.sub-item-row');
              const url = subItemRow.querySelector('.ext-sub-url').value;
              const filters = subItemRow.querySelector('.ext-sub-filters').value;
              const resultEl = document.getElementById(\`ext-sub-result-\${index}\`);
              
              if (!url) {
                  resultEl.textContent = 'URL为空';
                  return;
              }

              e.target.disabled = true;
              e.target.innerHTML = '<i class="fa fa-spinner fa-spin"></i>';
              resultEl.textContent = '';

              try {
                  const result = await apiFetch('/api/proxy/test_subscription', {
                      method: 'POST',
                      body: JSON.stringify({ url, filters })
                  });
                  if (result.success) {
                      resultEl.textContent = \`✔ 成功 (\${result.nodeCount}个节点)\`;
                      resultEl.style.color = 'var(--pico-color-green-500)';
                  } else {
                      resultEl.textContent = \`✖ 失败\`;
                      resultEl.style.color = 'var(--pico-color-red-500)';
                      showNotification(result.error, 'error');
                  }
              } catch (err) {
                  resultEl.textContent = \`✖ 失败\`;
                  resultEl.style.color = 'var(--pico-color-red-500)';
                  showNotification(err.message, 'error');
              } finally {
                  e.target.disabled = false;
                  e.target.textContent = '检测';
              }
          }
      });
      
      const filterModeRadios = document.querySelectorAll('input[name="filterMode"]');
      filterModeRadios.forEach(radio => radio.addEventListener('change', () => {
          renderExternalSubscriptions(currentSettings.proxySettings.externalSubscriptions);
      }));

      toggleWsConfig();
      toggleUuidInput();
      toggleSubIdInputs();
      setupSubscriptionButtons();
  }
  
  function renderExternalSubscriptions(subs = []) {
      const container = document.getElementById('external-subs-container');
      const filterMode = document.querySelector('input[name="filterMode"]:checked')?.value || 'none';
      
      document.getElementById('global-filters-container').style.display = filterMode === 'global' ? 'block' : 'none';

      container.innerHTML = '';
      if (subs.length >= 0) {
          subs.forEach((sub, index) => {
              const subEl = document.createElement('div');
              subEl.className = 'sub-item-row';
              subEl.style.marginBottom = '1.5rem';
              subEl.innerHTML = \`
                  <div class="grid">
                      <div style="grid-column: span 12;">
                      <input type="text" class="ext-sub-url" placeholder="订阅地址" value="\${sub.url || ''}">
                      </div>
                  </div>
                  <div class="grid" \${filterMode !== 'individual' ? 'style="display:none;"' : ''}>
                      <div style="grid-column: span 12;">
                          <textarea class="ext-sub-filters" rows="2" placeholder="过滤规则 (每行一条，例如 #M:名称=新名称)">\${sub.filters || ''}</textarea>
                      </div>
                  </div>
                  <div class="grid">
                      <div style="grid-column: span 12; display:flex; align-items:center; justify-content:flex-start; gap: 1rem;">
                          <label style="white-space: nowrap;">
                              <input type="checkbox" class="ext-sub-enabled" \${sub.enabled ? 'checked' : ''}> 启用
                          </label>
                          <small class="ext-sub-result" id="ext-sub-result-\${index}"></small>
                          <button class="secondary outline test-sub-btn" data-index="\${index}" style="margin-left:auto;">检测</button>
                          <button class="secondary outline delete-sub-btn" data-index="\${index}">删除</button>
                      </div>
                  </div>
              \`;
              container.appendChild(subEl);
          });
      }
  }
  
  function populateProxySettingsForm() {
      const ps = currentSettings.proxySettings || {};
      document.getElementById('enableWsReverseProxy').checked = ps.enableWsReverseProxy || false;
      document.getElementById('wsReverseProxyUrl').value = ps.wsReverseProxyUrl || '';
      document.getElementById('wsReverseProxyPath').value = ps.wsReverseProxyPath || '/';
      document.getElementById('wsReverseProxyUseRandomUuid').checked = ps.wsReverseProxyUseRandomUuid === undefined ? true : ps.wsReverseProxyUseRandomUuid;
      document.getElementById('wsReverseProxyUseSpecificUuid').checked = !ps.wsReverseProxyUseRandomUuid;
      document.getElementById('wsReverseProxyUuidValue').value = ps.wsReverseProxySpecificUuid || '';
      document.getElementById('useSelfUrlForSni').checked = ps.useSelfUrlForSni === undefined ? true : ps.useSelfUrlForSni;
      document.getElementById('useProxyUrlForSni').checked = ps.useProxyUrlForSni || false;
      document.getElementById('sublinkWorkerUrl').value = ps.sublinkWorkerUrl || 'https://nnmm.eu.org';
      document.getElementById('publicSubscriptionToggle').checked = ps.publicSubscription || false;
      document.getElementById('subUseRandomId').checked = ps.subUseRandomId === undefined ? true : ps.subUseRandomId;
      document.getElementById('subUseCustomId').checked = ! (ps.subUseRandomId === undefined ? true : ps.subUseRandomId);
      document.getElementById('subIdLength').value = ps.subIdLength || 12;
      document.getElementById('subCustomId').value = ps.subCustomId || '';
      document.getElementById('subIdCharset').value = ps.subIdCharset || 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
      document.getElementById('customNodes').value = ps.customNodes || '';
      document.getElementById(\`filterMode\${ps.filterMode ? ps.filterMode.charAt(0).toUpperCase() + ps.filterMode.slice(1) : 'None'}\`).checked = true;
      document.getElementById('globalFilters').value = ps.globalFilters || '';
      document.getElementById('appThemeSelector').value = ps.APP_THEME || 'default';
      
      renderExternalSubscriptions(ps.externalSubscriptions);
  }

  async function saveProxySettings() {
      clearTimeout(saveProxySettingsTimeout);
      saveProxySettingsTimeout = setTimeout(async () => {
          const externalSubscriptions = [];
          document.querySelectorAll('#external-subs-container .sub-item-row').forEach(el => {
              const url = el.querySelector('.ext-sub-url').value;
              const enabled = el.querySelector('.ext-sub-enabled').checked;
              const filters = el.querySelector('.ext-sub-filters').value;
              externalSubscriptions.push({ url, enabled, filters });
          });

          const proxySettings = {
              enableWsReverseProxy: document.getElementById('enableWsReverseProxy').checked,
              wsReverseProxyUrl: document.getElementById('wsReverseProxyUrl').value,
              wsReverseProxyPath: document.getElementById('wsReverseProxyPath').value,
              wsReverseProxyUseRandomUuid: document.getElementById('wsReverseProxyUseRandomUuid').checked,
              wsReverseProxySpecificUuid: document.getElementById('wsReverseProxyUuidValue').value,
              useSelfUrlForSni: document.getElementById('useSelfUrlForSni').checked,
              useProxyUrlForSni: document.getElementById('useProxyUrlForSni').checked,
              sublinkWorkerUrl: document.getElementById('sublinkWorkerUrl').value,
              publicSubscription: document.getElementById('publicSubscriptionToggle').checked,
              subUseRandomId: document.getElementById('subUseRandomId').checked,
              subIdLength: parseInt(document.getElementById('subIdLength').value, 10),
              subCustomId: document.getElementById('subCustomId').value,
              subIdCharset: document.getElementById('subIdCharset').value,
              externalSubscriptions: externalSubscriptions,
              customNodes: document.getElementById('customNodes').value,
              filterMode: document.querySelector('input[name="filterMode"]:checked').value,
              globalFilters: document.getElementById('globalFilters').value,
              APP_THEME: document.getElementById('appThemeSelector').value
          };
          
          const oldProxySettings = currentSettings.proxySettings;
          currentSettings.proxySettings = proxySettings;

          try {
              const fullSettings = { ...currentSettings, proxySettings };
              await apiFetch('/api/settings', { method: 'POST', body: JSON.stringify(fullSettings) });
              
              showNotification('代理设置已自动保存', 'success', 2000);

              proxySettings.externalSubscriptions.forEach((sub, index) => {
                  const oldSub = oldProxySettings.externalSubscriptions ? oldProxySettings.externalSubscriptions[index] : null;
                  if (oldSub && !oldSub.enabled && sub.enabled && sub.url) {
                      document.querySelector(\`#ext-sub-result-\${index} ~ .test-sub-btn\`).click();
                  }
              });

          } catch(e) {
              showNotification(\`代理设置保存失败: \${e.message}\`, 'error');
          }
      }, 500);
  }
  
  function setupSubscriptionButtons() {
      function generateAndCopySubLink(subType, button) {
          const ps = currentSettings.proxySettings || {};
          let subId;
          if (ps.subUseRandomId) {
              const len = ps.subIdLength || 12;
              const charset = ps.subIdCharset || 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
              if (!len || !charset) { showNotification("请先配置随机ID长度和字符集", "error"); return; }
              subId = Array.from({length: len}, () => charset.charAt(Math.floor(Math.random() * charset.length))).join('');
          } else {
              subId = ps.subCustomId || '';
              if (!subId) { showNotification("请先配置自定义ID", "error"); return; }
          }
          const finalUrl = \`\${window.location.origin}/\${subId}/\${subType}\`;
          
          navigator.clipboard.writeText(finalUrl).then(() => {
              if (button) {
                  const originalText = button.textContent;
                  button.textContent = '复制成功!';
                  setTimeout(() => { button.textContent = originalText; }, 3000);
              } else {
                  showToast(\`\${subType.charAt(0).toUpperCase() + subType.slice(1)} 订阅已复制\`);
              }
          }).catch(err => {
              if (button) {
                  const originalText = button.textContent;
                  button.textContent = '复制失败!';
                  setTimeout(() => { button.textContent = originalText; }, 3000);
              } else {
                  showToast('复制失败!');
              }
          });
      }
      
      document.getElementById('copyXraySub')?.addEventListener('click', (e) => generateAndCopySubLink('xray', e.target));
      document.getElementById('copyClashSub')?.addEventListener('click', (e) => generateAndCopySubLink('clash', e.target));
      document.getElementById('copySingboxSub')?.addEventListener('click', (e) => generateAndCopySubLink('singbox', e.target));
      document.getElementById('copySurgeSub')?.addEventListener('click', (e) => generateAndCopySubLink('surge', e.target));
      
      document.querySelectorAll('#sub-buttons-desktop button, #sub-buttons-mobile button').forEach(btn => {
          btn.addEventListener('click', (e) => {
              generateAndCopySubLink(e.target.dataset.subType);
          });
      });
  }

async function saveSettings(event) {
  event.preventDefault();
  const btn = event.target.querySelector('button');
  btn.disabled = true; btn.setAttribute('aria-busy', 'true');
  
  const oldThreeNetworkSource = currentSettings.THREE_NETWORK_SOURCE;
  const newThreeNetworkSource = document.getElementById('threeNetworkSource').value;

  const settingsToSave = {
      CF_API_TOKEN: document.getElementById('cfToken').value,
      CF_ZONE_ID: document.getElementById('cfZoneId').value,
      THREE_NETWORK_SOURCE: newThreeNetworkSource,
      GITHUB_TOKEN: document.getElementById('githubToken').value,
      GITHUB_OWNER: document.getElementById('githubOwner').value,
      GITHUB_REPO: document.getElementById('githubRepo').value,
      proxySettings: currentSettings.proxySettings 
  };
  try {
      const result = await apiFetch('/api/settings', { method: 'POST', body: JSON.stringify(settingsToSave) });
      showNotification(result.message || '设置已保存！', 'success');
      
      if (oldThreeNetworkSource !== newThreeNetworkSource) {
          showNotification('三网优选源已更改，正在为您同步系统域名...', 'info');
          handleStreamingRequest('/api/domains/sync_system', null, document.getElementById('logOutput'));
      }

      const newSettings = await apiFetch('/api/settings');
      currentSettings = {...currentSettings, ...newSettings };
      zoneName = currentSettings.zoneName || '';
      const githubSettingsComplete = currentSettings.GITHUB_TOKEN && currentSettings.GITHUB_OWNER && currentSettings.GITHUB_REPO;
      document.getElementById('addDomainBtn').disabled = !zoneName;
      document.getElementById('addIpSourceBtn').disabled = !githubSettingsComplete;
      await refreshDomains();
  } catch (e) {
      showNotification(\`保存失败: <br><code>\${e.message}</code>\`, 'error', 10000);
  } finally {
      btn.disabled = false; btn.removeAttribute('aria-busy');
  }
}

function applyTheme(theme) {
      document.documentElement.className = theme || 'default';
}

document.addEventListener('DOMContentLoaded', async () => {
  try {
      currentSettings = await apiFetch('/api/settings');
      zoneName = currentSettings.zoneName || '';
      applyTheme(currentSettings.proxySettings.APP_THEME);
  } catch(e) {
      showNotification('加载设置失败', 'error');
  }

  renderDomainList();
  renderIpSourceList();
  populateProxySettingsForm();
  
  const navLinks = document.querySelectorAll('.nav-link');
  const pages = document.querySelectorAll('.page');
  navLinks.forEach(link => {
      link.addEventListener('click', (e) => {
          e.preventDefault();
          const targetId = link.dataset.target;
          pages.forEach(page => page.classList.remove('active'));
          document.getElementById(targetId).classList.add('active');
          navLinks.forEach(l => l.classList.remove('active'));
          link.classList.add('active');
      });
  });
  
  setupProxyPageListeners();

  document.getElementById('settingsForm').addEventListener('submit', saveSettings);
  document.getElementById('addDomainBtn').addEventListener('click', () => openModal('domainModal'));
  document.getElementById('manualSyncBtn').addEventListener('click', (e) => handleStreamingRequest('/api/sync', e.target, document.getElementById('logOutput')));
  document.getElementById('domainForm').addEventListener('submit', (e) => { e.preventDefault(); saveDomain(); });
  
  const singleResolveSwitch = document.getElementById('is_single_resolve');
  const limitInput = document.getElementById('single_resolve_limit');
  singleResolveSwitch.addEventListener('change', () => {
      const show = singleResolveSwitch.checked;
      document.getElementById('single_resolve_limit_container').style.display = show ? 'block' : 'none';
      document.getElementById('single_node_names_container').style.display = show ? 'block' : 'none';
      if(show) { updateSingleNodeNameInputs(parseInt(limitInput.value, 10)); }
  });
  limitInput.addEventListener('input', () => {
      if(singleResolveSwitch.checked) {
          const currentNames = Array.from(document.querySelectorAll('.single-node-name-input')).map(input => input.value);
          updateSingleNodeNameInputs(parseInt(limitInput.value, 10) || 0, currentNames);
      }
  });

  document.getElementById('appThemeSelector').addEventListener('change', (e) => {
          applyTheme(e.target.value);
          saveProxySettings();
  });

  document.getElementById('addIpSourceBtn').addEventListener('click', () => openModal('ipSourceModal'));
  document.getElementById('manualSyncIpSourcesBtn').addEventListener('click', (e) => handleStreamingRequest('/api/ip_sources/sync_all', e.target, document.getElementById('ipLogOutput')));
  document.getElementById('ipSourceForm').addEventListener('submit', (e) => { e.preventDefault(); saveIpSource(); });

  document.getElementById('probeBtn').addEventListener('click', async (e) => {
      const url = document.getElementById('ip_source_url').value;
      if (!url) { showNotification('请输入IP源地址', 'warning'); return; }
      
      const btn = e.target;
      const progress = document.getElementById('probeProgress');
      const resultElem = document.getElementById('probeResult');
      const saveBtn = document.getElementById('saveIpSourceBtn');

      btn.disabled = true;
      saveBtn.disabled = true;
      progress.style.display = 'block';
      progress.removeAttribute('value');
      resultElem.textContent = '正在探测...';
      successfulProbeStrategy = null;

      try {
          const result = await apiFetch('/api/ip_sources/probe', { method: 'POST', body: JSON.stringify({ url }) });
          progress.setAttribute('value', '100');
          resultElem.textContent = \`探测成功！策略: \${result.strategy} | 发现 \${result.ipCount} 个IP\`;
          successfulProbeStrategy = result.strategy;
          saveBtn.disabled = false;
      } catch (error) {
          progress.style.display = 'none';
          resultElem.textContent = \`探测失败: \${error.message}\`;
          showNotification(\`探测失败: \${error.message}\`, 'error');
      } finally {
          btn.disabled = false;
      }
  });
});
`;}

async function apiGetIpSources(db) {
  const { results } = await db.prepare("SELECT id, url, github_path, commit_message, fetch_strategy, strftime('%Y-%m-%dT%H:%M:%SZ', last_synced_time) as last_synced_time, last_sync_status, last_sync_error, is_enabled FROM ip_sources ORDER BY github_path").all();
  return jsonResponse(results);
}

async function apiAddIpSource(request, db) {
  const { url, github_path, commit_message, fetch_strategy } = await request.json();
  if (!url || !github_path || !commit_message || !fetch_strategy) {
      return jsonResponse({ error: '缺少必填字段或尚未成功探测获取策略。' }, 400);
  }
  try {
      await db.prepare("INSERT INTO ip_sources (url, github_path, commit_message, fetch_strategy) VALUES (?, ?, ?, ?)")
          .bind(url, github_path, commit_message, fetch_strategy).run();
      return jsonResponse({ success: true, message: 'IP源添加成功。' });
  } catch (e) {
      if (e.message.includes('UNIQUE constraint failed')) return jsonResponse({ error: '该URL或GitHub文件路径已存在。' }, 409);
      throw e;
  }
}

async function apiUpdateIpSource(request, db, id) {
  const { url, github_path, commit_message, fetch_strategy } = await request.json();
  if (!url || !github_path || !commit_message || !fetch_strategy) {
      return jsonResponse({ error: '缺少必填字段或尚未成功探测获取策略。' }, 400);
  }
  try {
      await db.prepare("UPDATE ip_sources SET url=?, github_path=?, commit_message=?, fetch_strategy=? WHERE id=?")
          .bind(url, github_path, commit_message, fetch_strategy, id).run();
      return jsonResponse({ success: true, message: 'IP源更新成功。' });
  } catch (e) {
      if (e.message.includes('UNIQUE constraint failed')) return jsonResponse({ error: '该URL或GitHub文件路径已存在。' }, 409);
      throw e;
  }
}

async function apiDeleteIpSource(db, id) {
  await db.prepare('DELETE FROM ip_sources WHERE id = ?').bind(id).run();
  return jsonResponse({ success: true, message: "IP源删除成功。" });
}

const ipv4Regex = /\b((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b/g;
const ipv6Regex = /(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))/gi;

const FETCH_STRATEGIES = {
  direct_regex: async (url) => {
      const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!res.ok) throw new Error(`HTTP error ${res.status}`);
      const text = await res.text();
      
      const lines = text.split('\n');
      const ips = new Set();
      
      const strictIPv4Regex = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
      const strictIPv6Regex = /^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))$/i;

      for (const line of lines) {
          const trimmedLine = line.trim();
          if (strictIPv4Regex.test(trimmedLine) || strictIPv6Regex.test(trimmedLine)) {
              ips.add(trimmedLine);
          }
      }
      
      return Array.from(ips);
  },
  phantomjs_cloud: async (url) => {
      const res = await fetch('https://PhantomJsCloud.com/api/browser/v2/a-demo-key-with-low-quota-per-ip-address/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url, renderType: 'html' })
      });
      if (!res.ok) throw new Error(`PhantomJsCloud API error ${res.status}`);
      const text = await res.text();
      const ipv4s = text.match(ipv4Regex) || [];
      const ipv6s = text.match(ipv6Regex) || [];
      return [...new Set([...ipv4s, ...ipv6s])];
  },
  proxy_codetabs: async (url) => {
      const proxyUrl = 'https://api.codetabs.com/v1/proxy?quest=' + encodeURIComponent(url);
      const res = await fetch(proxyUrl);
      if (!res.ok) throw new Error(`CodeTabs Proxy error ${res.status}`);
      const text = await res.text();
      const ipv4s = text.match(ipv4Regex) || [];
      const ipv6s = text.match(ipv6Regex) || [];
      return [...new Set([...ipv4s, ...ipv6s])];
  },
};
for (let i = 4; i <= 30; i++) {
  FETCH_STRATEGIES[`dummy_strategy_${i}`] = async (url) => { 
      if (url.includes("special-case")) {
          return ["1.2.3." + i]; 
      }
      throw new Error("Dummy strategy failed"); 
  };
}

async function apiProbeIpSource(request) {
  const { url } = await request.json();
  if (!url) return jsonResponse({ error: 'URL is required for probing.' }, 400);

  for (const [strategyName, strategyFn] of Object.entries(FETCH_STRATEGIES)) {
      try {
          const ips = await strategyFn(url);
          if (ips && ips.length > 0) {
              return jsonResponse({
                  success: true,
                  strategy: strategyName,
                  ipCount: ips.length,
                  sampleIps: ips.slice(0, 5)
              });
          }
      } catch (e) {
          console.log(`Strategy '${strategyName}' failed for URL '${url}': ${e.message}`);
      }
  }

  return jsonResponse({ error: '所有探测方案均失败，无法从此URL提取IP。' }, 400);
}

async function fetchIpsFromSource(source) {
  const strategyFn = FETCH_STRATEGIES[source.fetch_strategy];
  if (!strategyFn) {
      throw new Error(`Unknown fetch strategy: ${source.fetch_strategy}`);
  }
  const ips = await strategyFn(source.url);
  if (!ips || ips.length === 0) {
      throw new Error('No IPs found using the cached strategy.');
  }
  return ips.sort();
}

async function githubApiRequest(url, token, options = {}) {
  const headers = {
      'Authorization': `Bearer ${token}`,
      'User-Agent': 'DNS-Clone-Worker',
      'Accept': 'application/vnd.github.v3+json',
      ...options.headers,
  };
  const response = await fetch(url, { ...options, headers });
  if (!response.ok) {
      const errorData = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(`GitHub API error (${response.status}): ${errorData.message}`);
  }
  return response;
}

async function ensureRepoExists(token, owner, repo, log) {
  const repoUrl = `https://api.github.com/repos/${owner}/${repo}`;
  try {
      await githubApiRequest(repoUrl, token);
      log(`仓库 '${owner}/${repo}' 已存在。`);
  } catch (e) {
      if (e.message.includes('404')) {
          log(`仓库 '${owner}/${repo}' 不存在，正在尝试创建...`);
          const createUrl = `https://api.github.com/user/repos`;
          const body = JSON.stringify({
              name: repo,
              private: true,
              description: 'Auto-generated repository for IP source files by DNS Clone Worker.'
          });
          await githubApiRequest(createUrl, token, { method: 'POST', body });
          log(`✔ 成功创建私有仓库 '${owner}/${repo}'。`);
      } else {
          throw e;
      }
  }
}

async function getCurrentGitHubContent({ token, owner, repo, path, log }) {
  const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
  try {
      const response = await githubApiRequest(apiUrl, token, {
          headers: { 'Accept': 'application/vnd.github.v3.raw' }
      });
      return await response.text();
  } catch (e) {
      if (e.message.includes('404')) {
          log(`GitHub文件 '${path}' 不存在，将创建新文件。`);
          return null;
      }
      throw e;
  }
}

async function updateFileOnGitHub({ token, owner, repo, path, content, message, log }) {
  await ensureRepoExists(token, owner, repo, log);
  
  const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
  let sha;
  try {
      const getFileResponse = await githubApiRequest(apiUrl, token);
      const fileData = await getFileResponse.json();
      sha = fileData.sha;
  } catch (e) {
      if (!e.message.includes('404')) throw e;
  }
  
  const body = JSON.stringify({
      message,
      content: btoa(unescape(encodeURIComponent(content))),
      sha
  });

  await githubApiRequest(apiUrl, token, { method: 'PUT', body });
}

function createLogStreamResponse(logFunction) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();
  const log = (message) => {
      const logMsg = beijingTimeLog(message);
      try {
          writer.write(encoder.encode(`data: ${logMsg}\n\n`));
      } catch(e) {
          console.error("Failed to write to stream:", e);
      }
  };

  (async () => {
      try {
          await logFunction(log);
      } catch (e) {
          log(`[FATAL_ERROR] ${e.message}`);
          console.error("Streaming log function error:", e.stack);
      } finally {
          try {
              await writer.close();
          } catch (e) {}
      }
  })();

  return new Response(readable, {
      headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' }
  });
}

function normalizeContent(text) {
    if (typeof text !== 'string') return '';
    return text.split(/\r?\n/).map(line => line.trim()).filter(Boolean).join('\n');
}

async function syncSingleIpSource(id, env, returnLogs) {
  const db = env.WUYA;
  const syncLogic = async (log) => {
      const githubSettings = await getGitHubSettings(db);
      if (!githubSettings.token || !githubSettings.owner || !githubSettings.repo) {
          throw new Error("GitHub API设置不完整。");
      }
      const source = await db.prepare("SELECT * FROM ip_sources WHERE id = ?").bind(id).first();
      if (!source) throw new Error(`未找到ID为 ${id} のIP源。`);
      
      log(`======== 开始同步IP源: ${source.url} ========`);
      
      try {
          const ips = await fetchIpsFromSource(source);
          log(`成功获取 ${ips.length} 个IP。`);
          
          const newContent = ips.join('\n');
          const oldContent = await getCurrentGitHubContent({ ...githubSettings, path: source.github_path, log });

          if (oldContent !== null && normalizeContent(newContent) === normalizeContent(oldContent)) {
              log(`内容无变化，无需更新 GitHub。`);
              await db.prepare("UPDATE ip_sources SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'no_change', last_sync_error = NULL WHERE id = ?").bind(id).run();
              log(`✔ 状态更新为内容一致。`);
              return;
          }
          
          await updateFileOnGitHub({ ...githubSettings, path: source.github_path, content: newContent, message: source.commit_message, log });
          log(`✔ 成功同步到GitHub: ${source.github_path}`);
          
          await db.prepare("UPDATE ip_sources SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'success', last_sync_error = NULL WHERE id = ?").bind(id).run();
      } catch (e) {
          log(`❌ 同步失败: ${e.message}`);
          await db.prepare("UPDATE ip_sources SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'failed', last_sync_error = ? WHERE id = ?").bind(e.message, id).run();
          throw e;
      }
  };

  if (returnLogs) return createLogStreamResponse(syncLogic);
  
  const noOpLog = (msg) => console.log(beijingTimeLog(msg));
  await syncLogic(noOpLog);
}

async function syncAllIpSources(env, returnLogs) {
  const db = env.WUYA;
  const syncLogic = async (log) => {
      log("开始批量同步IP源...");
      const { results: sources } = await db.prepare("SELECT * FROM ip_sources WHERE is_enabled = 1").all();
      if (sources.length === 0) {
          log("没有已启用的IP源需要同步。");
          return;
      }
      for (const source of sources) {
          await syncSingleIpSource(source.id, env, false).catch(e => log(`处理ID ${source.id} 失败: ${e.message}`));
      }
      log("所有IP源同步任务执行完毕。");
  };

  if (returnLogs) return createLogStreamResponse(syncLogic);

  const noOpLog = (msg) => console.log(beijingTimeLog(msg));
  await syncLogic(noOpLog);
}

async function handleGitHubFileProxy(fileName, env, ctx) {
  const db = env.WUYA;
  const source = await db.prepare("SELECT * FROM ip_sources WHERE github_path = ?").bind(fileName).first();

  if (!source) {
      return new Response('File not found or not managed by this service.', { status: 404 });
  }

  const githubSettings = await getGitHubSettings(db);
  if (!githubSettings.token || !githubSettings.owner || !githubSettings.repo) {
      return new Response('GitHub settings are not configured on the server.', { status: 500 });
  }
  
  const cache = caches.default;
  const cacheKey = new Request(new URL(fileName, "https://github-proxy.cache").toString());
  let response = await cache.match(cacheKey);

  if (!response) {
      const apiUrl = `https://api.github.com/repos/${githubSettings.owner}/${githubSettings.repo}/contents/${fileName}`;
      const headers = {
          'Authorization': `Bearer ${token}`,
          'User-Agent': 'DNS-Clone-Worker-Proxy',
          'Accept': 'application/vnd.github.v3.raw' 
      };
      
      const githubResponse = await fetch(apiUrl, { headers });

      if (!githubResponse.ok) {
          return new Response('Failed to fetch file from GitHub.', { status: githubResponse.status });
      }
      
      response = new Response(githubResponse.body, {
          headers: {
              'Content-Type': 'text/plain; charset=utf-8',
              'Cache-Control': 's-maxage=300'
          }
      });
      
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
  }

  return response;
}
async function syncDomainLogic(domain, token, zoneId, db, log, syncContext) {
  log(`======== 开始同步: ${domain.target_domain} ========`);
  try {
      let recordsToUpdate;
      if (domain.source_domain.startsWith('internal:hostmonit:')) {
          const type = domain.source_domain.split(':')[2];
          const sourceName = await getSetting(db, 'THREE_NETWORK_SOURCE') || 'CloudFlareYes';
          log(`模式: 系统内置源 (三网优选IP - ${type}, 来源: ${sourceName})`);
          if (!syncContext.threeNetworkIps || syncContext.threeNetworkIps.source !== sourceName) {
              log(`正在从 ${sourceName} 获取三网优选IP...`);
              syncContext.threeNetworkIps = await fetchThreeNetworkIps(sourceName, log);
              syncContext.threeNetworkIps.source = sourceName;
              const ips = syncContext.threeNetworkIps;
              if (ips && (ips.yd.length > 0 || ips.dx.length > 0 || ips.lt.length > 0)) {
                  log(`获取成功: 移动(${ips.yd.length}) 电信(${ips.dx.length}) 联通(${ips.lt.length})`);
              } else {
                  log(`未获取到任何三网IP。`);
              }
          }
          const ips = syncContext.threeNetworkIps[type] || [];
          recordsToUpdate = ips.map(ip => ({ type: 'A', content: ip }));
      } else if (domain.is_deep_resolve) {
          log(`模式: 深度解析 (追踪CNAME)`);
          recordsToUpdate = await resolveRecursively(domain.source_domain, log);
      } else {
          log(`模式: 浅层克隆 (直接克隆CNAME)`);
          const cnames = await getDnsFromDoh(domain.source_domain, 'CNAME');
          if (cnames.length > 0) {
              recordsToUpdate = [{ type: 'CNAME', content: cnames[0].replace(/\.$/, "") }];
          } else {
              throw new Error(`在浅层克隆模式下，源域名 ${domain.source_domain} 必须是一个CNAME记录。`);
          }
      }

      if (!recordsToUpdate || recordsToUpdate.length === 0) {
          const lastRecords = JSON.parse(domain.last_synced_records || '[]');
          if (lastRecords.length === 0) {
              log(`源域名 ${domain.source_domain} 未找到任何记录，与上次同步结果一致，无需操作。`);
              await db.prepare("UPDATE domains SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'no_change', last_sync_error = NULL WHERE id = ?").bind(domain.id).run();
              log(`✔ 同步完成 ${domain.target_domain} (内容一致)。`);
              return;
          } else {
              throw new Error(`源域名 ${domain.source_domain} 未找到任何可解析的记录（上次曾有记录）。`);
          }
      }

      const { allZoneRecords } = syncContext;
      const zoneName = await getZoneName(token, zoneId);

      let operations = [];

      const existingMainRecords = allZoneRecords.filter(r => r.name === domain.target_domain);
      const mainChanges = calculateDnsChanges(existingMainRecords, recordsToUpdate, domain);
      operations.push(...mainChanges.toDelete.map(rec => ({ action: 'delete', record: rec })));
      operations.push(...mainChanges.toAdd.map(rec => ({ action: 'add', record: rec, domain: domain })));

      const targetPrefix = domain.target_domain.replace(`.${zoneName}`, '');
      if (targetPrefix !== domain.target_domain) {
          const singleResolveRegex = new RegExp(`^${targetPrefix.replace(/\./g, '\\.')}\\.\\d+\\.${zoneName.replace(/\./g, '\\.')}$`);
          const existingSingleRecords = allZoneRecords.filter(r => singleResolveRegex.test(r.name));

          const recordsForSingleResolve = domain.is_single_resolve ? recordsToUpdate.slice(0, domain.single_resolve_limit) : [];
          
          for (let i = 0; i < recordsForSingleResolve.length; i++) {
              const record = recordsForSingleResolve[i];
              const singleDomainName = `${targetPrefix}.${i + 1}.${zoneName}`;
              const existing = existingSingleRecords.filter(r => r.name === singleDomainName);
              const singleDomain = { ...domain, target_domain: singleDomainName };
              const changes = calculateDnsChanges(existing, [record], singleDomain);
              operations.push(...changes.toDelete.map(rec => ({ action: 'delete', record: rec })));
              operations.push(...changes.toAdd.map(rec => ({ action: 'add', record: rec, domain: singleDomain })));
          }
          
          const recordsToClean = existingSingleRecords.filter(r => {
              const match = r.name.match(`^${targetPrefix.replace(/\./g, '\\.')}\\.(\\d+)\\.${zoneName.replace(/\./g, '\\.')}$`);
              const num = match ? parseInt(match[1], 10) : 0;
              return num > recordsForSingleResolve.length;
          });

          operations.push(...recordsToClean.map(rec => ({ action: 'delete', record: rec })));
      }

      if (operations.length === 0) {
          log(`所有记录无变化，无需操作。`);
          await db.prepare("UPDATE domains SET last_synced_records = ?, displayed_records = ?, last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'no_change', last_sync_error = NULL WHERE id = ?")
            .bind(JSON.stringify(recordsToUpdate), JSON.stringify(recordsToUpdate), domain.id).run();
          log(`✔ 同步完成 ${domain.target_domain} (内容一致)。`);
      } else {
          log(`共计 ${operations.length} 个操作待执行。`);
          await executeDnsOperations(token, zoneId, operations, log);
          await db.prepare("UPDATE domains SET last_synced_records = ?, displayed_records = ?, last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'success', last_sync_error = NULL WHERE id = ?")
            .bind(JSON.stringify(recordsToUpdate), JSON.stringify(recordsToUpdate), domain.id).run();
          log(`✔ 同步完成 ${domain.target_domain} (内容已更新)。`);
      }

  } catch (e) {
      log(`❌ 同步 ${domain.target_domain} 失败: ${e.message}`);
      await db.prepare("UPDATE domains SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'failed', last_sync_error = ? WHERE id = ?").bind(e.message, domain.id).run();
      throw e;
  }
}


async function syncSingleDomain(id, env, returnLogs) {
  const db = env.WUYA;
  const syncLogic = async (log) => {
      const { token, zoneId } = await getCfApiSettings(db);
      if (!token || !zoneId) throw new Error("尚未配置 Cloudflare API 令牌或区域 ID。");
      const domain = await db.prepare("SELECT * FROM domains WHERE id = ?").bind(id).first();
      if (!domain) throw new Error(`未找到 ID 为 ${id} 的目标。`);
      if (!domain.is_enabled) {
          log(`域名 ${domain.target_domain} 已被禁用，跳过同步。`);
          return;
      }
      
      const allZoneRecords = await listAllDnsRecords(token, zoneId);
      const syncContext = { allZoneRecords };
      await syncDomainLogic(domain, token, zoneId, db, log, syncContext);
  };

  if (returnLogs) return createLogStreamResponse(syncLogic);
  
  const noOpLog = (msg) => console.log(beijingTimeLog(msg));
  await syncLogic(noOpLog);
}

async function syncAllDomains(env, returnLogs) {
  const db = env.WUYA;
  const syncLogic = async (log) => {
      log("开始批量同步任务...");
      const { token, zoneId } = await getCfApiSettings(db);
      if (!token || !zoneId) throw new Error("尚未配置 Cloudflare API 令牌或区域 ID。");
      const { results: domains } = await db.prepare("SELECT * FROM domains WHERE is_enabled = 1").all();
      if (domains.length === 0) {
          log("没有需要同步的已启用目标。");
          return;
      }
      log(`发现 ${domains.length} 个已启用的目标需要同步。`);

      const allZoneRecords = await listAllDnsRecords(token, zoneId);
      const syncContext = { allZoneRecords };

      for (const domain of domains) {
          try {
              await syncDomainLogic(domain, token, zoneId, db, log, syncContext);
          } catch(e) {
              log(`处理域名 ${domain.target_domain} 失败: ${e.message}`);
          }
      }
      log("所有目标同步任务执行完毕。");
  };

  if (returnLogs) return createLogStreamResponse(syncLogic);

  const noOpLog = (msg) => console.log(beijingTimeLog(msg));
  await syncLogic(noOpLog);
}

async function syncSystemDomains(env, returnLogs) {
  const db = env.WUYA;
  const syncLogic = async (log) => {
      log("开始同步系统预设域名...");
      const { token, zoneId } = await getCfApiSettings(db);
      if (!token || !zoneId) throw new Error("尚未配置 Cloudflare API 令牌或区域 ID。");
      const { results: domains } = await db.prepare("SELECT * FROM domains WHERE is_enabled = 1 AND is_system = 1").all();
      if (domains.length === 0) {
          log("没有需要同步的已启用系统域名。");
          return;
      }
      log(`发现 ${domains.length} 个已启用的系统域名需要同步。`);
      
      const allZoneRecords = await listAllDnsRecords(token, zoneId);
      const syncContext = { allZoneRecords };

      for (const domain of domains) {
          await syncDomainLogic(domain, token, zoneId, db, log, syncContext).catch(e => {});
      }
      log("系统域名同步任务执行完毕。");
  };

  if (returnLogs) return createLogStreamResponse(syncLogic);
  
  const noOpLog = (msg) => console.log(beijingTimeLog(msg));
  await syncLogic(noOpLog);
}

async function resolveRecursively(domain, log, depth = 0) {
const MAX_DEPTH = 10;
if (depth > MAX_DEPTH) {
  log(`错误：解析深度超过 ${MAX_DEPTH} 层，可能存在CNAME循环。中止解析 ${domain}。`);
  return [];
}
log(`(深度 ${depth}) 正在解析: ${domain}`);
const cnames = await getDnsFromDoh(domain, 'CNAME');
if (cnames.length > 0) {
  const cnameTarget = cnames[0].replace(/\.$/, "");
  log(`(深度 ${depth}) 发现CNAME -> ${cnameTarget}`);
  const nextRecords = await resolveRecursively(cnameTarget, log, depth + 1);
  if (nextRecords.length > 0) {
      return nextRecords;
  }
  log(`(深度 ${depth + 1}) CNAME ${cnameTarget} 未解析到最终IP，将直接克隆此CNAME记录。`);
  return [{ type: 'CNAME', content: cnameTarget }];
}
log(`(深度 ${depth}) 未发现CNAME，查询最终IP for ${domain}`);
const ipv4s = await getDnsFromDoh(domain, 'A');
const ipv6s = await getDnsFromDoh(domain, 'AAAA');
const validIPv4s = ipv4s.filter(ip => /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/.test(ip));
const validIPv6s = ipv6s.filter(ip => ipv6Regex.test(ip));
return [...validIPv4s.map(ip => ({ type: 'A', content: ip })), ...validIPv6s.map(ip => ({ type: 'AAAA', content: ip }))];
}

function calculateDnsChanges(existingRecords, newRecords, domain) {
  const { ttl } = domain;
  const toDelete = [];
  const toAdd = newRecords.map(r => ({ ...r, content: r.content.replace(/\.$/, "") }));
  const newRecordIsCname = toAdd.some(r => r.type === 'CNAME');

  for (const existing of existingRecords) {
      const normalizedExistingContent = existing.content.replace(/\.$/, "");
      if ((newRecordIsCname && ['A', 'AAAA', 'CNAME'].includes(existing.type)) || (!newRecordIsCname && existing.type === 'CNAME')) {
          toDelete.push(existing);
          continue;
      }
      let foundMatch = false;
      for (let i = toAdd.length - 1; i >= 0; i--) {
          if (toAdd[i].type === existing.type && toAdd[i].content === normalizedExistingContent) {
              if(existing.proxied === false && existing.ttl === ttl) {
                  toAdd.splice(i, 1);
                  foundMatch = true;
                  break;
              }
          }
      }
      if (!foundMatch && ['A', 'AAAA', 'CNAME'].includes(existing.type)) {
          toDelete.push(existing);
      }
  }
  return { toDelete, toAdd };
}

async function executeDnsOperations(token, zoneId, operations, log) {
  const API_CHUNK_SIZE = 10;
  const API_ENDPOINT = `https://api.cloudflare.com/client/v4/zones/${zoneId}/dns_records`;
  const headers = { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' };

  const promises = operations.map(op => {
      if (op.action === 'delete') {
          log(`- 准备删除旧记录: [${op.record.type}] ${op.record.content} for ${op.record.name}`);
          return () => fetch(`${API_ENDPOINT}/${op.record.id}`, { method: 'DELETE', headers });
      }
      if (op.action === 'add') {
          const { record, domain } = op;
          log(`+ 准备添加新记录: [${record.type}] ${record.content} for ${domain.target_domain}`);
          return () => fetch(API_ENDPOINT, { method: 'POST', headers, body: JSON.stringify({ type: record.type, name: domain.target_domain, content: record.content, ttl: domain.ttl, proxied: false }) });
      }
      return null;
  }).filter(Boolean);

  for (let i = 0; i < promises.length; i += API_CHUNK_SIZE) {
      const chunk = promises.slice(i, i + API_CHUNK_SIZE);
      const responses = await Promise.all(chunk.map(p => p()));
      for (const res of responses) {
          if (!res.ok) {
              const errorBody = await res.json().catch(() => ({ errors: [{ code: 9999, message: 'Unknown error' }] }));
              const errorMessage = (errorBody.errors || []).map(e => `(Code ${e.code}: ${e.message})`).join(', ');
              log(`一个API调用失败: ${res.status} - ${errorMessage}`);
              if (errorMessage) throw new Error(`Cloudflare API操作失败: ${errorMessage}`);
          }
      }
  }
}

async function listAllDnsRecords(token, zoneId) {
  const API_ENDPOINT = `https://api.cloudflare.com/client/v4/zones/${zoneId}/dns_records?per_page=500`;
  const headers = { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' };
  const response = await fetch(API_ENDPOINT, { headers });
  if (!response.ok) throw new Error(`获取DNS记录列表失败: ${await response.text()}`);
  const data = await response.json();
  if (!data.success) throw new Error(`获取DNS记录列表API错误: ${JSON.stringify(data.errors)}`);
  return data.result;
}

async function getZoneName(token, zoneId) {
if (!token || !zoneId) throw new Error("API 令牌和区域 ID 不能为空。");
const response = await fetch(`https://api.cloudflare.com/client/v4/zones/${zoneId}`, { headers: { 'Authorization': `Bearer ${token}` } });
if (!response.ok) { const errText = await response.text(); throw new Error(`无法从 Cloudflare 获取区域信息: ${errText}`); }
const data = await response.json();
if (!data.success) throw new Error(`Cloudflare API 返回错误: ${JSON.stringify(data.errors)}`);
return data.result.name;
}

async function hashPassword(password, salt) { const data = new TextEncoder().encode(password + salt); const hashBuffer = await crypto.subtle.digest('SHA-256', data); return Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join(''); }
async function isAuthenticated(request, db) { const token = getCookie(request, 'session'); if (!token) return false; const session = await db.prepare("SELECT expires_at FROM sessions WHERE token = ?").bind(token).first(); if (!session || new Date(session.expires_at) < new Date()) { if (session) await db.prepare("DELETE FROM sessions WHERE token = ?").bind(token).run(); return false; } return true; }
function getCookie(request, name) { const cookieHeader = request.headers.get('Cookie'); if (cookieHeader) for (let cookie of cookieHeader.split(';')) { const [key, value] = cookie.trim().split('='); if (key === name) return value; } return null; }
function jsonResponse(data, status = 200, headers = {}) { return new Response(JSON.stringify(data, null, 2), { status, headers: { 'Content-Type': 'application/json;charset=UTF-8', ...headers } }); }
const beijingTimeLog = (message) => `[${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false })}] ${message}`;
async function getDnsFromDoh(domain, type) { try { const url = `https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(domain)}&type=${type}`; const response = await fetch(url, { headers: { 'accept': 'application/dns-json' } }); if (!response.ok) { console.warn(`DoH query failed for ${domain} (${type}): ${response.statusText}`); return []; } const data = await response.json(); return data.Answer ? data.Answer.map(ans => ans.data).filter(Boolean) : []; } catch (e) { console.error(`DoH query error for ${domain} (${type}): ${e.message}`); return []; } }

async function fetchThreeNetworkIps(source, log) {
  log(`正在从源 [${source}] 获取IP...`);

  async function parseHtmlTableWithOperator(htmlContent) {
      const ips = { yd: new Set(), dx: new Set(), lt: new Set() };
      const rowRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
      const cellRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
      const ipRegex = /\b(?:\d{1,3}\.){3}\d{1,3}\b/;

      let rowMatch;
      while ((rowMatch = rowRegex.exec(htmlContent)) !== null) {
          const cells = Array.from(rowMatch[1].matchAll(cellRegex), m => m[1].replace(/<[^>]+>/g, '').trim());
          if (cells.length >= 2) {
              const lineCell = cells.find(c => c.includes('电信') || c.includes('联通') || c.includes('移动'));
              const ipCell = cells.find(c => ipRegex.test(c));

              if (lineCell && ipCell) {
                  const ip = ipCell.match(ipRegex)[0];
                  if (lineCell.includes('移动')) ips.yd.add(ip);
                  else if (lineCell.includes('电信')) ips.dx.add(ip);
                  else if (lineCell.includes('联通')) ips.lt.add(ip);
              }
          }
      }
      
      const allIpsArray = [...ips.yd, ...ips.dx, ...ips.lt];
      if (allIpsArray.length > 0) {
          const allIps = new Set(allIpsArray);
          if (ips.yd.size === 0) ips.yd = allIps;
          if (ips.dx.size === 0) ips.dx = allIps;
          if (ips.lt.size === 0) ips.lt = allIps;
      }
      
      return { yd: Array.from(ips.yd), dx: Array.from(ips.dx), lt: Array.from(ips.lt) };
  }

  try {
      let url;
      let usePhantom = false;
      switch (source) {
          case 'api.uouin.com':
              url = 'https://api.uouin.com/cloudflare.html';
              break;
          case 'wetest.vip':
              url = 'https://www.wetest.vip/page/cloudflare/address_v4.html';
              break;
          case 'CloudFlareYes':
          default:
              url = 'https://stock.hostmonit.com/CloudFlareYes';
              usePhantom = true;
              break;
      }

      let htmlContent;
      if (usePhantom) {
          const fetchUrl = 'https://PhantomJsCloud.com/api/browser/v2/a-demo-key-with-low-quota-per-ip-address/';
          const response = await fetch(fetchUrl, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ url: url, renderType: 'html' })
          });
          if (!response.ok) throw new Error(`获取 ${url} 失败: ${response.statusText}`);
          htmlContent = await response.text();
      } else {
          const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }});
          if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
          htmlContent = await response.text();
      }
      
      return await parseHtmlTableWithOperator(htmlContent);

  } catch (e) {
      log(`从源 [${source}] 获取IP失败: ${e.message}`);
      return { yd: [], dx: [], lt: [] };
  }
}

async function syncExternalSubscription(sub, db, log) {
    const { url } = sub;
    try {
        log(`Fetching external sub: ${url}`);
        const rawContent = await fetchAndParseExternalSubscription(url);

        if (rawContent !== null) {
            const nodeCount = rawContent.trim().split('\n').filter(Boolean).length;
            log(`Success: Found ${nodeCount} valid nodes.`);
            await db.prepare("INSERT INTO external_nodes (url, content, status, last_updated, error) VALUES (?, ?, 'success', CURRENT_TIMESTAMP, NULL) ON CONFLICT(url) DO UPDATE SET content=excluded.content, status='success', last_updated=CURRENT_TIMESTAMP, error=NULL")
                .bind(url, rawContent).run();
        } else {
            throw new Error("No valid content returned from parser.");
        }
    } catch (e) {
        log(`Failed to fetch/parse sub ${url}: ${e.message}`);
        await db.prepare("INSERT INTO external_nodes (url, status, last_updated, error) VALUES (?, 'failed', CURRENT_TIMESTAMP, ?) ON CONFLICT(url) DO UPDATE SET status='failed', last_updated=CURRENT_TIMESTAMP, error=excluded.error")
            .bind(url, e.message).run();
    }
}

function parseFilterRules(filters) {
  if (!filters) return [];
  return filters.split('\n').filter(Boolean).map(line => {
      line = line.trim();
      if (line.startsWith('#M:') || line.startsWith('#H:')) {
          const type = line.substring(0, 3);
          const ruleContent = line.substring(3);
          const parts = ruleContent.split('=');
          if (parts.length === 2) {
              const [match, replacement] = parts.map(s => s.trim());
              if (match) {
                  return { type, match, replacement };
              }
          }
      } else if (line.startsWith('#T:') || line.startsWith('#W:')) {
          const type = line.substring(0, 3);
          const replacement = line.substring(3).trim();
          return { type, replacement };
      }
      return null;
  }).filter(Boolean);
}

function applyContentFilters(content, filters) {
  if (!filters) return content;

  const rules = parseFilterRules(filters);
  if (rules.length === 0) return content;

  const lines = content.split('\n');
  const processedLines = lines.map(line => {
      const parts = line.split('#');
      if (parts.length < 2) return line;

      let addressPort = parts[0];
      let remarks = parts.slice(1).join('#');
      const [address, port] = addressPort.split(':');
      let newAddress = address;

      for (const rule of rules) {
          switch(rule.type) {
              case '#H:':
                  if (newAddress.includes(rule.match)) {
                      newAddress = newAddress.replace(new RegExp(rule.match, 'g'), rule.replacement);
                  }
                  break;
              case '#M:':
                  if (remarks.includes(rule.match)) {
                      remarks = remarks.replace(new RegExp(rule.match, 'g'), rule.replacement);
                  }
                  break;
              case '#T:':
                  remarks = rule.replacement + remarks;
                  break;
              case '#W:':
                  remarks = remarks + rule.replacement;
                  break;
          }
      }
      return `${newAddress}:${port}#${remarks}`;
  });

  return processedLines.join('\n');
}

async function fetchAndParseExternalSubscription(url) {
  if(!url) return null;
  try {
      const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache' } });
      if (!response.ok) return null;
      
      let content = await response.text();

      if (content.trim().startsWith('<')) {
          const jsRegex = /copyURL\(\)\{navigator\.clipboard\.writeText\('([^']+)'\)/;
          const jsMatch = content.match(jsRegex);
          const subUrl = jsMatch ? jsMatch[1] : (content.match(/https:\/\/baidu\.sosorg\.nyc\.mn\/sub\?[^"'\s<]+/) || [])[0];

          if (subUrl) {
              const subResponse = await fetch(subUrl);
              if (!subResponse.ok) return null;
              content = await subResponse.text();
          } else {
              return null;
          }
      }

      try {
          const decoded = atob(content);
          if (decoded) content = decoded;
      } catch (e) { }
      
      return processSubscriptionContent(content.split('\n'));
  } catch (error) {
      console.error(`处理外部订阅 ${url} 失败: ${error.message}`);
      return null;
  }
}

function processSubscriptionContent(lines) {
  const output = [];
  const domainRegex = /^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,63}$/;
  const ipv4Regex = /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
  const ipv6Regex = /(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))/i;

  lines.forEach(line => {
      line = line.trim();
      if (!line) return;

      let nodeInfo = null;

      if (line.startsWith('vless://') || line.startsWith('vmess://')) {
          nodeInfo = parseShareLink(line);
      } else if (line.includes(':') && line.includes('#')) {
          const parts = line.split('#');
          if (parts.length >= 2) {
              const addressPort = parts[0];
              const originalRemarks = parts.slice(1).join('#');
              const [address, portStr] = addressPort.split(':');
              const port = parseInt(portStr, 10);
              nodeInfo = { address, port, remarks: originalRemarks };
          }
      }

      if (nodeInfo) {
          const { address, port, remarks } = nodeInfo;
          const isValidAddress = domainRegex.test(address) || ipv4Regex.test(address) || ipv6Regex.test(address);
          const isValidPort = !isNaN(port) && port > 0 && port < 65536;

          if (isValidAddress && isValidPort) {
              const newRemarks = formatRemarks(remarks, address);
              output.push(`${address}:${port}#${newRemarks}`);
          }
      }
  });
  return output.join('\n');
}

function formatRemarks(originalRemarks, address) {
const chineseRegex = /^[\u4e00-\u9fa5]/;
if (chineseRegex.test(originalRemarks.trim())) {
  return address;
}
const letterRegex = /^[A-Za-z]{2,3}/;
const letterMatch = originalRemarks.trim().match(letterRegex);
if (letterMatch) {
  return letterMatch[0];
}
return originalRemarks;
}

function parseShareLink(link) {
  try {
      if (link.startsWith('vmess://')) {
          const decoded = JSON.parse(atob(link.substring(8)));
          const address = decoded.add;
          const port = parseInt(decoded.port, 10);
          const remarks = decoded.ps || decoded.add;
          return { address, port, remarks };
      } else {
          const url = new URL(link);
          const address = url.hostname;
          const port = parseInt(url.port, 10) || 443;
          const remarks = decodeURIComponent(url.hash.substring(1));
          return { address, port, remarks };
      }
  } catch (e) {
      return null;
  }
}

function parseExternalNodes(content, proxySettings, request) {
  const lines = content.split('\n').filter(Boolean);
  const requestHostname = new URL(request.url).hostname;
  let sniHost = requestHostname;
  if (proxySettings.useProxyUrlForSni) {
      try {
          sniHost = new URL(proxySettings.wsReverseProxyUrl).hostname;
      } catch (e) { }
  }
  const path = proxySettings.wsReverseProxyPath || '/';
  const useRandomUuid = proxySettings.wsReverseProxyUseRandomUuid;
  const specificUuid = proxySettings.wsReverseProxySpecificUuid;

  return lines.map(line => {
      const parts = line.split('#');
      const addressPort = parts[0];
      const remarks = parts.slice(1).join('#');
      const [address, port] = addressPort.split(':');
      
      const uuid = useRandomUuid ? crypto.randomUUID() : specificUuid;
      const encodedPath = encodeURIComponent(encodeURIComponent(path));
      
      return `vless://${uuid}@${address}:${port}?encryption=none&security=tls&sni=${sniHost}&fp=random&type=ws&host=${sniHost}&path=${encodedPath}#${encodeURIComponent(remarks)}`;
  });
}

function parseCustomNodes(content, proxySettings, request) {
  if (!content) return [];
  const lines = content.split('\n').filter(Boolean);
  const requestHostname = new URL(request.url).hostname;
  let sniHost = requestHostname;
  if (proxySettings.useProxyUrlForSni) {
      try {
          sniHost = new URL(proxySettings.wsReverseProxyUrl).hostname;
      } catch (e) { }
  }
  const globalPath = proxySettings.wsReverseProxyPath || '/';
  const useRandomUuid = proxySettings.wsReverseProxyUseRandomUuid;
  const specificUuid = proxySettings.wsReverseProxySpecificUuid;

  return lines.map(line => {
      const pathParts = line.split('@');
      const mainPart = pathParts[0];
      const customPath = pathParts.length > 1 ? pathParts[1] : null;

      const remarkParts = mainPart.split('#');
      const addressPort = remarkParts[0];
      const remarks = remarkParts.slice(1).join('#');
      const [address, port] = addressPort.split(':');
      
      const uuid = useRandomUuid ? crypto.randomUUID() : specificUuid;
      const finalPath = customPath ? `/${customPath.replace(/^\//, '')}` : globalPath;
      const encodedPath = encodeURIComponent(encodeURIComponent(finalPath));
      
      return `vless://${uuid}@${address}:${port}?encryption=none&security=tls&sni=${sniHost}&fp=random&type=ws&host=${sniHost}&path=${encodedPath}#${encodeURIComponent(remarks)}`;
  });
}
