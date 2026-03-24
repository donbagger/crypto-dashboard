const express = require("express");
const https = require("https");
const http = require("http");
const path = require("path");
const { URL } = require("url");
const HttpsProxyAgent = require("https-proxy-agent");

const app = express();
const PORT = process.env.PORT || 3000;

// Configure proxy agent if proxy env is set
const proxyUrl = process.env.https_proxy || process.env.HTTPS_PROXY;
const proxyAgent = proxyUrl ? new HttpsProxyAgent(proxyUrl) : undefined;

// Top 10 Solana tokens with known addresses
const SOLANA_TOKENS = [
  { address: "So11111111111111111111111111111111111111112", symbol: "SOL", name: "Wrapped SOL" },
  { address: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", symbol: "USDC", name: "USD Coin" },
  { address: "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", symbol: "USDT", name: "Tether USD" },
  { address: "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN", symbol: "JUP", name: "Jupiter" },
  { address: "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", symbol: "RAY", name: "Raydium" },
  { address: "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", symbol: "Bonk", name: "Bonk" },
  { address: "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", symbol: "WIF", name: "dogwifhat" },
  { address: "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof", symbol: "RENDER", name: "Render Token" },
  { address: "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE", symbol: "ORCA", name: "Orca" },
  { address: "hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux", symbol: "HNT", name: "Helium Network Token" },
];

// In-memory price store
const prices = {};
SOLANA_TOKENS.forEach((t) => {
  prices[t.address] = {
    symbol: t.symbol,
    name: t.name,
    address: t.address,
    price_usd: null,
    change_24h: null,
    updated_at: null,
  };
});

// SSE clients connected to our server
const sseClients = new Set();

// Helper to make HTTPS GET request (with proxy support)
function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const options = { agent: proxyAgent };
    https
      .get(url, options, (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(new Error(`JSON parse error for ${url}: ${e.message}`));
          }
        });
      })
      .on("error", reject);
  });
}

// Fetch initial prices from DexPaprika (individual token endpoints)
async function fetchInitialPrices() {
  const results = await Promise.allSettled(
    SOLANA_TOKENS.map(async (token) => {
      const url = `https://api.dexpaprika.com/networks/solana/tokens/${token.address}`;
      const data = await httpsGet(url);
      if (data.summary && data.summary.price_usd) {
        prices[token.address].price_usd = data.summary.price_usd;
        prices[token.address].change_24h = data.summary["24h"]?.last_price_usd_change || 0;
        prices[token.address].updated_at = Date.now();
      }
    })
  );

  const ok = results.filter((r) => r.status === "fulfilled").length;
  const failed = results.filter((r) => r.status === "rejected");
  console.log(`Fetched initial prices: ${ok}/${SOLANA_TOKENS.length} succeeded`);
  failed.forEach((r) => console.error("  Fetch error:", r.reason?.message));
}

// Connect to DexPaprika SSE streaming for real-time updates
function startStreaming() {
  const payload = SOLANA_TOKENS.map((t) => ({
    chain: "solana",
    address: t.address,
    method: "t_p",
  }));

  const postData = JSON.stringify(payload);

  console.log("Connecting to DexPaprika SSE stream...");

  function connect() {
    const streamUrl = new URL("https://streaming.dexpaprika.com/stream");
    const options = {
      hostname: streamUrl.hostname,
      port: 443,
      path: streamUrl.pathname,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "text/event-stream",
        "Content-Length": Buffer.byteLength(postData),
      },
      agent: proxyAgent,
    };

    const req = https.request(options, (res) => {
      console.log(`SSE stream connected (status: ${res.statusCode})`);

      let buffer = "";

      res.on("data", (chunk) => {
        buffer += chunk.toString();

        // Parse SSE events from buffer
        const lines = buffer.split("\n");
        buffer = lines.pop(); // Keep incomplete line in buffer

        for (const line of lines) {
          if (line.startsWith("data:")) {
            const jsonStr = line.slice(5).trim();
            if (!jsonStr) continue;
            try {
              const event = JSON.parse(jsonStr);
              const address = event.a;
              if (prices[address]) {
                const newPrice = parseFloat(event.p);
                const oldPrice = prices[address].price_usd;
                prices[address].price_usd = newPrice;
                prices[address].updated_at = Date.now();

                // Broadcast to all connected SSE clients
                broadcastPrice(address, newPrice, oldPrice);
              }
            } catch (e) {
              // Skip non-JSON data lines
            }
          }
        }
      });

      res.on("end", () => {
        console.log("SSE stream ended. Reconnecting in 3s...");
        setTimeout(connect, 3000);
      });

      res.on("error", (err) => {
        console.error("SSE stream error:", err.message);
        setTimeout(connect, 3000);
      });
    });

    req.on("error", (err) => {
      console.error("SSE connection error:", err.message);
      setTimeout(connect, 3000);
    });

    req.write(postData);
    req.end();
  }

  connect();
}

// Broadcast price update to all connected frontend SSE clients
function broadcastPrice(address, newPrice, oldPrice) {
  const token = prices[address];
  const data = JSON.stringify({
    address,
    symbol: token.symbol,
    name: token.name,
    price_usd: newPrice,
    old_price_usd: oldPrice,
    updated_at: token.updated_at,
  });

  for (const client of sseClients) {
    client.write(`data: ${data}\n\n`);
  }
}

// Serve static files
app.use(express.static(path.join(__dirname, "public")));

// REST endpoint: get all current prices
app.get("/api/prices", (req, res) => {
  const tokenList = SOLANA_TOKENS.map((t) => ({
    ...prices[t.address],
  }));
  res.json(tokenList);
});

// SSE endpoint: stream price updates to frontend
app.get("/api/stream", (req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });

  // Send initial prices
  const tokenList = SOLANA_TOKENS.map((t) => prices[t.address]);
  res.write(`data: ${JSON.stringify({ type: "init", tokens: tokenList })}\n\n`);

  sseClients.add(res);

  req.on("close", () => {
    sseClients.delete(res);
  });
});

// Start server immediately, fetch data in background
app.listen(PORT, () => {
  console.log(`Dashboard running at http://localhost:${PORT}`);
  if (proxyAgent) {
    console.log("Using HTTPS proxy for outbound requests");
  }

  console.log("Fetching initial token prices...");
  fetchInitialPrices()
    .then(() => {
      startStreaming();
    })
    .catch((err) => {
      console.error("Initial fetch error:", err.message);
      startStreaming();
    });
});
