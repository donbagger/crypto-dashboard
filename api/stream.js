const https = require("https");
const { SOLANA_TOKENS } = require("./tokens");

// Vercel serverless function with streaming SSE response
// Proxies DexPaprika SSE stream to the client, avoiding CORS
module.exports = function handler(req, res) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });

  const payload = SOLANA_TOKENS.map((t) => ({
    chain: "solana",
    address: t.address,
    method: "t_p",
  }));

  const postData = JSON.stringify(payload);

  // Build a lookup map for token metadata
  const tokenMap = {};
  SOLANA_TOKENS.forEach((t) => {
    tokenMap[t.address] = { symbol: t.symbol, name: t.name };
  });

  const options = {
    hostname: "streaming.dexpaprika.com",
    path: "/stream",
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "text/event-stream",
      "Content-Length": Buffer.byteLength(postData),
    },
  };

  const upstream = https.request(options, (upstreamRes) => {
    let buffer = "";

    upstreamRes.on("data", (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop();

      for (const line of lines) {
        if (line.startsWith("data:")) {
          const jsonStr = line.slice(5).trim();
          if (!jsonStr) continue;
          try {
            const event = JSON.parse(jsonStr);
            const meta = tokenMap[event.a];
            if (meta) {
              const enriched = JSON.stringify({
                address: event.a,
                symbol: meta.symbol,
                name: meta.name,
                price_usd: parseFloat(event.p),
                updated_at: event.t * 1000,
              });
              res.write(`data: ${enriched}\n\n`);
            }
          } catch (e) {
            // skip
          }
        }
      }
    });

    upstreamRes.on("end", () => {
      res.end();
    });

    upstreamRes.on("error", () => {
      res.end();
    });
  });

  upstream.on("error", (err) => {
    res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
    res.end();
  });

  upstream.write(postData);
  upstream.end();

  // Clean up when client disconnects
  req.on("close", () => {
    upstream.destroy();
  });
};
