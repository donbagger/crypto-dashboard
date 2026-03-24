const { SOLANA_TOKENS } = require("./tokens");

module.exports = async function handler(req, res) {
  try {
    const results = await Promise.allSettled(
      SOLANA_TOKENS.map(async (token) => {
        const url = `https://api.dexpaprika.com/networks/solana/tokens/${token.address}`;
        const resp = await fetch(url);
        const data = await resp.json();
        return {
          symbol: token.symbol,
          name: token.name,
          address: token.address,
          price_usd: data.summary?.price_usd || null,
          change_24h: data.summary?.["24h"]?.last_price_usd_change || 0,
          updated_at: Date.now(),
        };
      })
    );

    const tokens = results.map((r, i) =>
      r.status === "fulfilled"
        ? r.value
        : {
            symbol: SOLANA_TOKENS[i].symbol,
            name: SOLANA_TOKENS[i].name,
            address: SOLANA_TOKENS[i].address,
            price_usd: null,
            change_24h: null,
            updated_at: null,
          }
    );

    res.setHeader("Cache-Control", "s-maxage=5, stale-while-revalidate=10");
    res.json(tokens);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
