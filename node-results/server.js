import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { WebSocketServer } from 'ws';
import pg from 'pg';

const app = express();
app.use(cors());
const PORT = process.env.PORT || 8090;
const pool = new pg.Pool({ connectionString: process.env.PG_URL });

async function getTotals(electionId) {
  const { rows } = await pool.query(
    `select candidate_id, votes
       from candidate_tally
      where election_id = $1
      order by votes desc`,
    [electionId]
  );
  return rows;
}

app.get('/healthz', (_, res) => res.json({ ok: true }));

app.get('/results/:electionId', async (req, res) => {
  try {
    const id = Number(req.params.electionId);
    const totals = await getTotals(id);
    res.json({ election_id: id, totals });
  } catch (e) {
    res.status(500).json({ error: 'failed' });
  }
});

const server = app.listen(PORT, () => {
  console.log(`results service on :${PORT}`);
});

// websocket for real time updates
const wss = new WebSocketServer({ server });
const broadcast = (payload) => {
  const msg = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client.readyState === 1) client.send(msg);
  }
};

// listen to postgres NOTIFY
(async () => {
  const client = await pool.connect();
  await client.query('listen vote_inserted');
  client.on('notification', async (n) => {
    try {
      const payload = JSON.parse(n.payload);
      const totals = await getTotals(payload.election_id);
      broadcast({ type: 'results', election_id: payload.election_id, totals });
    } catch {
      // ignore malformed payloads
    }
  });
})();
