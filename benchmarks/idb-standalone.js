const { Connection } = require('idb-pconnector');
const { creds, tableName, TEST_SCHEMA } = require('../config');
const { benchmark, timeIt, stats, printResults } = require('../utils');

const ITERATIONS = 10;
const BATCH_SIZE = 50;

async function runIdbStandaloneBenchmarks(connCreds) {
  const results = [];

  // idb-pconnector connects locally via *LOCAL — no network creds needed
  console.log('\nConnecting (idb-pconnector standalone)...');
  const SQL_ATTR_COMMIT = 0;
  const SQL_TXN_NO_COMMIT = 1;
  const connTime = await timeIt('Connection init', async () => {
    const conn = new Connection({ url: '*LOCAL' });
    // Disable commitment control so DML works on non-journaled tables
    conn.getConnection().setConnAttr(SQL_ATTR_COMMIT, SQL_TXN_NO_COMMIT);
    return conn;
  });
  results.push({
    label: 'Connection init',
    stats: { avg: connTime.durationMs, min: connTime.durationMs, max: connTime.durationMs, median: connTime.durationMs, p95: connTime.durationMs },
  });
  const conn = connTime.result;

  try {
    // Helper: run a non-parameterized query
    async function execQuery(sql) {
      const stmt = conn.getStatement();
      const result = await stmt.exec(sql);
      stmt.close();
      return result;
    }

    // Helper: run a parameterized query
    async function execPrepared(sql, params) {
      const stmt = conn.getStatement();
      await stmt.prepare(sql);
      await stmt.bindParameters(params);
      await stmt.execute();
      const rows = await stmt.fetchAll();
      stmt.close();
      return rows;
    }

    // 1. VALUES 1 — minimal round-trip latency
    const valuesResult = await benchmark(
      'VALUES 1 (round-trip)',
      () => execQuery('VALUES 1'),
      ITERATIONS
    );
    results.push(valuesResult);

    // 2. Simple SELECT — all rows
    const selectAllResult = await benchmark(
      'SELECT * (100 rows)',
      () => execQuery(`SELECT * FROM ${tableName}`),
      ITERATIONS
    );
    results.push(selectAllResult);

    // 3. Parameterized SELECT
    const paramSelectResult = await benchmark(
      'SELECT WHERE ID = ? (param)',
      () => execPrepared(`SELECT * FROM ${tableName} WHERE ID = ?`, [42]),
      ITERATIONS
    );
    results.push(paramSelectResult);

    // 4. SELECT with LIMIT
    const limitResult = await benchmark(
      'SELECT LIMIT 10',
      () => execQuery(`SELECT * FROM ${tableName} ORDER BY ID FETCH FIRST 10 ROWS ONLY`),
      ITERATIONS
    );
    results.push(limitResult);

    // 5. COUNT aggregate
    const countResult = await benchmark(
      'SELECT COUNT(*)',
      () => execQuery(`SELECT COUNT(*) AS CNT FROM ${tableName}`),
      ITERATIONS
    );
    results.push(countResult);

    // 6. INSERT single row
    let insertId = 5000;
    const insertResult = await benchmark(
      'INSERT single row',
      () => execQuery(`INSERT INTO ${tableName} (ID, NAME, VALUE) VALUES (${++insertId}, 'Bench', 99.99)`),
      ITERATIONS
    );
    results.push(insertResult);

    // 7. UPDATE single row
    const updateResult = await benchmark(
      'UPDATE single row',
      () => execQuery(`UPDATE ${tableName} SET VALUE = 55.55 WHERE ID = 1`),
      ITERATIONS
    );
    results.push(updateResult);

    // 8. DELETE single row
    let deleteId = 5001;
    const deleteResult = await benchmark(
      'DELETE single row',
      () => execQuery(`DELETE FROM ${tableName} WHERE ID = ${deleteId++}`),
      ITERATIONS
    );
    results.push(deleteResult);

    // 9. Sequential batch — 50 VALUES 1 in a loop
    const batchResult = await timeIt(
      `Sequential batch (${BATCH_SIZE} SELECTs)`,
      async () => {
        const durations = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          const { durationMs } = await timeIt('', () => execQuery('VALUES 1'));
          durations.push(durationMs);
        }
        return durations;
      }
    );
    results.push({
      label: `Seq. batch ${BATCH_SIZE}x VALUES 1`,
      stats: stats(batchResult.result),
    });

  } finally {
    conn.disconn();
    conn.close();
  }

  printResults('idb-pconnector Standalone (Connection) Benchmarks', results);
  return results;
}

// Allow standalone execution
if (require.main === module) {
  require('dotenv').config();
  runIdbStandaloneBenchmarks().catch((err) => {
    console.error('idb-pconnector standalone benchmarks failed:', err);
    process.exit(1);
  });
}

module.exports = { runIdbStandaloneBenchmarks };
