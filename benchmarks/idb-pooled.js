const { DBPool } = require('idb-pconnector');
const { creds, tableName, TEST_SCHEMA } = require('../config');
const { benchmark, timeIt, stats, printResults } = require('../utils');

const ITERATIONS = 10;
const BATCH_SIZE = 50;
const POOL_SIZE = 5;

async function runIdbPooledBenchmarks(connCreds) {
  const results = [];

  // idb-pconnector connects locally via *LOCAL — no network creds needed
  console.log(`\nInitializing idb-pconnector pool (${POOL_SIZE} connections)...`);
  const SQL_ATTR_COMMIT = 0;
  const SQL_TXN_NO_COMMIT = 1;
  const poolInitTime = await timeIt('Pool init', async () => {
    // Create all connections upfront so setConnectionAttribute covers them all
    const pool = new DBPool({ url: '*LOCAL' }, { incrementSize: BATCH_SIZE });
    // Disable commitment control so DML works on non-journaled tables
    await pool.setConnectionAttribute({ attribute: SQL_ATTR_COMMIT, value: SQL_TXN_NO_COMMIT });
    return pool;
  });
  results.push({
    label: `Pool init (${POOL_SIZE} conns)`,
    stats: { avg: poolInitTime.durationMs, min: poolInitTime.durationMs, max: poolInitTime.durationMs, median: poolInitTime.durationMs, p95: poolInitTime.durationMs },
  });
  const pool = poolInitTime.result;

  try {
    // 1. VALUES 1 — minimal round-trip via pool.runSql()
    const valuesResult = await benchmark(
      'VALUES 1 (pool.runSql)',
      () => pool.runSql('VALUES 1'),
      ITERATIONS
    );
    results.push(valuesResult);

    // 2. Simple SELECT — all rows
    const selectAllResult = await benchmark(
      'SELECT * (100 rows)',
      () => pool.runSql(`SELECT * FROM ${tableName}`),
      ITERATIONS
    );
    results.push(selectAllResult);

    // 3. Parameterized SELECT via prepareExecute
    const paramSelectResult = await benchmark(
      'SELECT WHERE ID = ? (param)',
      () => pool.prepareExecute(`SELECT * FROM ${tableName} WHERE ID = ?`, [42]),
      ITERATIONS
    );
    results.push(paramSelectResult);

    // 4. SELECT with LIMIT
    const limitResult = await benchmark(
      'SELECT LIMIT 10',
      () => pool.runSql(`SELECT * FROM ${tableName} ORDER BY ID FETCH FIRST 10 ROWS ONLY`),
      ITERATIONS
    );
    results.push(limitResult);

    // 5. COUNT aggregate
    const countResult = await benchmark(
      'SELECT COUNT(*)',
      () => pool.runSql(`SELECT COUNT(*) AS CNT FROM ${tableName}`),
      ITERATIONS
    );
    results.push(countResult);

    // 6. INSERT single row
    let insertId = 6000;
    const insertResult = await benchmark(
      'INSERT single row',
      () => pool.runSql(`INSERT INTO ${tableName} (ID, NAME, VALUE) VALUES (${++insertId}, 'Bench', 99.99)`),
      ITERATIONS
    );
    results.push(insertResult);

    // 7. UPDATE single row
    const updateResult = await benchmark(
      'UPDATE single row',
      () => pool.runSql(`UPDATE ${tableName} SET VALUE = 66.66 WHERE ID = 2`),
      ITERATIONS
    );
    results.push(updateResult);

    // 8. DELETE single row
    let deleteId = 6001;
    const deleteResult = await benchmark(
      'DELETE single row',
      () => pool.runSql(`DELETE FROM ${tableName} WHERE ID = ${deleteId++}`),
      ITERATIONS
    );
    results.push(deleteResult);

    // 9. Sequential batch — 50 queries via pool.runSql()
    const seqBatchResult = await timeIt(
      `Sequential batch (${BATCH_SIZE} queries)`,
      async () => {
        const durations = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          const { durationMs } = await timeIt('', () => pool.runSql('VALUES 1'));
          durations.push(durationMs);
        }
        return durations;
      }
    );
    results.push({
      label: `Seq. batch ${BATCH_SIZE}x VALUES 1`,
      stats: stats(seqBatchResult.result),
    });

    // 10. Concurrent batch — 50 queries via Promise.all
    const concurrentResult = await timeIt(
      `Concurrent batch (${BATCH_SIZE} queries)`,
      async () => {
        const promises = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          promises.push(timeIt('', () => pool.runSql('VALUES 1')));
        }
        const settled = await Promise.all(promises);
        return settled.map((r) => r.durationMs);
      }
    );
    results.push({
      label: `Concurrent ${BATCH_SIZE}x VALUES 1`,
      stats: stats(concurrentResult.result),
    });
    results.push({
      label: `Concurrent total wallclock`,
      stats: { avg: concurrentResult.durationMs, min: concurrentResult.durationMs, max: concurrentResult.durationMs, median: concurrentResult.durationMs, p95: concurrentResult.durationMs },
    });

    // 11. Manual attach + query + detach
    const manualResult = await benchmark(
      'Manual attach+query+detach',
      async () => {
        const conn = pool.attach();
        const stmt = conn.getStatement();
        await stmt.exec('VALUES 1');
        // Let detach() handle statement cleanup
        pool.detach(conn);
      },
      ITERATIONS
    );
    results.push(manualResult);

    // 12. Attach, run 10 queries, then detach
    const bulkManualResult = await benchmark(
      'Attach + 10 queries + detach',
      async () => {
        const conn = pool.attach();
        const stmt = conn.getStatement();
        for (let i = 0; i < 10; i++) {
          await stmt.exec('VALUES 1');
        }
        // Let detach() handle statement cleanup
        pool.detach(conn);
      },
      ITERATIONS
    );
    results.push(bulkManualResult);

  } finally {
    // DBPool connections are returned when detached; no explicit pool.close() needed
  }

  printResults('idb-pconnector Pooled (DBPool) Benchmarks', results);
  return results;
}

// Allow standalone execution
if (require.main === module) {
  require('dotenv').config();
  runIdbPooledBenchmarks().catch((err) => {
    console.error('idb-pconnector pooled benchmarks failed:', err);
    process.exit(1);
  });
}

module.exports = { runIdbPooledBenchmarks };
