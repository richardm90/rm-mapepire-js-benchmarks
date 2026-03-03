const { creds } = require('./config');
const { setup } = require('./setup');
const { teardown } = require('./teardown');
const { runStandaloneBenchmarks } = require('./benchmarks/standalone');
const { runPooledBenchmarks } = require('./benchmarks/pooled');
const { runMapepireStandaloneBenchmarks } = require('./benchmarks/mapepire-standalone');
const { runMapepirePooledBenchmarks } = require('./benchmarks/mapepire-pooled');
const { runIdbStandaloneBenchmarks } = require('./benchmarks/idb-standalone');
const { runIdbPooledBenchmarks } = require('./benchmarks/idb-pooled');

async function main() {
  console.log('='.repeat(80));
  console.log('  Database Connector Performance Benchmarks');
  console.log('='.repeat(80));
  console.log(`Host: ${creds.host}:${creds.port}`);
  console.log(`User: ${creds.user}`);
  console.log(`Date: ${new Date().toISOString()}`);
  console.log(`Node: ${process.version}`);

  const totalStart = process.hrtime.bigint();

  // 1. Setup
  console.log('\n--- SETUP ---');
  await setup(creds);

  // 2. rm-mapepire-js standalone benchmarks
  console.log('\n--- rm-mapepire-js STANDALONE BENCHMARKS ---');
  const rmStandaloneResults = await runStandaloneBenchmarks(creds);

  // 3. rm-mapepire-js pooled benchmarks
  console.log('\n--- rm-mapepire-js POOLED BENCHMARKS ---');
  const rmPooledResults = await runPooledBenchmarks(creds);

  // 4. @ibm/mapepire-js standalone benchmarks
  console.log('\n--- @ibm/mapepire-js STANDALONE BENCHMARKS ---');
  const mapStandaloneResults = await runMapepireStandaloneBenchmarks(creds);

  // 5. @ibm/mapepire-js pooled benchmarks
  console.log('\n--- @ibm/mapepire-js POOLED BENCHMARKS ---');
  const mapPooledResults = await runMapepirePooledBenchmarks(creds);

  // 6. idb-pconnector standalone benchmarks
  console.log('\n--- idb-pconnector STANDALONE BENCHMARKS ---');
  const idbStandaloneResults = await runIdbStandaloneBenchmarks(creds);

  // 7. idb-pconnector pooled benchmarks
  console.log('\n--- idb-pconnector POOLED BENCHMARKS ---');
  const idbPooledResults = await runIdbPooledBenchmarks(creds);

  // 8. Teardown
  console.log('\n--- TEARDOWN ---');
  await teardown(creds);

  // 9. Summary
  const totalEnd = process.hrtime.bigint();
  const totalMs = Number(totalEnd - totalStart) / 1e6;

  console.log(`\n${'='.repeat(80)}`);
  console.log(`  Total elapsed: ${(totalMs / 1000).toFixed(2)}s`);
  console.log('='.repeat(80));

  // 6-way comparison table
  const commonLabels = [
    'Connection init',
    'VALUES 1',
    'SELECT *',
    'SELECT WHERE ID',
    'SELECT LIMIT',
    'SELECT COUNT',
    'INSERT',
    'UPDATE',
    'DELETE',
    'Seq. batch',
  ];

  const sets = [
    { name: 'rm Standalone', results: rmStandaloneResults },
    { name: 'rm Pooled', results: rmPooledResults },
    { name: 'map Standalone', results: mapStandaloneResults },
    { name: 'map Pooled', results: mapPooledResults },
    { name: 'idb Standalone', results: idbStandaloneResults },
    { name: 'idb Pooled', results: idbPooledResults },
  ];

  console.log(`\n${'='.repeat(127)}`);
  console.log('  Comparison — Average (ms)');
  console.log('='.repeat(127));
  console.log(
    padRight('Test', 30) +
    sets.map((s) => padRight(s.name, 16)).join('')
  );
  console.log('-'.repeat(127));

  for (const keyword of commonLabels) {
    const row = [padRight(keyword, 30)];
    for (const set of sets) {
      const match = set.results.find((r) => r.label.includes(keyword));
      row.push(padRight(match ? match.stats.avg.toFixed(2) : '-', 16));
    }
    console.log(row.join(''));
  }
  console.log('-'.repeat(127));
}

function padRight(str, len) {
  return String(str).padEnd(len);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('\nBenchmark run failed:', err);
    process.exit(1);
  });
