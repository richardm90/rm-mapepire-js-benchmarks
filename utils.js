/**
 * Run an async function and measure its duration in milliseconds.
 * @param {string} label - descriptive name for the operation
 * @param {Function} fn - async function to execute
 * @returns {{ label: string, durationMs: number, result: any }}
 */
async function timeIt(label, fn) {
  const start = process.hrtime.bigint();
  const result = await fn();
  const end = process.hrtime.bigint();
  const durationMs = Number(end - start) / 1e6;
  return { label, durationMs, result };
}

/**
 * Run an async function N times, collecting durations.
 * @param {string} label - descriptive name
 * @param {Function} fn - async function to execute each iteration
 * @param {number} iterations - number of times to run
 * @returns {{ label: string, durations: number[], stats: object }}
 */
async function benchmark(label, fn, iterations = 10) {
  const durations = [];
  for (let i = 0; i < iterations; i++) {
    const { durationMs } = await timeIt(label, fn);
    durations.push(durationMs);
  }
  return { label, durations, stats: stats(durations) };
}

/**
 * Compute summary statistics for an array of durations.
 */
function stats(durations) {
  const sorted = [...durations].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  const len = sorted.length;
  const p95Index = Math.min(Math.ceil(len * 0.95) - 1, len - 1);
  return {
    min: sorted[0],
    max: sorted[len - 1],
    avg: sum / len,
    median: len % 2 === 0
      ? (sorted[len / 2 - 1] + sorted[len / 2]) / 2
      : sorted[Math.floor(len / 2)],
    p95: sorted[p95Index],
    total: sum,
    count: len,
  };
}

/**
 * Print a table of benchmark results to the console.
 * @param {string} title - section title
 * @param {Array} results - array of { label, stats } objects
 */
function printResults(title, results) {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`  ${title}`);
  console.log('='.repeat(80));
  console.log(
    padRight('Test', 30) +
    padRight('Avg (ms)', 12) +
    padRight('Min (ms)', 12) +
    padRight('Max (ms)', 12) +
    padRight('Median', 12) +
    padRight('P95', 12)
  );
  console.log('-'.repeat(90));

  for (const r of results) {
    const s = r.stats;
    console.log(
      padRight(r.label, 30) +
      padRight(s.avg.toFixed(2), 12) +
      padRight(s.min.toFixed(2), 12) +
      padRight(s.max.toFixed(2), 12) +
      padRight(s.median.toFixed(2), 12) +
      padRight(s.p95.toFixed(2), 12)
    );
  }

  console.log('-'.repeat(90));
}

function padRight(str, len) {
  return String(str).padEnd(len);
}

module.exports = { timeIt, benchmark, stats, printResults };
