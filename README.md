# Mapepire DB Connector Performance Benchmarks

Benchmarks database request timings for **rm-mapepire-js** (connection pool wrapper) and the base **@ibm/mapepire-js** package, comparing standalone and pooled connection patterns.

## Prerequisites

- Node.js (v18+)
- A Mapepire server running on your IBM i system (default port 8076)

## Setup

1. Install dependencies:

   ```bash
   npm install
   ```

2. Copy `.env.example` to `.env` and fill in your IBM i credentials:

   ```bash
   cp .env.example .env
   ```

   | Variable               | Description                          | Required |
   |------------------------|--------------------------------------|----------|
   | `DB_HOST`              | IBM i hostname or IP                 | Yes      |
   | `DB_PORT`              | Mapepire server port (default 8076)  | Yes      |
   | `DB_USER`              | IBM i user profile                   | Yes      |
   | `DB_PASSWORD`          | User password                        | Yes      |
   | `DB_REJECT_UNAUTHORIZED` | Validate TLS certificate (`true`/`false`) | Yes |
   | `DB_SCHEMA`            | Library/schema for the test table    | Yes      |

## Running

### Full Suite

Runs setup, all four benchmark suites, prints a comparison table, then tears down the test table:

```bash
npm start
```

### Individual Benchmarks

Run a specific benchmark suite on its own (assumes the test table already exists):

| Command | Description |
|---------|-------------|
| `npm run bench:standalone` | rm-mapepire-js — single `RmConnection` |
| `npm run bench:pooled` | rm-mapepire-js — `RmPool` (pooled connections) |
| `npm run bench:mapepire-standalone` | @ibm/mapepire-js — single `SQLJob` |
| `npm run bench:mapepire-pooled` | @ibm/mapepire-js — `Pool` (pooled connections) |

### Table Management

Create or drop the test table independently:

```bash
npm run setup      # creates PERFTEST table with 100 rows
npm run teardown   # drops the PERFTEST table
```

## What Gets Benchmarked

Each suite runs 10 iterations per test and reports min, max, average, median, and P95 timings.

**Common tests (all four suites):**

- Connection / pool initialisation
- `VALUES 1` — minimal round-trip latency
- `SELECT *` — fetch all 100 rows
- Parameterised `SELECT WHERE ID = ?`
- `SELECT` with `FETCH FIRST 10 ROWS ONLY`
- `SELECT COUNT(*)`
- `INSERT` single row
- `UPDATE` single row
- `DELETE` single row
- Sequential batch — 50x `VALUES 1` in a loop

**Pooled-only tests:**

- Concurrent batch — 50x `VALUES 1` via `Promise.all`
- Manual attach/detach (rm-mapepire-js) or popJob (mapepire-js) patterns
- Attach + 10 queries + detach (rm-mapepire-js only)

## Output

Each suite prints its own results table. When running the full suite (`npm start`), a final 4-way comparison table is printed showing average timings across all suites side by side.
