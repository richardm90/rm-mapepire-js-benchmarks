const { RmConnection } = require('rm-mapepire-js');
const { creds, tableName, TEST_SCHEMA } = require('./config');

async function setup(connCreds) {
  const conn = new RmConnection(connCreds || creds, { libraries: TEST_SCHEMA });
  await conn.init();

  console.log('Creating test table...');

  // Drop if exists (ignore errors if it doesn't exist)
  try {
    await conn.query(`DROP TABLE ${tableName}`);
  } catch (e) {
    // table didn't exist, that's fine
  }

  await conn.query(`
    CREATE TABLE ${tableName} (
      ID INT NOT NULL,
      NAME VARCHAR(100),
      VALUE DECIMAL(10, 2),
      CREATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (ID)
    )
  `);

  console.log('Seeding 100 rows...');

  for (let i = 1; i <= 100; i++) {
    await conn.query(
      `INSERT INTO ${tableName} (ID, NAME, VALUE) VALUES (${i}, 'Item ${i}', ${(i * 1.5).toFixed(2)})`
    );
  }

  console.log('Setup complete.');
  await conn.close();
}

// Allow standalone execution
if (require.main === module) {
  setup().catch((err) => {
    console.error('Setup failed:', err);
    process.exit(1);
  });
}

module.exports = { setup };
