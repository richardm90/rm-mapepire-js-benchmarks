const { RmConnection } = require('rm-mapepire-js');
const { creds, tableName, TEST_SCHEMA } = require('./config');

async function teardown(connCreds) {
  const conn = new RmConnection(connCreds || creds, { libraries: TEST_SCHEMA });
  await conn.init();

  console.log('Dropping test table...');

  try {
    await conn.query(`DROP TABLE ${tableName}`);
    console.log('Teardown complete.');
  } catch (err) {
    console.warn('Teardown warning:', err.message);
  }

  await conn.close();
}

// Allow standalone execution
if (require.main === module) {
  teardown().catch((err) => {
    console.error('Teardown failed:', err);
    process.exit(1);
  });
}

module.exports = { teardown };
