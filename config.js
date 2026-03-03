require('dotenv').config();

const creds = {
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || '8076', 10),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  rejectUnauthorized: process.env.DB_REJECT_UNAUTHORIZED === 'true',
};

const TEST_TABLE = 'PERFTEST';
const TEST_SCHEMA = process.env.DB_SCHEMA;

if (!TEST_SCHEMA) {
  console.error('ERROR: DB_SCHEMA is required in .env');
  process.exit(1);
}

const tableName = `${TEST_SCHEMA}.${TEST_TABLE}`;

module.exports = { creds, TEST_TABLE, TEST_SCHEMA, tableName };
