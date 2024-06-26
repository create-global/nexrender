const requireg = require('requireg');

const databaseType = process.env.NEXRENDER_DATABASE_PROVIDER;

/* place to register all plugins */
/* so they will be picked up and resolved by pkg */
if (process.env.NEXRENDER_REQUIRE_PLUGINS) {
    require('@create-global/nexrender-database-redis');
}

if (databaseType !== undefined) {
    module.exports = requireg(`@create-global/nexrender-database-${databaseType}`);
} else {
    module.exports = require('./disk.js');
}
