const elasticsearch = require('elasticsearch');
// const { ELASTIC_SEARCH_URL } = process.env;
let client = null;

const connect = async (ELASTIC_SEARCH_URL) => {
    client = new elasticsearch.Client({
        host: ELASTIC_SEARCH_URL,
        log: { type: 'stdio', levels: [] }
    });
    return client;
};

const ping = async () => {
    let attempts = 0;
    const pinger = ({ resolve, reject }) => {
        attempts += 1;
        client
            .ping({ requestTimeout: 30000 })
            .then(() => {
                console.log('Elasticsearch server available');
                resolve(true);
            })
            .catch(() => {
                if (attempts > 100) reject(new Error('Elasticsearch failed to ping'));
                console.log('Waiting for elasticsearch server...');
                setTimeout(() => {
                    pinger({ resolve, reject });
                }, 1000);
            });
    };

    return new Promise((resolve, reject) => {
        pinger({ resolve, reject });
    });
};

module.exports = async (ELASTIC_SEARCH_URL) => {
    if (client != null) {
        return client;
    }
    client = await connect(ELASTIC_SEARCH_URL);
    await ping();
    return client;
};