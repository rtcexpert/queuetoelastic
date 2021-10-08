# Queue To Elastic

You can use it like this.

```javascript

let tElasticSearch = {
    URL: ELASTIC_SEARCH_URL,
    indexName: ELASTIC_INDEX_NAME,
    indexType: ELASTIC_INDEX_TYPE
  }
  let tRedisConfig = {
    redis: {
      port: REDIS_PORT,
      host: REDIS_HOST,
      db: REDIS_DB
    }
  }
  this.mQueueToElastic = new queuetoelastic("PUSH_TO_ELASTIC_SEARCH",tRedisConfig,tElasticSearch);

import queuetoelastic from 'queuetoelastic';
mSimpleQueue.pause();
mSimpleQueue.addDataTOQueue(`xyz`,{Hello: 1234});
mSimpleQueue.addDataTOQueue(`xyz2`,{Hello: 1234});
mSimpleQueue.addDataTOQueue(`xyz3`,{Hello: 1234});
mSimpleQueue.addDataTOQueue(`xyz4`,{Hello: 1234});

setTimeout(() => {
  mSimpleQueue.resume();
}, 5 * 1000);

mSimpleQueue.attachQueue((job, done) => {
  console.log(job.data.data);
  done();
})

```