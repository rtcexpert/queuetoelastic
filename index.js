const dotenv = require('dotenv');
var bullQueue = require('bull')
const uuidv4 = require('uuid/v4');
const EventEmitter = require('events');
dotenv.config({ path: process.env.ENV_PATH || '.env' });
var { REDIS_HOST = 'localhost', REDIS_PORT = 6379, ELASTIC_INDEX_NAME = "", ELASTIC_INDEX_TYPE = "", ELASTIC_SEARCH_URL = "" } = process.env;


/* example To Use package.json - 'bull': '^3.7.0', 'ioredis': '^4.14.1',


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

{collection: "tmpCollection", insertObject: { tmp: 1234, hello: "asdfasdf" } }

setTimeout(() => {
  mSimpleQueue.resume();
}, 5 * 1000);

mSimpleQueue.attachQueue((job, done) => {
  console.log(job.data.data);
  done();
})

*/

class queuetoelastic extends EventEmitter {
  constructor(tQueueName,tRedisObject,tElasticSearch) {
      super()
      this.mActiveStatus = false;
      this.mQueueName = tQueueName;
      this.mForceStop = false;
      if(typeof tQueueName === "undefined") {
        throw new Error("You have t send Queue Name..");
      }

      if(typeof tRedisObject === "undefined") {
        throw new Error("Redis Object Not Found.");
      }

      if(tElasticSearch && tElasticSearch.indexName && tElasticSearch.indexType )
      {
        this.elasticSearchURL = tElasticSearch.URL;
        this.elasticSearchIndexName = tElasticSearch.indexName;
        this.elasticSearchType = tElasticSearch.indexType;
      }
      else
      {
        throw new Error("Elastic Search Config Not Define.");
      }
      
      console.log(`the Queue Name is ${this.mQueueName}`);
      this.jobQueue = new bullQueue(this.mQueueName, tRedisObject);
      this.listenerQEvents();
      this.attachQueue(() => {});
  }

  listenerQEvents()
  {
    var _this = this;
    this.jobQueue.on('completed', function(job, result){
        console.log(`Job Completed..`);
        job.remove()
    })

    this.jobQueue.on('error', function(error) {
      // An error occured.
      _this.mActiveStatus = false;
    })
    
    this.jobQueue.on('paused', function(){
      console.log(`get paused`);
      _this.mActiveStatus = false;
    })
    
    this.jobQueue.on('resumed', function(job){
      console.log(`get resumed`);
      _this.mActiveStatus = true; 
    })

    this.jobQueue.on('active', function(job, jobPromise){
      console.log(`get Active`);
    })
  }

  attachQueue(cb)
  {
      var _this = this;
      this.jobQueue.process(async function(job, done) {
          // cb(job,done);
          // console.log(JSON.stringify(job.data.data, null, 2));
          let tData = {}
          try {
            tData = JSON.parse(job.data.data)
          } catch (error) {
            tData = job.data.data;
          }
          // console.log(JSON.stringify(tData, null, 2));
          console.log(`this.ELASTIC_SEARCH_URL -> ${_this.elasticSearchURL}`)
          console.log(`this.ELASTIC_INDEX_NAME -> ${_this.elasticSearchIndexName}`)
          console.log(`this.ELASTIC_INDEX_TYPE -> ${_this.elasticSearchType}`)
          const getElasticSearchClient = require("./getElasticSearchClient");
          const es = await getElasticSearchClient(_this.elasticSearchURL);
          try {
            let tMP = await es.bulk({
                body: [
                    { index:  { _index: _this.elasticSearchIndexName, _type: _this.elasticSearchType, _id: uuidv4() } },
                    tData
                ]
            });
            console.log(JSON.stringify(tMP, null, 2));
          } catch (e) {
            console.log(e);
          }
          done()
      });
  }

  async clearQueue()
  {
    var _this = this;
    if(this.jobQueue)
    {
        var clean = this.jobQueue.clean.bind(this.jobQueue, 0);
        this.jobQueue.pause()
        .then(clean('completed'))
        .then(clean('active'))
        .then(clean('delayed'))
        .then(clean('failed'))
        .then(function () {
            _this.jobQueue.empty().then(() => {
                console.info(`Queue Clear..`);;
            })
        }).catch((e) => {
          console.error(e);
        });
    }
  }

  async count(res)
  {
    if(this.jobQueue)
    {
      this.jobQueue.count().then((count) => {
          console.error(count);
      }).catch((e) => {
          console.error(`Queue Count get Failed e`); 
      })
    }
  }

  async Stop()
  {
    var _this = this;
    if(this.jobQueue)
    {
      var clean = this.jobQueue.clean.bind(this.jobQueue, 0);
      this.jobQueue.pause()
      .then(clean('completed'))
      .then(clean('active'))
      .then(clean('delayed'))
      .then(clean('failed'))
      .then(function () {
          _this.jobQueue.empty().then(() => {
              console.info(`Queue Stoped`);
          })
      }).then(function () {
          _this.jobQueue.close().then(() => {
            console.info(`Queue Stoped`);
          })
      });
    }
  }

  async pause()
  {
    if(this.jobQueue)
    {
      this.jobQueue.pause().then(function(){
        console.info(`Queue Pause!!`);
      }).catch((e) => {
        console.error(`Pause Queue Failed e`);
      })
    }
  }

  async resume()
  {
    if(this.jobQueue)
    {
      this.jobQueue.resume().then(function(){
          console.info(`Queue Resume..`);
      }).catch((e) => {
        console.error(`Resime Error e`);
      })
    }
  }

  addDataTOQueue(_id,data)
  {
    if(this.jobQueue)
    {
      this.jobQueue.add({ "_id" : _id, "data" : data});
    }
    else
    {
      console.log("Queue is not started...");
    }
  }
}

module.exports = queuetoelastic;