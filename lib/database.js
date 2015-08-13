/**
 * Module dependencies
 */

var _ = require('lodash');
var async = require('async');
var waterlineCriteria = require('waterline-criteria');
var Aggregate = require('./aggregates');
var Errors = require('waterline-errors').adapter;


var indexExists = function(esclient, name, callback) {
  esclient.indices.exists({
    index: name
  }, function(err, resp) { //, respcode
    if (err) {
      console.log("indexExistserror", err, resp);
    }
    callback(resp);
  });
};

var createIndex = function(esclient, name, callback) {
  esclient.indices.create({
    index: name,
    body: {
      "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1
      }
      // "mappings": {
      //     "cache": {
      //         "_ttl": { "enabled": true }
      //     }
      // }
    }
  }, function(err, resp) {
    if (err) {
      console.log("createIndexerror", err, resp);
    }
    callback(resp.acknowledged);
  });
};

/**
 * An Elasticsearch Datastore
 *
 * @param {Object} config
 * @param {Object} collections
 * @return {Object}
 * @api public
 */

var Database = module.exports = function(config, collections, cb) {
  var self = this;
  this.config = config || {};
  this.collections = collections || {};

  var elasticsearch = require('elasticsearch');
  this.client = new elasticsearch.Client({
    host: this.config.host + ":" + this.config.port,
    log: this.config.log
  });
  indexExists(self.client, self.config.index, function(exists) {
    if (!exists) {
      createIndex(self.client, self.config.index, function() {});
    }
  });
  return this;
};

/**
 * Select
 */
Database.prototype.select = function(collectionName, options, cb) {
  // console.log("select", collectionName, options);
  var searchObj = {
    index: this.config.index,
    type: collectionName
  };
  if (options.where && options.where.id) {
    // Get ONE
    searchObj.id = options.where.id.toString();
    this.client.get(searchObj, function(error, response) {
      cb(null, response._source);
    });

  } else {
    // Get ALL
    this.client.search(searchObj, function(error, response) {
      var results = _.map(response.hits.hits, function(hit) {
        return hit._source;
      });
      cb(null, results);
    });

  }
  // Filter Data based on Options criteria
  // var resultSet = waterlineCriteria(collectionName, this.data, options);
  // // Process Aggregate Options
  // var aggregate = new Aggregate(options, resultSet.results);
  // setTimeout(function() {
  //   if (aggregate.error) return cb(aggregate.error);
  //   cb(null, aggregate.results);
  // }, 0);
};

/**
 * Insert A Record
 */
Database.prototype.insert = function(collectionName, values, cb) {
  // console.log("insert", collectionName, values);
  var self = this;

  self.client.update({
      index: self.config.index,
      type: collectionName,
      id: values.id,
      parent: values._parent,
      body: {
        doc: values,
        "doc_as_upsert": true
      }
    },
    function(error, response) { //error, response
      if (error) {
        console.log("inserterror", error);
      }
      if (response) {
        cb(null, values);
      }
    }
  );

  // var originalValues = _.clone(values);
  // if (!Array.isArray(values)) values = [values];

  // // To hold any uniqueness constraint violations we encounter:
  // var constraintViolations = [];

  // // Iterate over each record being inserted, deal w/ auto-incrementing
  // // and checking the uniquness constraints.
  // for (var i in values) {
  //   var record = values[i];

  //   // Check Uniqueness Constraints
  //   // (stop at the first failure)
  //   constraintViolations = constraintViolations.concat(self.enforceUniqueness(collectionName, record));
  //   if (constraintViolations.length) break;

  //   // Auto-Increment any values that need it
  //   record = self.autoIncrement(collectionName, record);
  //   record = self.serializeValues(collectionName, record);

  //   if (!self.data[collectionName]) return cb(Errors.CollectionNotRegistered);
  //   self.data[collectionName].push(record);
  // }

  // // If uniqueness constraints were violated, send back a validation error.
  // if (constraintViolations.length) {
  //   return cb(new UniquenessError(constraintViolations));
  // }

  // setTimeout(function() {
  //   cb(null, Array.isArray(originalValues) ? values : values[0]);
  // }, 0);
};

/**
 * Update A Record
 */
Database.prototype.update = function(collectionName, options, values, cb) {
  // console.log("update", collectionName, options, values);
  var self = this;

  self.client.update({
      index: self.config.index,
      type: collectionName,
      id: values.id,
      parent: values._parent,
      body: {
        doc: values,
        "doc_as_upsert": true
      }
    },
    function(error, response) { //error, response
      if (error) {
        console.log("updateerror", error);
      }
      if (response) {
        // console.log("update", [values]);
        cb(null, [values]);
      }
    }
  );

  // // Filter Data based on Options criteria
  // var resultSet = waterlineCriteria(collectionName, this.data, options);

  // // Enforce uniquness constraints
  // // If uniqueness constraints were violated, send back a validation error.
  // var violations = self.enforceUniqueness(collectionName, values);
  // if (violations.length) {
  //   return cb(new UniquenessError(violations));
  // }

  // // Otherwise, success!
  // // Build up final set of results.
  // var results = [];
  // for (var i in resultSet.indices) {
  //   var matchIndex = resultSet.indices[i];
  //   var _values = self.data[collectionName][matchIndex];

  //   // Clone the data to avoid providing raw access to the underlying
  //   // in-memory data, lest a user makes inadvertent changes in her app.
  //   self.data[collectionName][matchIndex] = _.extend(_values, values);
  //   results.push(_.cloneDeep(self.data[collectionName][matchIndex]));
  // }

  // setTimeout(function() {
  //   cb(null, results);
  // }, 0);
};

/**
 * Destroy A Record
 */
Database.prototype.destroy = function(collectionName, options, cb) {
  // console.log("destroy", collectionName, options.where.id);
  var self = this;

  self.client.delete({
      index: self.config.index,
      type: collectionName,
      id: options.where.id
    },
    function(error, response) { //error, response
      if (error) {
        console.log("destroyerror", error);
      }
      if (response) {
        cb(null, [{
          id: options.where.id
        }]);
      }
    }
  );
  // // Filter Data based on Options criteria
  // var resultSet = waterlineCriteria(collectionName, this.data, options);

  // this.data[collectionName] = _.reject(this.data[collectionName], function(model, i) {
  //   return _.contains(resultSet.indices, i);
  // });

  // setTimeout(function() {
  //   cb(null, resultSet.results);
  // }, 0);
};
