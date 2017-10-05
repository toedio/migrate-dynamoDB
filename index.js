'use strict';

var config = require('./config.js'),
    aws = require('aws-sdk'),
    async = require('async'),
    source_dynamoDB = new aws.DynamoDB(config.source),
    destination_dynamoDB = new aws.DynamoDB(config.destination);

module.exports = function() {
    console.log('Starting migration');
    async.eachLimit(config.tables, 1,
        function(table, nextTable) {

            var last_key = null,
                sourceParams = {
                    TableName: table,
                    Limit: config.limitRegisters,
                    Select: config.select
                };

            console.log('Starting table: ' + table);

            async.doDuring(
                function(done) {

                    if (last_key != null)
                        sourceParams.ExclusiveStartKey = last_key;

                    source_dynamoDB.scan(
                        sourceParams,
                        function(err, data) {
                            if (err)
                                next(err);
                            else {
                                last_key = data.LastEvaluatedKey ? data.LastEvaluatedKey : null;
                                async.eachLimit(data.Items, config.limitTables, function(item, next) {
                                    putRegister(destination_dynamoDB, item, table, next);
                                }, done);
                            }
                        });
                },
                function(done) {
                    return done(null, last_key != null)
                },
                function(err) {
                    console.log('Done table: ' + table);
                    nextTable(err);
                }
            );
        },
        logFinishMigration);
}();

function putRegister(destination, item, table, callback) {
    destination.putItem({
        Item: item,
        TableName: table
    }, callback);
}

function logFinishMigration(err) {
    if (err)
        return console.log('Error: ' + err);
    return console.log('Successful');
}