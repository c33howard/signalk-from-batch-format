/*
 * Copyright 2020 Craig Howard <craig@choward.ca>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const debug = require('debug')('signalk-from-csv');
const trace = require('debug')('signalk-from-csv:trace');

const _ = require('lodash');
const aws = require('aws-sdk');
const csv = require('fast-csv');
const zlib = require('zlib');

const s3 = new aws.S3();
const sqs = new aws.SQS();

module.exports = function(app) {
    const TIMEOUT_S = 20;
    let _in_flight_request;
    let _running = false;

    let _process_message = async function(body) {
        const s3_result = _get_from_s3(body);
        const rows = await _parse_csv(s3_result);

        _publish_to_signalk(rows);
    };

    let _get_from_s3 = function(body) {
        const s3_record = JSON.parse(body.Message).Records[0].s3;
        const s3_bucket = s3_record.bucket.name;
        const s3_key = decodeURIComponent(s3_record.object.key);

        trace(`new object in s3: ${s3_bucket} ${s3_key}`);

        const params = {
            Bucket: s3_bucket,
            Key: s3_key
        };
        return s3.getObject(params).createReadStream();
    };

    let _parse_csv = function(body) {
        return new Promise(function(resolve, reject) {
            const unzipped_stream = body.pipe(zlib.createGunzip());
            const row_stream = unzipped_stream.pipe(csv.parse({ headers: true }));

            let rows = [];
            row_stream.on('error', reject);
            row_stream.on('data', row => rows.push(row));
            row_stream.on('end', () => resolve(rows));
        });
    };

    let _get_value = function(v) {
        // assume a number, unless we get NaN, in which case assume a string
        if (Number.isNaN(+v)) {
            return v;
        } else {
            return +v;
        }
    };

    let _filter_compound_values = function(updates, re) {
        // extract the string before the last .
        const prefix = function(s) {
            const last_dot = s.lastIndexOf('.');
            return s.substring(0, last_dot);
        };
        // extract the string after the last .
        const suffix = function(s) {
            const last_dot = s.lastIndexOf('.');
            return s.substring(last_dot + 1);
        };

        // partition the updates into those that match the re and those that
        // don't.  Those that do match need to be combined.
        const partitioned = _.partition(updates, u => re.test(u.values[0].path));
        const to_combine = partitioned[0];

        // construct an object with the keys as the suffix of the path, and the
        // values as the values
        const combined_value = {};
        to_combine.forEach(function(c) {
            // assign to combined values
            combined_value[suffix(c.values[0].path)] = c.values[0].value;
        });

        // create a single combined object
        const combined = {
            // timestamps should all be the same, so just use the first
            timestamp: to_combine[0].timestamp,
            values: [{
                // prefixes should all be the same, so just use the prefix of
                // the first
                path: prefix(to_combine[0].values[0].path),
                value: combined_value
            }]
        };

        // join the the combined object with everything else
        return _.concat(partitioned[1], combined);
    };

    let _filter_updates_from_past = function() {
        // ensure we don't ingest old values by tracking the most recent timestamp we've seen
        // TODO: perhaps this should be in signalk core?
        const _timestamps_by_path = {};

        return function(updates) {
            // first, ensure that each path is present in _timestamps_by_path
            updates.forEach(function(u) {
                if (!_timestamps_by_path[u.values[0].path]) {
                    _timestamps_by_path[u.values[0].path] = new Date(0);
                }
            });

            // only keep the updates that are newer
            updates = updates.filter(function(u) {
                return new Date(u.timestamp) > _timestamps_by_path[u.values[0].path];
            });

            // update _timestamps_by_path
            updates.forEach(function(u) {
                _timestamps_by_path[u.values[0].path] = new Date(u.timestamp);
            });

            return updates;
        };
    }();

    let _publish_to_signalk = function(rows) {
        let updates = rows.map(function(row) {
            // we only publish the last timestamp, so remove the path key
            const data = _.omit(row, ['path', 'source']);

            // the rest of the keys are timestamps
            const timestamps = _.keys(data);
            // sort them, so the largest timestamp is now last
            timestamps.sort();

            // extract the last timestamp and associated value
            const last_timestamp = timestamps[timestamps.length - 1];
            const value = _get_value(data[last_timestamp]);

            // return a signalk update in delta format
            // (this will be part of a list)
            const delta = {
                timestamp: new Date(parseInt(last_timestamp)).toISOString(),
                values: [{
                    path: row.path,
                    value: value
                }]
            };

            // add the source, if it exists
            // TODO: this does no good, because for some reason, signalk stomps
            // source.label with my providerId (ie pluginId).  You're killing
            // me smalls.
            if (false && row.source) {
                delta.source = {
                    label: row.source
                };
            }

            return delta;
        });

        // fix the stupid combined objects, no doubt I'll need to add to this
        // list.  Sigh...
        updates = _filter_compound_values(updates, /^navigation\.position\.(latitude|longitude)$/);
        updates = _filter_compound_values(updates, /^navigation\.attitude\.(yaw|roll|pitch)$/);

        // ensure we don't regress by ingesting old data based on the timestamp
        updates = _filter_updates_from_past(updates);

        // Ugh, signalk swallows any exceptions, so we can't know if this
        // failed, which means we have no way of knowing whether to consume the
        // item from SQS or not, which means we always consume it.
        app.handleMessage(_plugin.id, {
            updates: updates
        });
    };

    let _delete_from_sqs = function(options, messages) {
        // now that we've received and processed them, communicate
        // success by deleting them from the queue
        const receipt_handles = messages.map(function(m) {
            return {
                Id: m.MessageId,
                ReceiptHandle: m.ReceiptHandle
            };
        });
        sqs.deleteMessageBatch({
            QueueUrl: options.sqs_url,
            Entries: receipt_handles
        }).
            on('success', function(response) {
                trace('done sqs delete');
            }).
            on('error', function(err, response) {
                debug(`could not delete from sqs ${err}`);
            }).
            send();
    };

    let _poll_sqs = function(options) {
        const params = {
            QueueUrl: options.sqs_url,
            WaitTimeSeconds: TIMEOUT_S,
            // 10 is the most messages we can consume, so get that, as we're
            // the only consumer and we only care about the latest data
            MaxNumberOfMessages: 10
        };

        const request_start = Date.now();
        _in_flight_request = sqs.receiveMessage(params).
            on('success', function(response) {
                const messages = response.data.Messages;

                if (_.isUndefined(messages) || messages.length == 0) {
                    trace('sqs has no messages to process');
                    return;
                }

                // we care about the most recent data, so we only want the last message
                const message = messages[messages.length - 1];

                // process the message
                trace('process message body');
                _process_message(JSON.parse(message.Body));
                trace('done process message body');

                _delete_from_sqs(options, messages);
            }).
            on('error', function(err, response) {
                console.log(err, err.stack);
            }).
            on('complete', function(response) {
                // clear the inflight request
                _in_flight_request = undefined;
                // schedule a new sqs poll
                if (_running) {
                    const since_start_ms = Math.min(Date.now() - request_start, 1000);

                    // never make more than one call every 1s
                    const delay_ms = since_start_ms >= 1000 ? 0 : 1000;
                    
                    setTimeout(function() {
                        _poll_sqs(options);
                    }, delay_ms);
                }
            }).
            send();
    };

    let _start = function(options) {
        debug('starting');
        _running = true;

        // TODO: on startup, scan S3 for the most recent timestamp and consume
        // that

        // start polling
        _poll_sqs(options);
    };

    let _stop = function(options) {
        debug('stopping');

        _running = false;

        if (_in_flight_request) {
            _in_flight_request.abort();
            _in_flight_request = undefined;
        }
    };

    const _plugin = {
        id: 'signalk-from-csv',
        name: 'CSV Ingestor',
        description: 'SignalK server plugin that ingests CSV data from S3/SQS',

        schema: {
            type: 'object',
            required: ['sqs_url'],
            properties: {
                sqs_url: {
                    type: 'string',
                    title: 'SQS URL',
                }
            }
        },

        start: _start,
        stop: _stop
    };

    return _plugin;
};
