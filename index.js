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

const debug = require('debug')('signalk-from-batch-format');
const trace = require('debug')('signalk-from-batch-format:trace');

const _ = require('lodash');
const from_batch = require('signalk-batcher').from_batch_to_delta;
const zlib = require('zlib');

const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { SQSClient, DeleteMessageBatchCommand, ReceiveMessageCommand } = require("@aws-sdk/client-sqs");
const { AbortController } = require("@smithy/abort-controller");

const s3_client = new S3Client();
const sqs_client = new SQSClient();

module.exports = function(app) {
    const TIMEOUT_S = 20;
    let _running = false;
    const _abort_controller = new AbortController();

    let _process_message = async function(body) {
        const read_stream = await _get_from_s3(body);
        if (read_stream) {
            const batch = await _parse_json(read_stream);

            _publish_to_signalk(batch);
        }
    };

    let _get_from_s3 = async function(body) {
        const s3_record = JSON.parse(body.Message).Records[0].s3;
        const s3_bucket = s3_record.bucket.name;
        const s3_key = decodeURIComponent(s3_record.object.key);

        trace(`new object in s3: ${s3_bucket} ${s3_key}`);

        const params = {
            Bucket: s3_bucket,
            Key: s3_key
        };
        const cmd = new GetObjectCommand(params);
        try {
            const response = await s3_client.send(cmd);
            return response.Body;
        } catch (err) {
            debug(`error from s3 get ${err}`);
            return undefined;
        }
    };

    let _parse_json = function(read_stream) {
        return new Promise(function(resolve, reject) {
            const unzipped_stream = read_stream.pipe(zlib.createGunzip());

            let str = '';
            unzipped_stream.on('error', reject);
            unzipped_stream.on('data', data => str += data);
            unzipped_stream.on('end', () => resolve(JSON.parse(str)));
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

    let _publish_to_signalk = function(batch) {
        const deltas = from_batch(batch);
        deltas.forEach(function(delta) {
            // TODO: it'd be better to build up a batch and send a single
            // delta, but since handleMessage overrides the source.label
            // attribute, I have to pretend to be multiple providers
            // instead of a single provider.  Additionally, I'm forced to pass
            // in $source, rather than the source object.  Sigh.
            //
            // The real TODO is to fix signalk itself, once I understand the
            // reasoning as to why it works this way.
            const $source = delta.updates.$source;
            app.handleMessage($source, delta);
        });
    };

    let _delete_from_sqs = async function(options, messages) {
        // now that we've received and processed them, communicate
        // success by deleting them from the queue
        const receipt_handles = messages.map(function(m) {
            return {
                Id: m.MessageId,
                ReceiptHandle: m.ReceiptHandle
            };
        });

        try {
            await sqs_client.send(new DeleteMessageBatchCommand({
                QueueUrl: options.sqs_url,
                Entries: receipt_handles
            }));
            trace('done sqs delete');
        } catch (err) {
            debug(`could not delete from sqs ${err}`);
        }
    };

    let _poll_sqs = async function(options) {
        const params = {
            QueueUrl: options.sqs_url,
            WaitTimeSeconds: TIMEOUT_S,
            // 10 is the most messages we can consume, so get that, as we're
            // the only consumer and we only care about the latest data
            MaxNumberOfMessages: 10
        };
        const request_start = Date.now();

        try {
            const response = await sqs_client.send(
                new ReceiveMessageCommand(params),
                { abortSignal: _abort_controller.signal }
            );

            trace(`sqs response: ${response}`);
            const messages = response.Messages;

            if (_.isUndefined(messages) || messages.length == 0) {
                trace('sqs has no messages to process');
            } else {
                // we care about the most recent data, so we only want the last message
                const message = messages[messages.length - 1];

                // process the message
                trace('process message body');
                await _process_message(JSON.parse(message.Body));
                trace('done process message body');

                await _delete_from_sqs(options, messages);
            }
        } catch (err) {
            console.log(err, err.stack);
        }

        // schedule a new sqs poll
        if (_running) {
            const since_start_ms = Math.min(Date.now() - request_start, 1000);

            // never make more than one call every 1s
            const delay_ms = since_start_ms >= 1000 ? 0 : 1000;

            setTimeout(function() {
                _poll_sqs(options);
            }, delay_ms);
        }
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

        _abort_controller.abort();
    };

    const _plugin = {
        id: 'signalk-from-batch-format',
        name: 'Batch format ingestor',
        description: 'SignalK server plugin that ingests batched json files from S3/SQS',

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
