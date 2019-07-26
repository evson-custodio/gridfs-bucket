const assert = require('assert');
const mongodb = require('mongodb');

function client(uri, options) {
    return new mongodb
        .MongoClient(uri, Object.assign({ useNewUrlParser: true }, options))
        .connect();
}

function bucket(client, options = {}) {
    return Promise.resolve(new mongodb.GridFSBucket(client.db(), options));
}

function write(bucket, filename, metadata = {}) {
    return Promise.resolve(bucket.openUploadStream(filename, {
        metadata: metadata,
        disableMD5: true
    }));
}

function streamById(bucket, id, options = { start: 0, end: 0 }) {
    return Promise.resolve(bucket.openDownloadStream(new mongodb.ObjectId(id), {
        start: options.start || 0,
        end: options.end || 0
    }));
}

function removeById(bucket, id) {
    return new Promise((resolve, reject) => {
        id = new mongodb.ObjectId(id);
        bucket.delete(id, err => {
            if (err) {
                reject(err);
            }
            else {
                resolve(`File ${id} is deleted`);
            }
        });
    });
}

function list(bucket) {
    return bucket.find().toArray();
}

class GFSBucket {

    constructor(uri, options) {
        client(uri, options)
        .then(_client => this._client = _client)
        .then(bucket)
        .then(_bucket => this._bucket = _bucket)
        .catch(err => assert.ifError(err));

        this._express_endpoints = {
            create: (req, res) => {
                this.write(req.headers['filename'], {
                    contentType: req.headers['content-type']
                })
                .then(stream => {
                    req.pipe(stream);
                    stream.on('error', err => {
                        res.json(err);
                    })
                    .on('finish', () => {
                        res.json({
                            message: 'OK'
                        });
                    });
                })
                .catch(err => {
                    res.json(err);
                });
            },
            streamById: (req, res) => {
                this.streamById(req.params.id)
                .then(stream => stream.pipe(res))
                .catch(err => {
                    res.json(err);
                });
            },
            removeById: (req, res) => {
                this.removeById(req.params.id)
                .then(message => {
                    res.json({
                        message: message
                    });
                })
                .catch(err => {
                    res.json(err);
                });
            },
            list: (req, res) => {
                this.list()
                .then(docs => {
                    res.json(docs);
                })
                .catch(err => {
                    res.json(err);
                });
            }
        }
    }

    getExpressEndpoints() {
        return this._express_endpoints;
    }

    write(filename, metadata = {}) {
        return write(this._bucket, filename, metadata);
    }

    streamById(id, options = { start: 0, end: 0 }) {
        return streamById(this._bucket, id, options);
    }

    removeById(id) {
        return removeById(this._bucket, id);
    }

    list() {
        return list(this._bucket);
    }
}

module.exports.client = client;
module.exports.bucket = bucket;
module.exports.write = write;
module.exports.streamById = streamById;
module.exports.removeById = removeById;
module.exports.list = list;
module.exports.GFSBucket = GFSBucket;