'use strict';
/**
Copyright (c) 2015, Derek Elkins.  See LICENSE.

A JavaScript implementation of "Tango: Distributed Data Structures over a Shared Log" by Balakrishnan et al.

@module Tango
*/
(function(factory) {
    if(typeof define === 'function' && define.amd) {
        define(['q'], factory);
    } else if(typeof require === 'function' && typeof exports === 'object' && typeof module === 'object') { 
        module.exports = factory(require('q'));
    } else {
        self.Tango = factory(self.Q);
    }
})(function(Q) {
var noop = function() {};
var Tango;

/**
 * @class Runtime
 */
function TangoRuntime(logStore, stream) {
    var self = this;

    var CHECKPOINT_ENTRY_TYPE = 'checkpoint',
        UPDATE_ENTRY_TYPE = 'update',
        COMMIT_ENTRY_TYPE = 'commit',
        DECISION_ENTRY_TYPE = 'decision',
        INIT_ENTRY_TYPE = 'init';

    var TANGO_NAME_MAP_NAME = '__TANGO_NAME_MAP__';

    var typeRegistry = {};
    var offsets = {};
    var latestSeenOffset = 0;
    var objectRegistry = {};

    var persistentNameMap;

    var advanceTo = function(oid, offset) {
        offsets[oid] = offset;
    };

    var newOid = function() { 
        return (Math.random() * 9007199254740992) | 0;
    };

    /**
     * @callback typeFactory
     * @param {!int} oid The object ID.
     * @param {...*} argument Any additional arguments.
     * @returns {ITangoObject} The constructed object with the given object ID.
     */

    /**
     * Registers a factory function for a new type.
     *
     * @method registerType
     * @param {!string} typeName The name of the type.
     * @param {!typeFactory} factory A factory function for creating elements of the type.  It will be passed
     *  an OID as its first argument, the rest of the arguments of {@link fetch}.
     */
    self.registerType = function(typeName, factory) {
        typeRegistry[typeName] = factory;
    };

    var make = function(typeName, name, oid, args) {
        var factory = typeRegistry[typeName];
        args.unshift(oid);
        var object = factory.apply(null, args);
        return logStore.append(stream, {
            type: INIT_ENTRY_TYPE,
            typeName: typeName,
            oid: oid,
            state: args
        });
    };

    self.getOidByName = function(name) {
        return persistentNameMap.get(self, name)
                .then(function(oid) {
                    if(oid === void(0)) {
                        oid = newOid();
                        return persistentNameMap.put(self, name, oid)
                                .thenResolve(oid);
                    } else {
                        return oid;
                    }
                });
    };

    var internalFetch = function(typeName, name, oid, args) {
        return self.queryHelper(oid)
                .then(function(obj) {
                    var obj = objectRegistry[oid];
                    if(obj !== void(0)) {
                        return obj;
                    } else {
                        return make(typeName, name, oid, args)
                                .then(function() {
                                    return internalFetch(typeName, name, oid, args);
                                });
                    }
                });
    };

    /**
     * This method will fetch the latest version of the object of the given name.
     * If the object has not been created, anywhere currently, it will be created.
     *
     * @method fetch
     * @param {!string} typeName The name of the type to instantiate.
     * @param {!string} name The name of the object.
     * @param {...*} args Any additional arguments needed to instantiate the object.
     * @returns {Promise.<ITangoObject>} A promise that returns the latest version of the object.
     */
    self.fetch = function(typeName, name) {
        var args = Array.prototype.slice.call(arguments, 2);
        return self.getOidByName(name)
                .then(function(oid) { return internalFetch(typeName, name, oid, args); });
    };

    // interface ITango {
    //     Promise updateHelper(int oid, any value);
    //     Promise queryHelper(int oid, optional int stopIndex);
    //     Promise<int> checkpointHelper(int oid, any state);
    // }

    var checkCommitEntry = function(entry, offset) {
        var readSet = entry.readSet, writeSet = entry.writeSet;
        if(readSet.length === 0) { // Write-only transactions can't fail.
            return Q(true);
        } else {
            return checkConflicts(readSet, writeSet, Math.min(readSet[0], writeSet[0]), offset-1);
        }
    };
    
    // Walk the writeSet of a potentially conflicting commit.
    var checkWriteSetConflicts = function(reads, innerEntry, innerOffset) {
        // TODO: XXX Need to be able to skip aborted transactions somehow.
        // Preferably without having to recursively check them.
        return checkCommitEntry(innerEntry, innerOffset)
                .then(function(committed) {
                    if(committed) {
                        var writeSet = innerEntry.writeSet;
                        return Q.all(writeSet.map(function(offset) { return logStore.read(stream, offset); }))
                                .then(function(entries) {
                                    var len = entries.length;
                                    var succeeded = true, entry;
                                    for(var i = 0; i < len; ++i) {
                                        entry = entries[i];
                                        if(entry.type === COMMIT_ENTRY_TYPE || entry.type === DECISION_ENTRY_TYPE)
                                            throw 'TangoRuntime.checkWriteSetConflicts: Bad write set: ' + JSON.stringify(writeSet);
                                        if(reads[entry.oid] !== void(0)) {
                                            succeeded = false;
                                            break;
                                        }
                                    }
                                    return succeeded;
                                });
                    } else { 
                        return true;
                    }
                });
    };

    // Walk the log seeing if any writes conflict with the reads in readSet in the interval.
    // Pre-conditions:
    //  - readSet and writeSet are in order of ascending offsets
    //  - Offsets in readSet or writeSet are always INIT/UPDATE/CHECKPOINT, NEVER COMMIT/DECISION.
    //  - readSet and writeSet are disjoint.
    //
    // TODO: XXX For now, any writes to an OID (outside of the transaction) conflict.
    var checkConflicts = function(readSet, writeSet, startIndex, stopIndex) {
        var reads = {}, readIx = 0, writeIx = 0;
        var aborted = false;
        var sidetracks = [];
        var deferred = Q.defer();
        var observer = logStore.traverse(stream, startIndex, stopIndex);
        observer.onNext = function(entry, offset) {
            if(offset === readSet[readIx]) {
                if(entry.type === COMMIT_ENTRY_TYPE || entry.type === DECISION_ENTRY_TYPE)
                    throw 'TangoRuntime.checkConflicts: Bad read set: ' + JSON.stringify(readSet) + ' at offset ' + offset;
                reads[entry.oid] = entry;
                readIx++;
                return true;
            } else if(offset === writeSet[writeIx]) {
                if(entry.type === COMMIT_ENTRY_TYPE || entry.type === DECISION_ENTRY_TYPE)
                    throw 'TangoRuntime.checkConflicts: Bad write set: ' + JSON.stringify(writeSet) + ' at offset ' + offset;
                writeIx++;
                return true;
            } else {
                if(entry.type === COMMIT_ENTRY_TYPE && !entry.deleted) {
                    sidetracks.push(checkWriteSetConflicts(reads, entry, offset));
                    return true;
                } else if(entry.type === DECISION_ENTRY_TYPE && !entry.deleted) { // TODO
                    throw 'TangoRuntime.checkConflicts: DECISION_ENTRY_TYPE not implemented.';
                } else if(reads[entry.oid] !== void(0) && !entry.speculative && !entry.deleted) { // conflict
                    // TODO: This needs to be more fine-grained, which also means using an
                    // object indexed only by oid is inadequate.
                    aborted = true;
                    return false;
                } else {
                    return true;
                }
            }
        };
        observer.onError = function(err) { deferred.reject(err); };
        observer.onCompleted = function() { 
            if(aborted) {
                deferred.resolve(false);
            } else {
                Q.all(sidetracks).then(function(didSucceed) {
                    var len = didSucceed.length;
                    for(var i = 0; i < len; ++i) {
                        if(!didSucceed[i]) {
                            deferred.resolve(false);
                        }
                    }
                    deferred.resolve(true);
                }).done();
            }
        };
        return deferred.promise;
    };

    var applyInit = function(entry, oid) {
        var factory = typeRegistry[entry.typeName];
        if(objectRegistry[oid] !== void(0)) throw 'Tango.Runtime.applyInit: Duplicate init entries.';
        if(factory === void(0)) throw 'Tango.Runtime.applyInit: Unrecognized type: ' + entry.typeName;
        objectRegistry[oid] = factory.apply(null, entry.state);
    };

    var applyWrite = function(entry, offset) {
        var oid = entry.oid;
        var entryType = entry.type;
        if(entryType === INIT_ENTRY_TYPE) {
            applyInit(entry, oid);
        } else {
            var obj = objectRegistry[oid];
            if(obj === void(0)) throw 'Tango.Runtime.applyWrite: Unregistered object for OID: ' + oid;
            if(entryType === UPDATE_ENTRY_TYPE) {
                obj.applyUpdate(entry.value, offset); 
            } else if(entryType === CHECKPOINT_ENTRY_TYPE) {
                obj.applyCheckpoint(entry.state, offset); 
            } else {
                throw 'Tango.Runtime.applyWrite: Unexpected entry type, ' + entryType + ', at offset ' + offset;
            }
        }
        advanceTo(oid, offset);
    };

    var applyWrites = function(writeOffsets) {
        return Q.all(writeOffsets.map(function(offset) { 
                    return logStore.read(stream, offset)
                            .then(function(entry) {
                                return { entry: entry, offset: offset };
                            });
                }))
                .then(function(writeEntries) {
                    writeEntries.forEach(function(e) {
                        applyWrite(e.entry, e.offset);
                    });
                });
    };

    var handleEntry = function(entry, offset) {
        if(latestSeenOffset >= offset) return; // TODO: Or should this be an error?
        latestSeenOffset = offset;
        if(entry.deleted || entry.speculative) return; // Ignore these records.
        var oid, obj;
        switch(entry.type) {
            case INIT_ENTRY_TYPE:
                oid = entry.oid;
                applyInit(entry, oid);
                advanceTo(oid, offset);
                break;
            case CHECKPOINT_ENTRY_TYPE:
                oid = entry.oid;
                obj = objectRegistry[oid];
                if(obj === void(0)) throw 'Tango.Runtime.handleEntry: Unregistered object for OID: ' + oid;
                obj.applyCheckpoint(entry.state, offset);
                advanceTo(oid, offset);
                break;
            case UPDATE_ENTRY_TYPE:
                oid = entry.oid;
                obj = objectRegistry[oid];
                if(obj === void(0)) throw 'Tango.Runtime.handleEntry: Unregistered object for OID: ' + oid;
                obj.applyUpdate(entry.value, offset); 
                advanceTo(oid, offset);
                break;
            case COMMIT_ENTRY_TYPE:
                var readSet = entry.readSet, writeSet = entry.writeSet;
                if(readSet.length === 0) { // Write-only transactions can't fail.
                    return applyWrites(writeSet);
                } else {
                    return checkConflicts(readSet, writeSet, Math.min(readSet[0], writeSet[0]), offset-1)
                            .then(function(succeeded) {
                                if(succeeded) {
                                    return applyWrites(writeSet);
                                } else {
                                    return; // Commit was aborted, so we just ignore it.
                                }
                            });
                }
            case DECISION_ENTRY_TYPE:
                throw 'Tango.Runtime.handleEntry: Decision entry type not implemented.';
                break;
            default:
                throw 'Tango.Runtime.handleEntry: Unexpected entry type (' + entry.type + ')';
        };
    };

    /**
     * @method queryHelper
     *
     * @param {!int} oid The OID of the object being queried.
     * @param {int} [stopIndex] How far to progress in the log. (Ignored)
     * @returns {Promise} A promise that completes once the object has been made up-to-date.
     */
    // The oid is not needed for reads outside of a transaction
    self.queryHelper = function(_oid/*, stopIndex*/) {
        //// TODO: Try to find a more recent checkpoint in this situation.
        //if(stopIndex !== void(0) && stopIndex < startIndex) {
        //    // TODO: I haven't really decided what the semantics of rolling back in time mean.
        //    // My current thought is that you should be able to fetch a read-only copy from the
        //    // runtime and so I don't think having stopIndex here makes sense.
        //    throw 'Tango.logStore.queryHelper: Rollback not implemented.';
        //
        //    startIndex = 1; // Start at the beginning.
        //}

        var deferred = Q.defer();
        var observer, sidetrack, onCompleted;
        var onNext = function(entry, offset) {
            sidetrack = handleEntry(entry, offset);
            return !sidetrack;
        };
        var onError = function(err) { deferred.reject(err); };
        onCompleted = function() { 
            if(sidetrack) {
                sidetrack.then(function() {
                    observer = logStore.traverse(stream, latestSeenOffset+1/*, stopIndex*/);
                    observer.onError = onError;
                    observer.onNext = onNext;
                    observer.onCompleted = onCompleted;
                }).done();
                sidetrack = void(0);
            } else {
                deferred.resolve(); 
            }
        };
        observer = logStore.traverse(stream, latestSeenOffset+1/*, stopIndex*/);
        observer.onError = onError;
        observer.onNext = onNext;
        observer.onCompleted = onCompleted;
        return deferred.promise;
    };

    var logUpdate = function(oid, value, speculative) {
        return logStore.append(stream, {
            type: UPDATE_ENTRY_TYPE,
            oid: oid,
            value: value,
            deleted: false,
            speculative: false
        });
    };

    self.updateHelper = function(oid, value) {
        return logUpdate(oid, value, false);
    };
    var logCheckpoint = function(oid, state, speculative) {        
        return logStore.append(stream, {
            type: CHECKPOINT_ENTRY_TYPE,
            oid: oid,
            state : state,
            deleted: false,
            speculative: speculative
        });
    };

    self.checkpointHelper = function(oid, state) {
        return logCheckpoint(oid, state, false);
    };

    // interface ITangoRuntime : ITango {
    //     void forget(int offset);
    //     ITangoTransaction beginTransaction();
    // }

    self.forget = function(offset) {
        throw 'Tango.Runtime.forget: not implemented';
    };

    // interface ITangoTransaction : ITangoRuntime {
    //     Promise commit();
    //     Promise abort();
    // }

    // TODO: This is an unbuffered implementation.  It would be nice to have the buffer size be configurable
    // from 0 (this) to unbounded.
    var TangoTransaction = function(offsets) {
        var self = this;
        var writeSet = [], readSet = [], completedMsg = '';
        var readHash = {}, writeHash = {};

        self.queryHelper = function(oid/*, stopIndex*/) {
            if(completedMsg !== '') return Q.reject(completedMsg);
            // TODO: XXX Don't include reads that occur after writes conflicting writes.
            // E.g. register.set(T, f(register.get(T))); register.get(T);  The second get should NOT
            // record a read.  The second get should also see the value of the set, so this needs to notify
            // a parallel copy of the object to apply the update represented by the write.
            // 
            // And, um, don't push duplicate reads ...
            if(!readHash[oid] && !writeHash[oid]) {
                readSet.push(offsets[oid]);
                readHash[oid] = true;
            }
            return Q();
        };

        self.updateHelper = function(oid, value) {
            if(completedMsg !== '') return Q.reject(completedMsg);
            return logUpdate(oid, value, true)
                    .then(function(offset) { 
                        writeSet.push(offset); 
                        writeHash[oid] = true;
                    });
        };

        self.checkpointHelper = function(oid, state) {
            if(completedMsg !== '') return Q.reject(completedMsg);
            return logCheckpoint(oid, state, true)
                    .then(function(offset) { 
                        writeSet.push(offset); 
                        writeHash[oid] = true;
                    });
        };

        /**
         * @method commit
         * @returns {Promise} A promise that completes when the transaction commits successfully.
         */
        self.commit = function() {
            if(completedMsg !== '') return Q.reject(completedMsg);
            readSet.sort();
            if(writeSet.length > 0 && readSet.length > 0) { // Read-write transaction
                return logStore.append(stream, {
                    type: COMMIT_ENTRY_TYPE,
                    readSet: readSet,
                    writeSet: writeSet,
                    deleted: false
                }).then(function(offset) {
                    return checkConflicts(readSet, writeSet, Math.min(readSet[0], writeSet[0]), offset-1)
                        .then(function(succeeded) {
                            completedMsg = succeeded ? 'Transaction already committed.'
                                                     : 'Transaction aborted.';
                            return succeeded ? Q() : Q.reject('Transaction aborted.');
                        });
                });
            } else if(writeSet.length > 0) { // Write-only transaction
                completedMsg = 'Transaction already committed.';
                return logStore.append(stream, {
                    type: COMMIT_ENTRY_TYPE,
                    readSet: readSet,
                    writeSet: writeSet,
                    deleted: false
                }).then(function(offset) {
                    return; // Write-only transactions can't fail.
                });
            } else if(readSet.length > 0) { // Read-only transaction
                return checkConflicts(readSet, writeSet, readSet[0], readSet[readSet.length-1])
                        .then(function(succeeded) {
                            completedMsg = succeeded ? 'Transaction already committed.'
                                                     : 'Transaction aborted.';
                            return succeeded ? Q() : Q.reject('Transaction aborted');
                        });
            } else { // Empty transaction
                completedMsg = 'Transaction already committed.';
                return Q();
            }
        };

        self.abort = function() {
            if(completedMsg !== '') return Q.reject(completedMsg);
            completedMsg = 'Transaction aborted.';
            return Q();
        };

        self.forget = function(offset) {
            throw 'TangoTransaction.forget: not implemented';
        };

        /**
         * @method beginTransaction
         * @returns {Promise.<ITangoTransaction>} A promise returning a new nested transaction.
         */
        self.beginTransaction = function() {
            return Q({
                localCompletedMsg: '',
                queryHelper: self.queryHelper,
                updateHelper: self.updateHelper,
                checkpointHelper: self.checkpointHelper,
                forget: self.forget,
                commit: function() {
                    if(completedMsg !== '') return Q.reject(completedMsg);
                    if(localCompletedMsg !== '') return Q.reject(localCompletedMsg);
                    localCompletedMsg = 'Transaction already committed.';
                    return Q(); // Do nothing.
                },
                abort: self.abort, 
                beginTransaction: self.beginTransaction
            });
        };
    };

    /**
     * @method beginTransaction
     * @returns {Promise.<ITangoTransaction>} A promise returning a new transaction.
     */
    self.beginTransaction = function() {
        return  self.queryHelper(0).then(function() {
            var offsetsSnapshot = {};
            Object.keys(offsets).forEach(function(oid) {
                offsetsSnapshot[oid] = offsets[oid];
            });
            return new TangoTransaction(offsetsSnapshot);
        });
    };

    // Initialization

    self.registerType(Tango.Register.TYPE_NAME, Tango.Register.factory);
    self.registerType(Tango.Counter.TYPE_NAME, Tango.Counter.factory);
    self.registerType(Tango.Queue.TYPE_NAME, Tango.Queue.factory);
    self.registerType(Tango.Map.TYPE_NAME, Tango.Map.factory);

    self.init = function() {
        var nameDict = {};
        nameDict[TANGO_NAME_MAP_NAME] = 0;
        return internalFetch(Tango.Map.TYPE_NAME, TANGO_NAME_MAP_NAME, 0, [nameDict])
                .then(function(pm) { persistentNameMap = pm; return self; });
    };
}

/**
 * @method init
 * @returns {Promise.<ITangoRuntime>} A promise that returns a ready `Runtime` object.
 */
TangoRuntime.init = function(logStore, stream) {
    return new TangoRuntime(logStore, stream).init();
};

// interface ITangoObject {
//     void applyUpdate(any value, int offset);
//     void applyCheckpoint(any state, int offset);
// }

/**
 * A single piece of durable, distributed mutable state.
 *
 * @class Register
 * @param {*} initialValue The initial value of the register.
 */
function TangoRegister(oid, initialValue) {
    var self = this;
    var box = initialValue;
    self.applyUpdate = function() {
        throw 'TangoRegister.applyUpdate: This never updates, it only ever checkpoints so we shouldn\'t see an applyUpdate.'
    };
    self.applyCheckpoint = function(state) {
        box = state;
    };

    /**
     * @method get
     * @param {!ITango} T The transaction that contains this action.
     * @returns {Promise.<*>} A promise that returns the value of the register.
     */
    self.get = function(T) {
        return T.queryHelper(oid).then(function() { return box; });
    };

    /**
     * @method set
     * @param {!ITango} T The transaction that contains this action.
     * @param {!any} v The new value for the register.
     * @returns {Promise} A promise that completes once the set has happened.
     */
    self.set = function(T, v) {
        return T.checkpointHelper(oid, v).then(noop);
    };
}
TangoRegister.TYPE_NAME = 'Tango.Register';
TangoRegister.factory = function(oid, value) {
    return new TangoRegister(oid, value);
};

/**
 * A durable, distributed counter.
 *
 * Using a {@link Register} as a counter would require a transaction on each increment.  Incrementing
 * a counter, though, is atomic.
 *
 * @class Counter
 * @param {int} [initialValue=0] The initialValue of the counter.
 */
function TangoCounter(oid, initialValue) {
    var self = this;
    var box = initialValue || 0;
    self.applyUpdate = function(delta) {
        box += delta;
    };
    self.applyCheckpoint = function(state) {
        box = state;
    };

    /**
     * @method get
     * @param {!ITango} T The transaction that contains this action.
     * @returns {Promise.<int>} A promise that returns the current value of the counter.
     */
    self.get = function(T) {
        return T.queryHelper(oid).then(function() { return box; });
    };

    /**
     * @method inc
     * @param {!ITango} T The transaction that contains this action.
     * @param {int} [delta=1] The amount to add to the counter.  This can be negative.
     * @returns {Promise} A promise that completes once the increment has happened.
     */
    self.inc = function(T, delta) {
        return T.updateHelper(oid, delta || 1).then(noop);
    };

    /**
     * @method checkpoint
     * @param {!ITango} T The transacton that contains this action.
     * @returns {Promise} A promise that completes once the checkpoint is saved.
     */
    self.checkpoint = function(T) {
        return T.checkpointHelper(oid, box).then(noop);
    };
}
TangoCounter.TYPE_NAME = 'Tango.Counter';
TangoCounter.factory = function(oid, value) {
    return new TangoCounter(oid, value);
};

/**
 * A durable, distributed FIFO queue.
 *
 * @class Queue
 */
function TangoQueue(oid) {
    var self = this;
    var head = null, tail = null;
    self.applyUpdate = function(op) {
        var entry;
        if(op === 'dequeue') {
            if(head === null) return; // 0-item queue
            var newHead = head.next;
            if(newHead === null) { // 1-item queue
                head = null;
                tail = null;
            } else { // n-item queue, n > 1
                head = newHead;
            }
        } else { // op === { item: * }
            if(head === null) { // 0-item queue
                entry = { value: op.item, next: null };
                head = entry;
                tail = entry;
            } else { // n-item queue, n > 0
                entry = { value: op.item, next: null };
                tail.next = entry;
                tail = entry;
            }
        }
    };
    self.applyCheckpoint = function(state) {
        var len = state.length;
        if(len === 0) {
            head = null;
            tail = null;
        } else {
            head = { value: state[0], next: null };
            tail = head;
            for(var i = 1; i < len; ++i) {
                tail.next = { value: state[i], next: null };
                tail = tail.next;
            }
        }
    };

    /**
     * @method enqueue
     * @param {!ITango} T The transaction that contains this action.
     * @param {*} item The item to add to the queue.
     * @returns {Promise} A promise that completes once the item is enqueued.
     */
    self.enqueue = function(T, item) {
        return T.updateHelper(oid, { item: item }).then(noop);
    };

    /**
     * This reads the end of the queue without modifying it.
     *
     * @method head
     * @param {!ITango} T The transaction that contains this action.
     * @returns {Promise.<*>} A promise that returns the item at the front of the queue or undefined if the queue is empty.
     */
    self.head = function(T) {
        return T.queryHelper(oid).then(function() { 
            return head === null ? void(0) : head.value;
        });
    };

    /**
     * Removes the last item in the queue or does nothing if the queue is empty.
     *
     * @method dequeue
     * @param {!ITango} T The transaction that contains this action.
     * @returns {Promise} A promise that completes once the item has been removed from the queue.
     */
    self.dequeue = function(T) {
        return T.updateHelper(oid, 'dequeue').then(noop);
    };

    /**
     * @method checkpoint
     * @param {!ITango} T The transacton that contains this action.
     * @returns {Promise} A promise that completes once the checkpoint is saved.
     */
    self.checkpoint = function(T) {
        var state = [];
        var cursor = head;
        while(cursor !== null) {
            state.push(cursor.value);
            cursor = cursor.next;
        }
        return T.checkpointHelper(oid, state).then(noop);
    };
}
TangoQueue.TYPE_NAME = 'Tango.Queue';
TangoQueue.factory = function(oid) {
    return new TangoQueue(oid);
};

/**
 * A durable, distributed map.
 *
 * @class Map
 * @param {Object.<string, *>} [initialMapping={}] Initial mapping.
 */
function TangoMap(oid, initialMapping) {
    var self = this;
    var map = initialMapping || {};
    self.applyUpdate = function(u) {
        map[u[0]] = u[1];
    };
    self.applyCheckpoint = function(state) {
        map = state;
    };

    /**
     * @method get
     * @param {!ITango} T The transaction that contains this action.
     * @param {!string|int} key The key to look up.
     * @returns {Promise.<*>} A promise that returns the current value of the key or undefined if the key does not exist.
     */
    self.get = function(T, key) {
        return T.queryHelper(oid).then(function() { return map[key]; });
    };

    /**
     * @method put
     * @param {!ITango} T The transaction that contains this action.
     * @param {!string|int} key The key to update.
     * @param {*} value The value to assign to the key.
     * @returns {Promise} A promise that completes once the put has happened.
     */
    self.put = function(T, key, value) {
        return T.updateHelper(oid, [key, value]).then(noop);
    };

    /**
     * @method checkpoint
     * @param {!ITango} T The transacton that contains this action.
     * @returns {Promise} A promise that completes once the checkpoint is saved.
     */
    self.checkpoint = function(T) {
        return T.checkpointHelper(oid, map).then(noop);
    };
}
TangoMap.TYPE_NAME = 'Tango.Map';
TangoMap.factory = function(oid, initialMapping) {
    return new TangoMap(oid, initialMapping);
};

// Exports

Tango = {
    Runtime: TangoRuntime,
    Counter: TangoCounter,
    Queue: TangoQueue,
    Map: TangoMap,
    Register: TangoRegister
};

return Tango;
});
