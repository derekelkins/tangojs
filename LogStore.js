'use strict';
/**
Copyright (c) 2015, Derek Elkins.  See LICENSE.

Provides a client-side persistent store.  The exported API is intended to be compatible with a log store
that is remote.

@module LogStore
*/
(function(factory) {
    if(typeof define === 'function' && define.amd) {
        define(['q'], factory);
    } else if(typeof require === 'function' && typeof exports === 'object' && typeof module === 'object') { 
        module.exports = factory(require('q'));
    } else {
        self.LogStore = factory(self.Q);
    }
})(function(Q) {
var indexedDB = self.indexedDB; // TODO: Any clean way to have this work in Node.
if(!indexedDB) throw 'LogStore: Couldn\'t load IndexedDB.';
var localStorage = self.localStorage;
var IDBKeyRange = self.IDBKeyRange;

var LogStore = { DATABASE_NAME: 'LogStore' };

var noop = function() {};
var allTrue = function() { return true; };

/**
 * A set of log streams.
 *
 * @class logStore
 * @param {!IDBDatabase} db An IndexedDB database.
 */
function logStore(db) {
    var self = this;

    /**
     * Append a log entry to a stream.
     *
     * @method append
     * @param {!string} stream The stream to which to append.
     * @param {*} entry Any object that the structured clone algorithm works on.
     * @returns {Promise} A promise returning the key of the new entry.  This is guaranteed to be monotonically increasing.
     */
    self.append = function(stream, entry) {
        var deferred = Q.defer();
        var transaction = db.transaction(stream, 'readwrite');
        var request = transaction.objectStore(stream).add(entry);
        transaction.oncomplete = function() { deferred.resolve(request.result); };
        transaction.onabort = function() { deferred.reject(); };
        return deferred.promise;
    };

    /* This isn't right.  It happens atomically, but it doesn't have a single global offset.
       self.multiAppend = function(streams, entry) {
       var deferred = Q.defer();
       var transaction = db.transaction(streams, 'readwrite');
       transaction.oncomplete = function() { deferred.resolve(); };
       transaction.onabort = function() { deferred.reject(); };
       for(var i = 0; i < streams.length; ++i) {
       transaction.objectStore(streams[i]).add(entry);
       }
       return deferred.promise;
       };
       */

    /**
     * Read an entry at a specified offset.
     *
     * @method read
     * @param {!string} stream The stream from which to read.
     * @param {!int} index The key of the entry.
     * @returns {Promise} A promise returning the entry.
     */
    self.read = function(stream, index) {
        var deferred = Q.defer();
        var transaction = db.transaction(stream, 'readonly');
        transaction.onabort = function() { deferred.reject(); };
        transaction.objectStore(stream).get(index).onsuccess = function(evt) {
            deferred.resolve(evt.target.result);
        };
        return deferred.promise;
    };

    /**
     * Called on each item visited in the traversal.  It will be called in log order.
     *
     * @callback onNext
     * @param {*} value The value at the current log offset.
     * @param {!int} offset The current log offset.
     * @returns {boolean} A boolean specifying whether to continue traversing or not.
     */

    /**
     * Called if an error has occurred during the traversal.
     *
     * @callback onError
     * @param {!error} error The error that occurred.
     */

    /**
     * Called when the traversal stops, either due to onNext returning `false` or the end of the stream being reached.
     * This will not be called if onError is called.
     *
     * @callback onCompleted
     */

    /**
     * Traverse a range of log entries.
     *
     * @method traverse
     * @param {!string} stream The stream to traverse.
     * @param {int} [startIndex] The index of where to start the traversal (inclusive).
     * @param {int} [stopIndex] The index of where to stop the traversal (inclusive).
     * @returns {{onError: onError, onNext: onNext, onCompleted: onCompleted}} A roughly Rx.JS-style Observer.  Overwrite
     *  the methods to handle the events.
     */
    self.traverse = function(stream, startIndex /* Optional */, stopIndex /* Optional */) {
        var keyRange = startIndex === void(0) ? void(0) : 
            stopIndex === void(0) ? IDBKeyRange.lowerBound(startIndex)
                                  : IDBKeyRange.bound(startIndex, stopIndex);

        // Following RxJS naming convention, but not really the feel...
        var observer = { onError: noop, onNext: allTrue, onCompleted: noop };
        var transaction = db.transaction(stream, 'readonly');
        transaction.onabort = function() { observer.onCompleted(); };
        var cursorRequest = transaction.objectStore(stream).openCursor(keyRange);
        cursorRequest.onerror = function(evt) { observer.onError(cursorRequest.error); };
        cursorRequest.onsuccess = function(evt) {
            var cursor = evt.target.result;
            if(cursor) {
                try {
                    if(observer.onNext(cursor.value, cursor.key)) {
                        cursor.continue();
                    } else {
                        observer.onCompleted();
                    }
                } catch(e) {
                    observer.onError(e);
                }
            } else {
                observer.onCompleted();
            }
        };
        return observer;
    };

    /**
     * Return the largest key in the stream.
     *
     * @method latestIndex
     * @param {!string} stream The stream to check.
     * @returns {Promise} A promise returning the largest key.
     */
    self.latestIndex = function(stream) {
        var deferred = Q.defer();
        var transaction = db.transaction(stream, 'readonly');
        transaction.onabort = function() { deferred.reject(); };
        transaction.objectStore(stream).openCursor(void(0), 'prev').onsuccess = function(evt) {
            var cursor = evt.target.result;
            if(cursor) {
                deferred.resolve(cursor.key);
            } else {
                deferred.resolve(0);
            }
        };
        return deferred.promise;
    };
}

/**
 * Initialize a set of log streams.  Currently built over IndexedDB and localStorage.
 *
 * @param {!string[]} streams Names for a set of independent log streams.
 * @returns {Promise} A {@link logStore} object.
 */
LogStore.init = function(streams) {
    if(!Array.isArray(streams) || streams.length === 0) throw 'LogStore.init expects a non-empty array as an argument.';

    var deferred = Q.defer();
    var version = localStorage.getItem('LogStore.VERSION');
    version = version === null ? 0 : parseInt(version);
    var schema = localStorage.getItem('LogStore.SCHEMA');
    schema = schema === null ? [] : JSON.parse(schema);

    var newStreams = streams.filter(function(stream) {
        return schema.indexOf(stream) === -1
    });

    if(newStreams.length > 0) {
        ++version;
    }

    var openRequest = indexedDB.open(LogStore.DATABASE_NAME, version);
    openRequest.onerror = function() { deferred.reject(openRequest.errorCode); };
    openRequest.onsuccess = function(evt) { 
        localStorage.setItem('LogStore.VERSION', version);
        localStorage.setItem('LogStore.SCHEMA', JSON.stringify(schema.concat(newStreams)));
        deferred.resolve(new logStore(evt.target.result));
    };
    openRequest.onupgradeneeded = function(evt) {
        var db = evt.target.result;
        var len = newStreams.length;
        for(var i = 0; i < len; ++i) {
            db.createObjectStore(newStreams[i], { autoIncrement: true });
        }
    };
    return deferred.promise;
};

return LogStore;
});
