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
// DEBUG
Q.longStackSupport = true;

var Tango = {};

function randomInt() {
    return (Math.random() * 9007199254740992) | 0;
}

function TangoRuntime(logStore) {
    var self = this;
    var typeRegistry = {};

    var registerObject = function(typeName, name, oid, object) {
        // TODO
        return Q();
    };

    self.registerType = function(typeName, factory) {
        self.typeRegistry[typeName] = factory;
    };

    self.newOid = function() { return randomInt(); };

    self.fetch = function(typeName, oidOrName) {
        if(typeof(oidOrName) === 'number') {

            Q.reject('TangoRuntime.fetch: Not implemented.');

        } else if(oidOrName) {
            return self.getOidByName(typeName, oidOrName)
                    .then(function(oid) { return self.fetch(typeName, oid); });
        } else {
            return Q.reject('TangoRuntime.fetch: Bad request: ' + oidOrName);
        }
    };

    self.make = function(typeName, name, initialState) {
        var oid = self.newOid();
        var object = typeRegistry[typeName](oid, initialState);
        return self.checkpoint(oid, initialState)
                .then(function(offset) { 
                    self.advanceTo(oid, offset);
                    self.registerObject(typeName, name, oid, object)
                        .then(function() { return object; });
                });
    };

    // ITangoRuntime
    self.checkpoint = function(oid, state) {

    };

    self.forget = function(offset) {

    };

    self.beginTransaction = function() {

    };

    // ITango
    self.queryHelper = function(oid, stopIndex) {

    };

    var logUpdate = function( ) {

    };

    self.updateHelper = function(oid, value) {

    };

    // ITangoObject
    // ...
}

/*
The opaque objects need to be serializable via the structured clone algorithm.

interface ITangoObject {
    void apply(any value, int offset);
    void applyCheckpoint(any state);
    bool conflicts(any oldValue, any newValue);
}

interface ITango {
    Promise updateHelper(int oid, any value);
    Promise queryHelper(int oid, optional int stopIndex);
}

interface ITangoRuntime : ITango {
    Promise<int> checkpoint(int oid, any state);
    void forget(int offset);
    ITangoTransaction beginTransaction();
}

interface ITangoTransaction : ITango {
    Promise commit();
    Promise abort();
}

Example:

function TangoRegister(oid, initialValue) {
    var value = initialValue;
    this.apply = function(v, _idx) {
        value = v;
    };

    // T : ITango
    this.set = function(T, v) {
        return T.updateHelper(v, oid);
    };

    // T : ITango
    this.get = function(T) { 
        return T.queryHelper(oid).then(function() {
            return value; // apply has been called up to the current state by this point.
        });
    };
}

Random Implementation Note:
    When checking a commit record, a recorded write does not invalidate a recorded read.  So as we roll forward
we need to invalidate if we see any conflicting write unless it's in the commit record's write set.

*/

return Tango;
});
