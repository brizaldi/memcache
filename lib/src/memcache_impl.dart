// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library memcache.impl;

import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import '../memcache.dart';
import '../memcache_raw.dart' as raw;

class MemCacheImpl implements Memcache {
  final raw.RawMemcache _raw;
  final bool _withCas;
  Map<List<int>, int>? _cas;
  Expando<int>? _hashCache;

  MemCacheImpl(this._raw, {withCas = false}) : _withCas = withCas {
    if (withCas) {
      _cas = HashMap(equals: _keyCompare, hashCode: _keyHash);
      _hashCache = Expando<int>();
    }
  }

  // Jenkins's one-at-a-time hash, see:
  // http://en.wikipedia.org/wiki/Jenkins_hash_function.
  int _keyHash(List<int> key) {
    final MASK_32 = 0xffffffff;
    int add32(x, y) => (x + y) & MASK_32;
    int shl32(x, y) => (x << y) & MASK_32;

    var cached = _hashCache?[key];
    if (cached != null) {
      return cached;
    }

    int hash = 0;
    for (int i = 0; i < key.length; i++) {
      hash = add32(hash, key[i]);
      hash = add32(hash, shl32(hash, 10));
      hash ^= hash >> 6;
    }

    hash = add32(hash, shl32(hash, 3));
    hash ^= hash >> 11;
    hash = add32(hash, shl32(hash, 15));
    _hashCache?[key] = hash;
    return hash;
  }

  bool _keyCompare(List<int> key1, List<int> key2) {
    if (key1.length != key2.length) return false;
    for (int i = 0; i < key1.length; i++) {
      if (key1[i] != key2[i]) return false;
    }
    return true;
  }

  List<int> _createKey(dynamic key) {
    if (key is String) {
      final encodedKey = utf8.encode(key);
      return encodedKey;
    } else {
      if (key is! List<int>) {
        throw ArgumentError('Key must have type String or List<int>');
      }
    }
    return key;
  }

  List<int> _createValue(dynamic value) {
    if (value is String) {
      final encodedValue = utf8.encode(value);
      return encodedValue;
    } else {
      if (value is! List<int>) {
        throw ArgumentError('Value must have type String or List<int>');
      }
    }
    return value;
  }

  void _checkExpiration(Duration? expiration) {
    const int secondsInThirtyDays = 60 * 60 * 24 * 30;
    // Expiration cannot exceed 30 days.
    if (expiration != null && expiration.inSeconds > secondsInThirtyDays) {
      throw ArgumentError('Expiration cannot exceed 30 days');
    }
  }

  raw.GetOperation _createGetOperation(List<int> key) {
    return raw.GetOperation(key);
  }

  raw.SetOperation _createSetOperation(
      dynamic key, dynamic value, SetAction action, Duration? expiration) {
    var operation;
    var cas;
    switch (action) {
      case SetAction.SET:
        operation = raw.SetOperation.SET;
        if (_withCas) cas = _findCas(key as List<int>);
        break;
      case SetAction.ADD:
        operation = raw.SetOperation.ADD;
        break;
      case SetAction.REPLACE:
        operation = raw.SetOperation.REPLACE;
        break;
      default:
        throw ArgumentError('Unsupported set action $action');
    }
    var exp = expiration != null ? expiration.inSeconds : 0;
    return raw.SetOperation(
        operation, key as List<int>, 0, cas, _createValue(value), exp);
  }

  raw.RemoveOperation _createRemoveOperation(dynamic key) {
    return raw.RemoveOperation(_createKey(key));
  }

  raw.IncrementOperation _createIncrementOperation(
      dynamic key, int direction, int delta, int initialValue) {
    if (delta is! int) {
      throw ArgumentError('Delta value must have type int');
    }
    return raw.IncrementOperation(
        _createKey(key), delta, direction, 0, initialValue);
  }

  void _addCas(List<int> key, int cas) {
    _cas?[key] = cas;
  }

  int? _findCas(List<int> key) {
    return _cas?[key];
  }

  Future get(dynamic key, {bool asBinary = false}) {
    return Future.sync(() => _raw.get([_createGetOperation(_createKey(key))]))
        .then((List<raw.GetResult> response) {
      if (response.length != 1) {
        throw const MemcacheError.internalError();
      }
      var result = response.first;
      if (_withCas) {
        _addCas(key as List<int>, result.cas!);
      }
      if (result.status == raw.Status.KEY_NOT_FOUND) return null;
      if (result.status == raw.Status.NO_ERROR) {
        return asBinary ? result.value! : utf8.decode(result.value!);
      }
      throw MemcacheError(result.status, 'Error getting item');
    });
  }

  Future<Map> getAll(Iterable keys, {bool asBinary = false}) {
    return Future.sync(() {
      // Copy the keys as they might get mutated by _createKey below.
      var keysList = keys.toList();
      var binaryKeys = [];
      var request = <raw.GetOperation>[];
      for (int i = 0; i < keysList.length; i++) {
        binaryKeys[i] = _createKey(keysList[i]);
        request[i] = _createGetOperation(binaryKeys[i]);
      }
      return _raw.get(request).then((List<raw.GetResult> response) {
        if (response.length != request.length) {
          throw const MemcacheError.internalError();
        }
        var result = Map();
        for (int i = 0; i < keysList.length; i++) {
          var value;
          final responseStatus = response[i].status;
          if (responseStatus == raw.Status.KEY_NOT_FOUND) {
            value = null;
          } else if (responseStatus == raw.Status.NO_ERROR) {
            value =
                asBinary ? response[i].value : utf8.decode(response[i].value!);
            if (_withCas) {
              _addCas(binaryKeys[i], response[i].cas!);
            }
          } else {
            throw MemcacheError(responseStatus, 'Error getting item');
          }
          result[keysList[i]] = value;
        }

        return result;
      });
    });
  }

  Future set(key, value,
      {Duration? expiration, SetAction action = SetAction.SET}) {
    return Future.sync(() {
      _checkExpiration(expiration);
      key = _createKey(key);
      return _raw
          .set([_createSetOperation(key, value, action, expiration)]).then(
              (List<raw.SetResult> response) {
        if (response.length != 1) {
          // TODO(sgjesse): Improve error.
          throw const MemcacheError.internalError();
        }
        var result = response.first;
        if (result.status == raw.Status.NO_ERROR) return null;
        if (result.status == raw.Status.NOT_STORED) {
          throw const NotStoredError();
        }
        if (result.status == raw.Status.KEY_EXISTS) {
          throw const ModifiedError();
        }
        throw MemcacheError(result.status, 'Error storing item');
      });
    });
  }

  Future setAll(Map keysAndValues,
      {Duration? expiration, SetAction action = SetAction.SET}) {
    return Future.sync(() {
      _checkExpiration(expiration);
      var request = <raw.SetOperation>[];
      keysAndValues.forEach((key, value) {
        key = _createKey(key);
        request.add(_createSetOperation(key, value, action, expiration));
      });
      return _raw.set(request).then((List<raw.SetResult> response) {
        if (response.length != request.length) {
          throw const MemcacheError.internalError();
        }
        response.forEach((raw.SetResult result) {
          if (result.status == raw.Status.NO_ERROR) return;
          if (result.status == raw.Status.NOT_STORED) {
            // If one element is not stored throw NotStored.
            throw const NotStoredError();
          }
          if (result.status == raw.Status.KEY_EXISTS) {
            // If one element is modified throw Modified.
            throw const ModifiedError();
          }
          // If one element has another status throw.
          throw MemcacheError(result.status, 'Error storing item');
        });
        return null;
      });
    });
  }

  Future remove(key) {
    return Future.sync(() => _raw.remove([_createRemoveOperation(key)]))
        .then((List<raw.RemoveResult> responses) {
      final result = responses[0];
      if (result.status != raw.Status.NO_ERROR &&
          result.status != raw.Status.KEY_NOT_FOUND) {
        throw MemcacheError(result.status, 'Error removing item');
      }

      // The remove is considered succesful no matter whether the key was
      // there or not.
      return null;
    });
  }

  Future removeAll(Iterable keys) {
    return Future.sync(() {
      var request = keys.map(_createRemoveOperation).toList(growable: false);
      return _raw.remove(request).then((List<raw.RemoveResult> responses) {
        if (responses.length != request.length) {
          throw const MemcacheError.internalError();
        }

        for (final result in responses) {
          if (result.status != raw.Status.NO_ERROR &&
              result.status != raw.Status.KEY_NOT_FOUND) {
            throw MemcacheError(result.status, 'Error removing item');
          }
        }

        // The remove is considered succesful no matter whether the key was
        // there or not.
        return null;
      });
    });
  }

  Future<int> increment(key, {int delta = 1, int initialValue = 0}) {
    var direction = delta >= 0
        ? raw.IncrementOperation.INCREMENT
        : raw.IncrementOperation.DECREMENT;
    return Future.sync(() => _raw.increment([
          _createIncrementOperation(key, direction, delta.abs(), initialValue)
        ])).then((List<raw.IncrementResult> responses) {
      if (responses.length != 1) {
        // TODO(sgjesse): Improve error.
        throw const MemcacheError.internalError();
      }
      var response = responses[0];
      if (response.status != raw.Status.NO_ERROR) {
        throw MemcacheError(response.status, response.message);
      }
      return response.value;
    });
  }

  Future<int> decrement(key, {int delta = 1, int initialValue = 0}) {
    return increment(key, delta: -delta, initialValue: initialValue);
  }

  Future clear() {
    return Future.sync(() => _raw.clear());
  }

  Memcache withCAS() {
    return MemCacheImpl(_raw, withCas: true);
  }
}
