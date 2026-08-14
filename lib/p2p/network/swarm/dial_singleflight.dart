import 'dart:async';

/// Coalesces concurrent dial operations with the same key.
///
/// Entries exist only while [operation] is in flight. Successful and failed
/// operations are both removed, allowing a later caller to retry.
class DialSingleflight<K, V> {
  final Map<K, Completer<V>> _inFlight = <K, Completer<V>>{};
  bool _isClosed = false;

  /// Whether this singleflight instance has been closed.
  bool get isClosed => _isClosed;

  /// Runs [operation] or returns an existing in-flight future for [key].
  Future<V> run(K key, Future<V> Function() operation) {
    if (_isClosed) {
      return Future.error(StateError('Swarm is closed'));
    }

    final existing = _inFlight[key];
    if (existing != null) {
      return existing.future;
    }

    final completer = Completer<V>();
    _inFlight[key] = completer;

    Future<void> execute() async {
      try {
        final result = await operation();
        if (!completer.isCompleted) {
          completer.complete(result);
        }
      } catch (error, stackTrace) {
        if (!completer.isCompleted) {
          completer.completeError(error, stackTrace);
        }
      } finally {
        if (identical(_inFlight[key], completer)) {
          _inFlight.remove(key);
        }
      }
    }

    unawaited(execute());

    return completer.future;
  }

  /// Closes the singleflight instance, completing any in-flight operations
  /// with [error] so no unresolved futures linger.
  void close([Object? error, StackTrace? stackTrace]) {
    _isClosed = true;
    final entries = _inFlight.values.toList(growable: false);
    _inFlight.clear();
    final err = error ?? StateError('Swarm is closed');
    for (final completer in entries) {
      if (!completer.isCompleted) {
        completer.completeError(err, stackTrace);
      }
    }
  }
}
