import 'dart:async';

import 'package:dart_libp2p/config/config.dart';
import 'package:dart_libp2p/core/crypto/ed25519.dart';
import 'package:dart_libp2p/core/network/context.dart';
import 'package:dart_libp2p/core/network/rcmgr.dart';
import 'package:dart_libp2p/core/peer/peer_id.dart';
import 'package:dart_libp2p/p2p/host/peerstore/pstoremem/peerstore.dart';
import 'package:dart_libp2p/p2p/network/swarm/dial_singleflight.dart';
import 'package:dart_libp2p/p2p/network/swarm/swarm.dart';
import 'package:dart_libp2p/p2p/transport/basic_upgrader.dart';
import 'package:test/test.dart';

void main() {
  group('DialSingleflight', () {
    test('coalesces concurrent operations with the same dial intent', () async {
      final singleflight = DialSingleflight<String, int>();
      final release = Completer<void>();
      var calls = 0;

      Future<int> dial() async {
        calls++;
        await release.future;
        return 42;
      }

      final first = singleflight.run('peer|normal', dial);
      final second = singleflight.run('peer|normal', dial);

      expect(calls, 1);
      release.complete();
      expect(await Future.wait([first, second]), [42, 42]);
      expect(calls, 1);
    });

    test('does not merge normal and forced-direct dial intents', () async {
      final singleflight = DialSingleflight<String, String>();
      final normal = Completer<String>();
      final direct = Completer<String>();

      final normalResult = singleflight.run('peer|normal', () => normal.future);
      final directResult = singleflight.run('peer|direct', () => direct.future);

      normal.complete('relay');
      direct.complete('direct');
      expect(await normalResult, 'relay');
      expect(await directResult, 'direct');
    });

    test('removes failed operations so a later dial can retry', () async {
      final singleflight = DialSingleflight<String, int>();
      var calls = 0;

      Future<int> dial() async {
        calls++;
        if (calls == 1) throw StateError('dial failed');
        return 7;
      }

      await expectLater(
        singleflight.run('peer|normal', dial),
        throwsStateError,
      );
      expect(await singleflight.run('peer|normal', dial), 7);
      expect(calls, 2);
    });

    test('all waiters receive the same error and stack trace', () async {
      final singleflight = DialSingleflight<String, int>();
      final release = Completer<void>();
      final customError = FormatException('test format error');

      Future<int> dial() async {
        await release.future;
        throw customError;
      }

      final first = singleflight.run('peer|normal', dial);
      final second = singleflight.run('peer|normal', dial);

      release.complete();

      final res1 = await catchError(first);
      final res2 = await catchError(second);
      expect(res1, same(customError));
      expect(res2, same(customError));
    });

    test('one waiter timing out does not cancel or break other waiters',
        () async {
      final singleflight = DialSingleflight<String, int>();
      final release = Completer<void>();

      Future<int> dial() async {
        await release.future;
        return 99;
      }

      final first = singleflight.run('peer|normal', dial);
      final second = singleflight.run('peer|normal', dial);

      // Short timeout on first waiter
      expect(
        first.timeout(const Duration(milliseconds: 10)),
        throwsA(isA<TimeoutException>()),
      );

      // Complete operation later
      await Future<void>.delayed(const Duration(milliseconds: 20));
      release.complete();

      expect(await second, 99);
    });

    test('closing singleflight fails in-flight operations without hanging',
        () async {
      final singleflight = DialSingleflight<String, int>();
      final release = Completer<void>();

      final first =
          singleflight.run('peer|normal', () => release.future.then((_) => 1));
      final second =
          singleflight.run('peer|normal', () => release.future.then((_) => 1));

      expect(singleflight.isClosed, isFalse);
      singleflight.close(StateError('Swarm is closed'));
      expect(singleflight.isClosed, isTrue);

      expect(first, throwsStateError);
      expect(second, throwsStateError);

      // Subsequent run calls after close return immediate error
      expect(
        singleflight.run('peer|normal', () async => 2),
        throwsStateError,
      );
    });

    test('Swarm.dialPeer coalesces N concurrent calls for same peer and intent',
        () async {
      final keyPair = await generateEd25519KeyPair();
      final localPeerId = PeerId.fromPublicKey(keyPair.publicKey);
      final targetPeerId =
          PeerId.fromPublicKey((await generateEd25519KeyPair()).publicKey);
      final peerstore = MemoryPeerstore();
      final resourceManager = NullResourceManager();
      final upgrader = BasicUpgrader(resourceManager: resourceManager);
      final config = Config();

      final swarm = Swarm(
        host: null,
        localPeer: localPeerId,
        peerstore: peerstore,
        resourceManager: resourceManager,
        upgrader: upgrader,
        config: config,
      );

      final ctx = Context();
      final futures =
          List.generate(5, (_) => swarm.dialPeer(ctx, targetPeerId));

      final results = await Future.wait(
        futures.map((f) => f.then<Object?>((v) => v).catchError((e) => e)),
      );

      expect(results.length, 5);
      final firstError = results.first;
      expect(firstError, isA<Exception>());
      for (final res in results) {
        expect(res, same(firstError));
      }

      await swarm.close();
    });
  });
}

Future<Object?> catchError(Future<dynamic> future) async {
  try {
    await future;
    return null;
  } catch (e) {
    return e;
  }
}
