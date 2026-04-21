import 'dart:async';
import 'dart:convert';
import 'dart:io';

const schemaVersion = 1;

Future<void> main(List<String> args) async {
  final options = BatchOptions.parse(args);
  if (options.help) {
    stdout.writeln(BatchOptions.usage);
    return;
  }

  final runner = IpoCompetitionBatch(options);
  if (!options.watch) {
    await runner.runOnce();
    return;
  }

  stdout.writeln(
    'Watching active IPO subscriptions every ${options.interval.inMinutes} minutes.',
  );
  await runner.runOnce();
  Timer.periodic(options.interval, (_) {
    unawaited(runner.runOnce());
  });
}

class BatchOptions {
  const BatchOptions({
    required this.seedPath,
    required this.liveDir,
    required this.outDir,
    required this.backfillYears,
    required this.interval,
    required this.watch,
    required this.help,
  });

  final String seedPath;
  final String liveDir;
  final String outDir;
  final int backfillYears;
  final Duration interval;
  final bool watch;
  final bool help;

  static const usage = '''
Usage:
  dart run tool/ipo_competition_batch.dart [options]

Options:
  --seed <path>               Seed JSON path. Default: data/ipo_competition_seed.json
  --live-dir <dir>            Directory with live snapshot JSON files. Default: data/live_snapshots
  --out <dir>                 Output directory. Default: ipo_competition_data
  --backfill-years <years>    Include IPOs from the last N years. Default: 3
  --interval-minutes <min>    Watch interval. Default: 10
  --watch                     Keep running and refresh active subscriptions.
  --help                      Show this help.

Seed from the example file:
  cp data/ipo_competition_seed.example.json data/ipo_competition_seed.json
''';

  factory BatchOptions.parse(List<String> args) {
    String valueAfter(String name, String fallback) {
      final index = args.indexOf(name);
      if (index < 0 || index + 1 >= args.length) {
        return fallback;
      }
      return args[index + 1];
    }

    int intAfter(String name, int fallback) {
      return int.tryParse(valueAfter(name, '$fallback')) ?? fallback;
    }

    return BatchOptions(
      seedPath: valueAfter('--seed', 'data/ipo_competition_seed.json'),
      liveDir: valueAfter('--live-dir', 'data/live_snapshots'),
      outDir: valueAfter('--out', 'ipo_competition_data'),
      backfillYears: intAfter('--backfill-years', 3),
      interval: Duration(minutes: intAfter('--interval-minutes', 10)),
      watch: args.contains('--watch'),
      help: args.contains('--help') || args.contains('-h'),
    );
  }
}

class IpoCompetitionBatch {
  IpoCompetitionBatch(this.options);

  final BatchOptions options;
  bool _running = false;

  Future<void> runOnce() async {
    if (_running) {
      return;
    }
    _running = true;
    try {
      final generatedAt = DateTime.now();
      final stocks = mergeStocks([
        ...await _loadSeedStocks(),
        ...await _loadLiveStocks(),
      ]);
      final cutoff = DateTime(
        generatedAt.year - options.backfillYears,
        generatedAt.month,
        generatedAt.day,
      );
      final selected = stocks.where((stock) {
        final end = parseDate(stock.subscriptionEnd);
        return end == null || !end.isBefore(cutoff);
      }).toList()
        ..sort((a, b) {
          final byDate = (b.subscriptionStart ?? '').compareTo(
            a.subscriptionStart ?? '',
          );
          if (byDate != 0) {
            return byDate;
          }
          return a.company.compareTo(b.company);
        });

      await Directory('${options.outDir}/stocks').create(recursive: true);
      final indexStocks = <Map<String, Object?>>[];

      for (final stock in selected) {
        final normalized = stock.normalized();
        final path = 'stocks/${stock.id}.json';
        await File('${options.outDir}/$path').writeAsString(
          prettyJson(normalized.toJson()),
        );
        indexStocks.add(normalized.toIndexJson(path));
      }

      final index = <String, Object?>{
        'schemaVersion': schemaVersion,
        'generatedAt': generatedAt.toIso8601String(),
        'stocks': indexStocks,
      };
      await File('${options.outDir}/index.json').writeAsString(
        prettyJson(index),
      );

      stdout.writeln(
        '[${generatedAt.toIso8601String()}] generated ${selected.length} stock files.',
      );
    } finally {
      _running = false;
    }
  }

  Future<List<IpoCompetitionStock>> _loadSeedStocks() async {
    final file = File(options.seedPath);
    if (!await file.exists()) {
      stderr.writeln(
        'Seed file not found: ${options.seedPath}. Create it from data/ipo_competition_seed.example.json.',
      );
      return const [];
    }
    final decoded = jsonDecode(await file.readAsString());
    if (decoded is! Map<String, Object?>) {
      throw const FormatException('Seed root must be a JSON object.');
    }
    final rawStocks = decoded['stocks'];
    if (rawStocks is! List) {
      throw const FormatException('Seed field "stocks" must be a list.');
    }
    return rawStocks
        .whereType<Map<String, Object?>>()
        .map(IpoCompetitionStock.fromJson)
        .toList();
  }

  Future<List<IpoCompetitionStock>> _loadLiveStocks() async {
    final dir = Directory(options.liveDir);
    if (!await dir.exists()) {
      return const [];
    }
    final stocks = <IpoCompetitionStock>[];
    await for (final entity in dir.list()) {
      if (entity is! File || !entity.path.endsWith('.json')) {
        continue;
      }
      final decoded = jsonDecode(await entity.readAsString());
      if (decoded is Map<String, Object?> && decoded['stocks'] is List) {
        stocks.addAll(
          (decoded['stocks'] as List)
              .whereType<Map<String, Object?>>()
              .map(IpoCompetitionStock.fromJson),
        );
      } else if (decoded is Map<String, Object?>) {
        stocks.add(IpoCompetitionStock.fromJson(decoded));
      }
    }
    return stocks;
  }
}

class IpoCompetitionStock {
  const IpoCompetitionStock({
    required this.id,
    required this.company,
    required this.market,
    required this.subscriptionStart,
    required this.subscriptionEnd,
    required this.leadManagers,
    required this.snapshots,
  });

  final String id;
  final String company;
  final String market;
  final String? subscriptionStart;
  final String? subscriptionEnd;
  final List<String> leadManagers;
  final List<IpoCompetitionSnapshot> snapshots;

  factory IpoCompetitionStock.fromJson(Map<String, Object?> json) {
    return IpoCompetitionStock(
      id: readRequiredString(json, 'id'),
      company: readRequiredString(json, 'company'),
      market: readString(json, 'market') ?? '',
      subscriptionStart: readString(json, 'subscriptionStart'),
      subscriptionEnd: readString(json, 'subscriptionEnd'),
      leadManagers: readStringList(json['leadManagers']),
      snapshots: readObjectList(json['snapshots'])
          .map(IpoCompetitionSnapshot.fromJson)
          .toList(),
    );
  }

  IpoCompetitionStock normalized() {
    return IpoCompetitionStock(
      id: safeId(id),
      company: company.trim(),
      market: market.trim(),
      subscriptionStart: subscriptionStart,
      subscriptionEnd: subscriptionEnd,
      leadManagers: leadManagers
          .map((item) => item.trim())
          .where((item) => item.isNotEmpty)
          .toList(),
      snapshots: snapshots.map((snapshot) => snapshot.normalized()).toList()
        ..sort((a, b) => a.capturedAt.compareTo(b.capturedAt)),
    );
  }

  IpoCompetitionSnapshot? get latestSnapshot {
    if (snapshots.isEmpty) {
      return null;
    }
    return snapshots.reduce(
      (a, b) => a.capturedAt.compareTo(b.capturedAt) >= 0 ? a : b,
    );
  }

  Map<String, Object?> toJson() {
    return {
      'schemaVersion': schemaVersion,
      'id': safeId(id),
      'company': company,
      'market': market,
      'subscriptionStart': subscriptionStart,
      'subscriptionEnd': subscriptionEnd,
      'leadManagers': leadManagers,
      'snapshots': snapshots.map((snapshot) => snapshot.toJson()).toList(),
    };
  }

  Map<String, Object?> toIndexJson(String path) {
    final latest = latestSnapshot;
    return {
      'id': safeId(id),
      'company': company,
      'market': market,
      'subscriptionStart': subscriptionStart,
      'subscriptionEnd': subscriptionEnd,
      'latestCompetitionRate': latest?.aggregate.competitionRate,
      'latestSnapshotAt': latest?.capturedAt,
      'path': path,
    };
  }
}

List<IpoCompetitionStock> mergeStocks(List<IpoCompetitionStock> stocks) {
  final byId = <String, IpoCompetitionStock>{};
  for (final stock in stocks) {
    final id = safeId(stock.id);
    final existing = byId[id];
    if (existing == null) {
      byId[id] = stock;
      continue;
    }
    byId[id] = IpoCompetitionStock(
      id: id,
      company: stock.company.trim().isEmpty ? existing.company : stock.company,
      market: stock.market.trim().isEmpty ? existing.market : stock.market,
      subscriptionStart: stock.subscriptionStart ?? existing.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd ?? existing.subscriptionEnd,
      leadManagers: {...existing.leadManagers, ...stock.leadManagers}.toList(),
      snapshots: [...existing.snapshots, ...stock.snapshots],
    );
  }
  return byId.values.toList();
}

class IpoCompetitionSnapshot {
  const IpoCompetitionSnapshot({
    required this.capturedAt,
    required this.source,
    required this.brokers,
  });

  final String capturedAt;
  final String source;
  final List<IpoBrokerCompetition> brokers;

  factory IpoCompetitionSnapshot.fromJson(Map<String, Object?> json) {
    return IpoCompetitionSnapshot(
      capturedAt: readRequiredString(json, 'capturedAt'),
      source: readString(json, 'source') ?? 'manual',
      brokers: readObjectList(json['brokers'])
          .map(IpoBrokerCompetition.fromJson)
          .toList(),
    );
  }

  IpoCompetitionSnapshot normalized() {
    return IpoCompetitionSnapshot(
      capturedAt: capturedAt,
      source: source.trim().isEmpty ? 'manual' : source.trim(),
      brokers: brokers.map((broker) => broker.normalized()).toList()
        ..sort((a, b) => a.name.compareTo(b.name)),
    );
  }

  IpoBrokerCompetitionAggregate get aggregate {
    final offeredShares = brokers.fold<int>(
      0,
      (sum, broker) => sum + broker.offeredShares,
    );
    final subscribedShares = brokers.fold<int>(
      0,
      (sum, broker) => sum + broker.subscribedShares,
    );
    return IpoBrokerCompetitionAggregate(
      offeredShares: offeredShares,
      subscribedShares: subscribedShares,
      competitionRate: offeredShares <= 0 ? null : subscribedShares / offeredShares,
    );
  }

  Map<String, Object?> toJson() {
    return {
      'capturedAt': capturedAt,
      'source': source,
      'brokers': brokers.map((broker) => broker.toJson()).toList(),
      'aggregate': aggregate.toJson(),
    };
  }
}

class IpoBrokerCompetition {
  const IpoBrokerCompetition({
    required this.name,
    required this.offeredShares,
    required this.subscribedShares,
    required this.competitionRate,
    required this.equalCompetitionRate,
    required this.proportionalCompetitionRate,
  });

  final String name;
  final int offeredShares;
  final int subscribedShares;
  final double? competitionRate;
  final double? equalCompetitionRate;
  final double? proportionalCompetitionRate;

  factory IpoBrokerCompetition.fromJson(Map<String, Object?> json) {
    final offeredShares = readInt(json['offeredShares']);
    final subscribedShares = readInt(json['subscribedShares']);
    return IpoBrokerCompetition(
      name: readRequiredString(json, 'name'),
      offeredShares: offeredShares,
      subscribedShares: subscribedShares,
      competitionRate: readDouble(json['competitionRate']) ??
          (offeredShares <= 0 ? null : subscribedShares / offeredShares),
      equalCompetitionRate: readDouble(json['equalCompetitionRate']),
      proportionalCompetitionRate: readDouble(json['proportionalCompetitionRate']),
    );
  }

  IpoBrokerCompetition normalized() {
    return IpoBrokerCompetition(
      name: name.trim(),
      offeredShares: offeredShares,
      subscribedShares: subscribedShares,
      competitionRate: competitionRate,
      equalCompetitionRate: equalCompetitionRate,
      proportionalCompetitionRate: proportionalCompetitionRate,
    );
  }

  Map<String, Object?> toJson() {
    return {
      'name': name,
      'offeredShares': offeredShares,
      'subscribedShares': subscribedShares,
      'competitionRate': competitionRate,
      'equalCompetitionRate': equalCompetitionRate,
      'proportionalCompetitionRate': proportionalCompetitionRate,
    };
  }
}

class IpoBrokerCompetitionAggregate {
  const IpoBrokerCompetitionAggregate({
    required this.offeredShares,
    required this.subscribedShares,
    required this.competitionRate,
  });

  final int offeredShares;
  final int subscribedShares;
  final double? competitionRate;

  Map<String, Object?> toJson() {
    return {
      'offeredShares': offeredShares,
      'subscribedShares': subscribedShares,
      'competitionRate': competitionRate,
    };
  }
}

String prettyJson(Object? value) {
  return '${const JsonEncoder.withIndent('  ').convert(value)}\n';
}

String safeId(String value) {
  return value
      .trim()
      .toLowerCase()
      .replaceAll(RegExp(r'[^a-z0-9가-힣_-]+'), '_')
      .replaceAll(RegExp(r'_+'), '_')
      .replaceAll(RegExp(r'^_|_$'), '');
}

DateTime? parseDate(String? value) {
  if (value == null || value.trim().isEmpty) {
    return null;
  }
  return DateTime.tryParse(value);
}

String readRequiredString(Map<String, Object?> json, String key) {
  final value = readString(json, key);
  if (value == null || value.trim().isEmpty) {
    throw FormatException('Missing required string field: $key');
  }
  return value;
}

String? readString(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value == null) {
    return null;
  }
  return '$value';
}

List<String> readStringList(Object? value) {
  if (value is! List) {
    return const [];
  }
  return value.map((item) => '$item').toList();
}

List<Map<String, Object?>> readObjectList(Object? value) {
  if (value is! List) {
    return const [];
  }
  return value.whereType<Map<String, Object?>>().toList();
}

int readInt(Object? value) {
  if (value is int) {
    return value;
  }
  if (value is double) {
    return value.round();
  }
  return int.tryParse('$value'.replaceAll(',', '').trim()) ?? 0;
}

double? readDouble(Object? value) {
  if (value == null) {
    return null;
  }
  if (value is num) {
    return value.toDouble();
  }
  return double.tryParse('$value'.replaceAll(',', '').trim());
}

void unawaited(Future<void> future) {}
