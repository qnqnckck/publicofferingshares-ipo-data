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
    required this.discoveredPath,
    required this.outDir,
    required this.backfillYears,
    required this.interval,
    required this.discover,
    required this.dartApiKeyEnv,
    required this.itickApiKeyEnv,
    required this.watch,
    required this.help,
  });

  final String seedPath;
  final String liveDir;
  final String discoveredPath;
  final String outDir;
  final int backfillYears;
  final Duration interval;
  final bool discover;
  final String dartApiKeyEnv;
  final String itickApiKeyEnv;
  final bool watch;
  final bool help;

  static const usage = '''
Usage:
  dart run tool/ipo_competition_batch.dart [options]

Options:
  --seed <path>               Seed JSON path. Default: data/ipo_competition_seed.json
  --live-dir <dir>            Directory with live snapshot JSON files. Default: data/live_snapshots
  --discovered <path>         Auto-discovered stock JSON path. Default: data/discovered/ipo_events.json
  --out <dir>                 Output directory. Default: ipo_competition_data
  --backfill-years <years>    Include IPOs from the last N years. Default: 3
  --interval-minutes <min>    Watch interval. Default: 10
  --dart-api-key-env <name>   Environment variable for DART API key. Default: DART_API_KEY
  --itick-api-key-env <name>  Environment variable for iTick API key. Default: ITICK_API_KEY
  --no-discover               Skip remote discovery and only normalize local input files.
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
      discoveredPath: valueAfter('--discovered', 'data/discovered/ipo_events.json'),
      outDir: valueAfter('--out', 'ipo_competition_data'),
      backfillYears: intAfter('--backfill-years', 3),
      interval: Duration(minutes: intAfter('--interval-minutes', 10)),
      discover: !args.contains('--no-discover'),
      dartApiKeyEnv: valueAfter('--dart-api-key-env', 'DART_API_KEY'),
      itickApiKeyEnv: valueAfter('--itick-api-key-env', 'ITICK_API_KEY'),
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
      final discoveredStocks = options.discover
          ? mergeStocks([
              ...await _loadDiscoveredStocks(),
              ...await _discoverRemoteStocks(generatedAt),
            ])
          : await _loadDiscoveredStocks();
      await _writeDiscoveredStocks(discoveredStocks);

      final stocks = mergeStocks([
        ...await _loadSeedStocks(),
        ...discoveredStocks,
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

  Future<List<IpoCompetitionStock>> _loadDiscoveredStocks() async {
    final file = File(options.discoveredPath);
    if (!await file.exists()) {
      return const [];
    }
    final decoded = jsonDecode(await file.readAsString());
    if (decoded is! Map<String, Object?> || decoded['stocks'] is! List) {
      return const [];
    }
    return (decoded['stocks'] as List)
        .whereType<Map<String, Object?>>()
        .map(IpoCompetitionStock.fromJson)
        .toList();
  }

  Future<void> _writeDiscoveredStocks(List<IpoCompetitionStock> stocks) async {
    final file = File(options.discoveredPath);
    await file.parent.create(recursive: true);
    await file.writeAsString(
      prettyJson({
        'schemaVersion': schemaVersion,
        'generatedAt': DateTime.now().toIso8601String(),
        'stocks': stocks
            .map((stock) => stock.normalized().toJson())
            .toList()
          ..sort((a, b) {
            final aDate = '${a['subscriptionStart'] ?? ''}';
            final bDate = '${b['subscriptionStart'] ?? ''}';
            final byDate = bDate.compareTo(aDate);
            if (byDate != 0) {
              return byDate;
            }
            return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
          }),
      }),
    );
  }

  Future<List<IpoCompetitionStock>> _discoverRemoteStocks(DateTime now) async {
    final discovered = <IpoCompetitionStock>[];
    discovered.addAll(await _discoverDartStocks(now));
    discovered.addAll(await _discoverItickStocks());
    return discovered;
  }

  Future<List<IpoCompetitionStock>> _discoverDartStocks(DateTime now) async {
    final apiKey = Platform.environment[options.dartApiKeyEnv]?.trim() ?? '';
    if (apiKey.isEmpty) {
      return const [];
    }
    final start = compactDate(DateTime(now.year, now.month - 2, now.day));
    final end = compactDate(DateTime(now.year, now.month + 6, now.day));
    final uri = Uri.parse(
      'https://opendart.fss.or.kr/api/isuPblmnDd.json?auth=$apiKey&bgnde=$start&endde=$end',
    );
    try {
      final response = await httpGetJson(uri);
      final rows = response['list'];
      if (rows is! List) {
        return const [];
      }
      return rows
          .whereType<Map<String, Object?>>()
          .map(stockFromDartRow)
          .whereType<IpoCompetitionStock>()
          .toList();
    } catch (error) {
      stderr.writeln('DART discovery failed: $error');
      return const [];
    }
  }

  Future<List<IpoCompetitionStock>> _discoverItickStocks() async {
    final apiKey = Platform.environment[options.itickApiKeyEnv]?.trim() ?? '';
    if (apiKey.isEmpty) {
      return const [];
    }
    final uri = Uri.parse(
      'https://api.itick.org/stock/ipo?region=Korea&type=upcoming&apikey=$apiKey',
    );
    try {
      final response = await httpGetJson(uri);
      final rows = response['data'] ?? response['list'] ?? response['items'];
      if (rows is! List) {
        return const [];
      }
      return rows
          .whereType<Map<String, Object?>>()
          .map(stockFromItickRow)
          .whereType<IpoCompetitionStock>()
          .toList();
    } catch (error) {
      stderr.writeln('iTick discovery failed: $error');
      return const [];
    }
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
    required this.sourceUrl,
    required this.aggregateCompetitionRate,
    required this.brokers,
  });

  final String capturedAt;
  final String source;
  final String? sourceUrl;
  final double? aggregateCompetitionRate;
  final List<IpoBrokerCompetition> brokers;

  factory IpoCompetitionSnapshot.fromJson(Map<String, Object?> json) {
    final aggregate = json['aggregate'];
    return IpoCompetitionSnapshot(
      capturedAt: readRequiredString(json, 'capturedAt'),
      source: readString(json, 'source') ?? 'manual',
      sourceUrl: readString(json, 'sourceUrl'),
      aggregateCompetitionRate: readDouble(json['aggregateCompetitionRate']) ??
          (aggregate is Map<String, Object?>
              ? readDouble(aggregate['competitionRate'])
              : null),
      brokers: readObjectList(json['brokers'])
          .map(IpoBrokerCompetition.fromJson)
          .toList(),
    );
  }

  IpoCompetitionSnapshot normalized() {
    return IpoCompetitionSnapshot(
      capturedAt: capturedAt,
      source: source.trim().isEmpty ? 'manual' : source.trim(),
      sourceUrl: sourceUrl,
      aggregateCompetitionRate: aggregateCompetitionRate,
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
      competitionRate: aggregateCompetitionRate ??
          (offeredShares <= 0 ? null : subscribedShares / offeredShares),
    );
  }

  Map<String, Object?> toJson() {
    return {
      'capturedAt': capturedAt,
      'source': source,
      'sourceUrl': sourceUrl,
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

Future<Map<String, Object?>> httpGetJson(Uri uri) async {
  final client = HttpClient();
  try {
    final request = await client.getUrl(uri);
    request.headers.set(HttpHeaders.acceptHeader, 'application/json');
    final response = await request.close();
    final body = await utf8.decodeStream(response);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw HttpException('HTTP ${response.statusCode}: $body', uri: uri);
    }
    final decoded = jsonDecode(body);
    if (decoded is! Map<String, Object?>) {
      throw const FormatException('Response root must be a JSON object.');
    }
    return decoded;
  } finally {
    client.close(force: true);
  }
}

IpoCompetitionStock? stockFromDartRow(Map<String, Object?> row) {
  final company = firstNonEmptyString(row, [
    'corp_name',
    'corpNm',
    'corp_name_eng',
    'stock_name',
  ]);
  if (company == null) {
    return null;
  }
  final subscriptionStart = normalizeDate(
    firstNonEmptyString(row, ['sbd', 'subscrpt_bgnde', 'subscriptionStart']),
  );
  final subscriptionEnd = normalizeDate(
    firstNonEmptyString(row, ['pymd', 'subscrpt_endde', 'subscriptionEnd']) ??
        subscriptionStart,
  );
  return IpoCompetitionStock(
    id: safeId('${company}_${subscriptionStart ?? ''}'),
    company: company,
    market: '',
    subscriptionStart: subscriptionStart,
    subscriptionEnd: subscriptionEnd,
    leadManagers: readLeadManagers(
      firstNonEmptyString(row, ['lead_mgr', 'rprsntv_mngr', 'underwriter']),
    ),
    snapshots: const [],
  );
}

IpoCompetitionStock? stockFromItickRow(Map<String, Object?> row) {
  final company = firstNonEmptyString(row, [
    'company',
    'name',
    'symbolName',
    'stockName',
  ]);
  if (company == null) {
    return null;
  }
  final subscriptionStart = normalizeDate(
    firstNonEmptyString(row, [
      'subscriptionStart',
      'subscription_start',
      'startDate',
      'ipoDate',
    ]),
  );
  final subscriptionEnd = normalizeDate(
    firstNonEmptyString(row, [
          'subscriptionEnd',
          'subscription_end',
          'endDate',
        ]) ??
        subscriptionStart,
  );
  return IpoCompetitionStock(
    id: safeId('${company}_${subscriptionStart ?? ''}'),
    company: company,
    market: firstNonEmptyString(row, ['market', 'exchange']) ?? '',
    subscriptionStart: subscriptionStart,
    subscriptionEnd: subscriptionEnd,
    leadManagers: readLeadManagers(
      firstNonEmptyString(row, ['leadManager', 'lead_manager', 'underwriter']),
    ),
    snapshots: const [],
  );
}

String compactDate(DateTime value) {
  final year = value.year.toString().padLeft(4, '0');
  final month = value.month.toString().padLeft(2, '0');
  final day = value.day.toString().padLeft(2, '0');
  return '$year$month$day';
}

String? normalizeDate(String? value) {
  if (value == null) {
    return null;
  }
  final digits = value.replaceAll(RegExp(r'[^0-9]'), '');
  if (digits.length >= 8) {
    return '${digits.substring(0, 4)}-${digits.substring(4, 6)}-${digits.substring(6, 8)}';
  }
  return null;
}

String? firstNonEmptyString(Map<String, Object?> row, List<String> keys) {
  for (final key in keys) {
    final value = row[key];
    if (value == null) {
      continue;
    }
    final text = '$value'.trim();
    if (text.isNotEmpty && text.toLowerCase() != 'null') {
      return text;
    }
  }
  return null;
}

List<String> readLeadManagers(String? value) {
  if (value == null || value.trim().isEmpty) {
    return const [];
  }
  return value
      .split(RegExp(r'[,/·、]|및|,|;'))
      .map((item) => item.trim())
      .where((item) => item.isNotEmpty)
      .toList();
}
