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
    required this.outcomeDir,
    required this.brokerSnapshotDir,
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
  final String outcomeDir;
  final String brokerSnapshotDir;
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
  --outcome-dir <dir>         Directory with historical outcome JSON files. Default: data/outcomes
  --broker-snapshot-dir <dir> Directory with broker-level snapshot JSON files. Default: data/broker_snapshots
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
      outcomeDir: valueAfter('--outcome-dir', 'data/outcomes'),
      brokerSnapshotDir: valueAfter('--broker-snapshot-dir', 'data/broker_snapshots'),
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

      final stocksWithoutExternalOutcomes = mergeStocks([
        ...await _loadSeedStocks(),
        ...discoveredStocks,
        ...await _loadLiveStocks(),
      ]);
      final stocks = mergeOutcomes(
        stocksWithoutExternalOutcomes,
        await _loadOutcomeRows(),
      );
      final enrichedStocks = mergeBrokerSnapshots(
        stocks,
        await _loadBrokerSnapshotRows(),
      );
      final cutoff = DateTime(
        generatedAt.year - options.backfillYears,
        generatedAt.month,
        generatedAt.day,
      );
      final selected = enrichedStocks.where((stock) {
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
      await File('${options.outDir}/backtest_report.json').writeAsString(
        prettyJson(buildBacktestReport(selected, generatedAt)),
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

  Future<List<IpoOutcomeRow>> _loadOutcomeRows() async {
    final dir = Directory(options.outcomeDir);
    if (!await dir.exists()) {
      return const [];
    }
    final rows = <IpoOutcomeRow>[];
    await for (final entity in dir.list()) {
      if (entity is! File || !entity.path.endsWith('.json')) {
        continue;
      }
      final decoded = jsonDecode(await entity.readAsString());
      if (decoded is Map<String, Object?> && decoded['outcomes'] is List) {
        rows.addAll(
          (decoded['outcomes'] as List)
              .whereType<Map<String, Object?>>()
              .map(IpoOutcomeRow.fromJson),
        );
      } else if (decoded is Map<String, Object?>) {
        rows.add(IpoOutcomeRow.fromJson(decoded));
      }
    }
    return rows;
  }

  Future<List<IpoBrokerSnapshotRow>> _loadBrokerSnapshotRows() async {
    final dir = Directory(options.brokerSnapshotDir);
    if (!await dir.exists()) {
      return const [];
    }
    final rows = <IpoBrokerSnapshotRow>[];
    await for (final entity in dir.list()) {
      if (entity is! File || !entity.path.endsWith('.json')) {
        continue;
      }
      final decoded = jsonDecode(await entity.readAsString());
      if (decoded is Map<String, Object?> && decoded['snapshots'] is List) {
        rows.addAll(
          (decoded['snapshots'] as List)
              .whereType<Map<String, Object?>>()
              .map(IpoBrokerSnapshotRow.fromJson),
        );
      } else if (decoded is Map<String, Object?>) {
        rows.add(IpoBrokerSnapshotRow.fromJson(decoded));
      }
    }
    return rows;
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
    required this.fundamentals,
    required this.outcome,
    required this.snapshots,
  });

  final String id;
  final String company;
  final String market;
  final String? subscriptionStart;
  final String? subscriptionEnd;
  final List<String> leadManagers;
  final IpoFundamentals fundamentals;
  final IpoOutcome? outcome;
  final List<IpoCompetitionSnapshot> snapshots;

  factory IpoCompetitionStock.fromJson(Map<String, Object?> json) {
    return IpoCompetitionStock(
      id: readRequiredString(json, 'id'),
      company: readRequiredString(json, 'company'),
      market: readString(json, 'market') ?? '',
      subscriptionStart: readString(json, 'subscriptionStart'),
      subscriptionEnd: readString(json, 'subscriptionEnd'),
      leadManagers: readStringList(json['leadManagers']),
      fundamentals: IpoFundamentals.fromJson(
        json['fundamentals'] is Map<String, Object?>
            ? json['fundamentals'] as Map<String, Object?>
            : const {},
      ),
      outcome: json['outcome'] is Map<String, Object?>
          ? IpoOutcome.fromJson(json['outcome'] as Map<String, Object?>)
          : null,
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
      fundamentals: fundamentals.normalized(),
      outcome: outcome?.normalized(),
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
    final analysis = analyzeStock(this);
    return {
      'schemaVersion': schemaVersion,
      'id': safeId(id),
      'company': company,
      'market': market,
      'subscriptionStart': subscriptionStart,
      'subscriptionEnd': subscriptionEnd,
      'leadManagers': leadManagers,
      'fundamentals': fundamentals.toJson(),
      'outcome': outcome?.toJson(),
      'snapshots': snapshots.map((snapshot) => snapshot.toJson()).toList(),
      'analysis': analysis.toJson(),
    };
  }

  Map<String, Object?> toIndexJson(String path) {
    final latest = latestSnapshot;
    final analysis = analyzeStock(this);
    return {
      'id': safeId(id),
      'company': company,
      'market': market,
      'subscriptionStart': subscriptionStart,
      'subscriptionEnd': subscriptionEnd,
      'latestCompetitionRate': latest?.aggregate.competitionRate,
      'latestSnapshotAt': latest?.capturedAt,
      'score': analysis.score.overall,
      'grade': analysis.score.grade,
      'decisionLevel': analysis.decision.level,
      'expectedGainRate': analysis.expectedReturn.expectedListingGainRate,
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
      fundamentals: existing.fundamentals.merge(stock.fundamentals),
      outcome: stock.outcome ?? existing.outcome,
      snapshots: [...existing.snapshots, ...stock.snapshots],
    );
  }
  return byId.values.toList();
}

List<IpoCompetitionStock> mergeOutcomes(
  List<IpoCompetitionStock> stocks,
  List<IpoOutcomeRow> outcomes,
) {
  if (outcomes.isEmpty) {
    return stocks;
  }
  final byId = <String, IpoOutcomeRow>{
    for (final outcome in outcomes)
      if (outcome.id != null) safeId(outcome.id!): outcome,
  };
  final byCompany = <String, IpoOutcomeRow>{
    for (final outcome in outcomes)
      if (outcome.company != null) normalizeLookup(outcome.company!): outcome,
  };
  return stocks.map((stock) {
    final outcomeRow =
        byId[safeId(stock.id)] ?? byCompany[normalizeLookup(stock.company)];
    if (outcomeRow == null) {
      return stock;
    }
    return IpoCompetitionStock(
      id: stock.id,
      company: stock.company,
      market: stock.market,
      subscriptionStart: stock.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd,
      leadManagers: stock.leadManagers,
      fundamentals: stock.fundamentals.merge(
        IpoFundamentals(
          offerPrice: outcomeRow.offerPrice,
          priceBandMin: null,
          priceBandMax: null,
          institutionCompetitionRate: null,
          institutionParticipants: null,
          lockupCommitmentRate: null,
          floatRate: null,
          marketCapKrw: null,
          publicAllocationShares: null,
        ),
      ),
      outcome: outcomeRow.toOutcome(),
      snapshots: stock.snapshots,
    );
  }).toList();
}

List<IpoCompetitionStock> mergeBrokerSnapshots(
  List<IpoCompetitionStock> stocks,
  List<IpoBrokerSnapshotRow> rows,
) {
  if (rows.isEmpty) {
    return stocks;
  }
  final byId = <String, List<IpoBrokerSnapshotRow>>{};
  final byCompany = <String, List<IpoBrokerSnapshotRow>>{};
  for (final row in rows) {
    if (row.id != null) {
      byId.putIfAbsent(safeId(row.id!), () => []).add(row);
    }
    if (row.company != null) {
      byCompany.putIfAbsent(normalizeLookup(row.company!), () => []).add(row);
    }
  }
  return stocks.map((stock) {
    final matches = [
      ...?byId[safeId(stock.id)],
      ...?byCompany[normalizeLookup(stock.company)],
    ];
    if (matches.isEmpty) {
      return stock;
    }
    final extraSnapshots = matches.map((row) => row.toSnapshot()).toList();
    return IpoCompetitionStock(
      id: stock.id,
      company: stock.company,
      market: stock.market,
      subscriptionStart: stock.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd,
      leadManagers: stock.leadManagers,
      fundamentals: stock.fundamentals,
      outcome: stock.outcome,
      snapshots: [...stock.snapshots, ...extraSnapshots],
    );
  }).toList();
}

class IpoBrokerSnapshotRow {
  const IpoBrokerSnapshotRow({
    required this.id,
    required this.company,
    required this.capturedAt,
    required this.source,
    required this.sourceUrl,
    required this.brokers,
  });

  final String? id;
  final String? company;
  final String capturedAt;
  final String source;
  final String? sourceUrl;
  final List<IpoBrokerCompetition> brokers;

  factory IpoBrokerSnapshotRow.fromJson(Map<String, Object?> json) {
    return IpoBrokerSnapshotRow(
      id: readString(json, 'id'),
      company: readString(json, 'company'),
      capturedAt:
          readString(json, 'capturedAt') ?? DateTime.now().toIso8601String(),
      source: readString(json, 'source') ?? 'broker_snapshot',
      sourceUrl: readString(json, 'sourceUrl'),
      brokers: readObjectList(json['brokers'])
          .map(IpoBrokerCompetition.fromJson)
          .toList(),
    );
  }

  IpoCompetitionSnapshot toSnapshot() {
    return IpoCompetitionSnapshot(
      capturedAt: capturedAt,
      source: source,
      sourceUrl: sourceUrl,
      aggregateCompetitionRate: null,
      brokers: brokers,
    );
  }
}

String normalizeLookup(String value) {
  return value.replaceAll(RegExp(r'\s+'), '').toLowerCase();
}

class IpoOutcomeRow {
  const IpoOutcomeRow({
    required this.id,
    required this.company,
    required this.listingDate,
    required this.offerPrice,
    required this.openPrice,
    required this.highPrice,
    required this.closePrice,
    required this.sourceUrl,
  });

  final String? id;
  final String? company;
  final String? listingDate;
  final int? offerPrice;
  final int? openPrice;
  final int? highPrice;
  final int? closePrice;
  final String? sourceUrl;

  factory IpoOutcomeRow.fromJson(Map<String, Object?> json) {
    return IpoOutcomeRow(
      id: readString(json, 'id'),
      company: readString(json, 'company'),
      listingDate: readString(json, 'listingDate'),
      offerPrice: readOptionalInt(json['offerPrice']),
      openPrice: readOptionalInt(json['openPrice']),
      highPrice: readOptionalInt(json['highPrice']),
      closePrice: readOptionalInt(json['closePrice']),
      sourceUrl: readString(json, 'sourceUrl'),
    );
  }

  IpoOutcome toOutcome() {
    return IpoOutcome(
      listingDate: normalizeDate(listingDate) ?? listingDate,
      openReturnRate: returnRate(openPrice),
      highReturnRate: returnRate(highPrice),
      closeReturnRate: returnRate(closePrice),
      sourceUrl: sourceUrl,
    );
  }

  double? returnRate(int? price) {
    final offer = offerPrice;
    if (offer == null || offer <= 0 || price == null || price <= 0) {
      return null;
    }
    return (price - offer) / offer;
  }
}

class IpoFundamentals {
  const IpoFundamentals({
    required this.offerPrice,
    required this.priceBandMin,
    required this.priceBandMax,
    required this.institutionCompetitionRate,
    required this.institutionParticipants,
    required this.lockupCommitmentRate,
    required this.floatRate,
    required this.marketCapKrw,
    required this.publicAllocationShares,
  });

  final int? offerPrice;
  final int? priceBandMin;
  final int? priceBandMax;
  final double? institutionCompetitionRate;
  final int? institutionParticipants;
  final double? lockupCommitmentRate;
  final double? floatRate;
  final int? marketCapKrw;
  final int? publicAllocationShares;

  factory IpoFundamentals.fromJson(Map<String, Object?> json) {
    return IpoFundamentals(
      offerPrice: readOptionalInt(json['offerPrice']),
      priceBandMin: readOptionalInt(json['priceBandMin']),
      priceBandMax: readOptionalInt(json['priceBandMax']),
      institutionCompetitionRate: readDouble(json['institutionCompetitionRate']),
      institutionParticipants: readOptionalInt(json['institutionParticipants']),
      lockupCommitmentRate: readRatio(json['lockupCommitmentRate']),
      floatRate: readRatio(json['floatRate']),
      marketCapKrw: readOptionalInt(json['marketCapKrw']),
      publicAllocationShares: readOptionalInt(json['publicAllocationShares']),
    );
  }

  IpoFundamentals normalized() {
    return this;
  }

  IpoFundamentals merge(IpoFundamentals other) {
    return IpoFundamentals(
      offerPrice: other.offerPrice ?? offerPrice,
      priceBandMin: other.priceBandMin ?? priceBandMin,
      priceBandMax: other.priceBandMax ?? priceBandMax,
      institutionCompetitionRate:
          other.institutionCompetitionRate ?? institutionCompetitionRate,
      institutionParticipants: other.institutionParticipants ?? institutionParticipants,
      lockupCommitmentRate: other.lockupCommitmentRate ?? lockupCommitmentRate,
      floatRate: other.floatRate ?? floatRate,
      marketCapKrw: other.marketCapKrw ?? marketCapKrw,
      publicAllocationShares: other.publicAllocationShares ?? publicAllocationShares,
    );
  }

  Map<String, Object?> toJson() {
    return {
      'offerPrice': offerPrice,
      'priceBandMin': priceBandMin,
      'priceBandMax': priceBandMax,
      'institutionCompetitionRate': institutionCompetitionRate,
      'institutionParticipants': institutionParticipants,
      'lockupCommitmentRate': lockupCommitmentRate,
      'floatRate': floatRate,
      'marketCapKrw': marketCapKrw,
      'publicAllocationShares': publicAllocationShares,
    };
  }
}

class IpoOutcome {
  const IpoOutcome({
    required this.listingDate,
    required this.openReturnRate,
    required this.highReturnRate,
    required this.closeReturnRate,
    required this.sourceUrl,
  });

  final String? listingDate;
  final double? openReturnRate;
  final double? highReturnRate;
  final double? closeReturnRate;
  final String? sourceUrl;

  factory IpoOutcome.fromJson(Map<String, Object?> json) {
    return IpoOutcome(
      listingDate: readString(json, 'listingDate'),
      openReturnRate: readRatio(json['openReturnRate']),
      highReturnRate: readRatio(json['highReturnRate']),
      closeReturnRate: readRatio(json['closeReturnRate']),
      sourceUrl: readString(json, 'sourceUrl'),
    );
  }

  IpoOutcome normalized() {
    return IpoOutcome(
      listingDate: normalizeDate(listingDate) ?? listingDate,
      openReturnRate: openReturnRate,
      highReturnRate: highReturnRate,
      closeReturnRate: closeReturnRate,
      sourceUrl: sourceUrl,
    );
  }

  Map<String, Object?> toJson() {
    return {
      'listingDate': listingDate,
      'openReturnRate': openReturnRate,
      'highReturnRate': highReturnRate,
      'closeReturnRate': closeReturnRate,
      'sourceUrl': sourceUrl,
    };
  }
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
    required this.offerPrice,
    required this.depositRate,
    required this.feeKrw,
    required this.competitionRate,
    required this.equalCompetitionRate,
    required this.proportionalCompetitionRate,
    this.equalAllocationShares,
    this.proportionalAllocationShares,
    this.applicationCount,
  });

  final String name;
  final int offeredShares;
  final int subscribedShares;
  final int? offerPrice;
  final double? depositRate;
  final int? feeKrw;
  final double? competitionRate;
  final double? equalCompetitionRate;
  final double? proportionalCompetitionRate;
  final int? equalAllocationShares;
  final int? proportionalAllocationShares;
  final int? applicationCount;

  factory IpoBrokerCompetition.fromJson(Map<String, Object?> json) {
    final offeredShares = readInt(json['offeredShares']);
    final subscribedShares = readInt(json['subscribedShares']);
    return IpoBrokerCompetition(
      name: readRequiredString(json, 'name'),
      offeredShares: offeredShares,
      subscribedShares: subscribedShares,
      offerPrice: readOptionalInt(json['offerPrice']),
      depositRate: readRatio(json['depositRate']),
      feeKrw: readOptionalInt(json['feeKrw']),
      competitionRate: readDouble(json['competitionRate']) ??
          (offeredShares <= 0 ? null : subscribedShares / offeredShares),
      equalCompetitionRate: readDouble(json['equalCompetitionRate']),
      proportionalCompetitionRate: readDouble(json['proportionalCompetitionRate']),
      equalAllocationShares: readOptionalInt(json['equalAllocationShares']) ??
          readOptionalInt(json['equalAllocationVolume']),
      proportionalAllocationShares:
          readOptionalInt(json['proportionalAllocationShares']) ??
              readOptionalInt(json['proportionalAllocationVolume']),
      applicationCount: readOptionalInt(json['applicationCount']),
    );
  }

  IpoBrokerCompetition normalized() {
    return IpoBrokerCompetition(
      name: name.trim(),
      offeredShares: offeredShares,
      subscribedShares: subscribedShares,
      offerPrice: offerPrice,
      depositRate: depositRate,
      feeKrw: feeKrw,
      competitionRate: competitionRate,
      equalCompetitionRate: equalCompetitionRate,
      proportionalCompetitionRate: proportionalCompetitionRate,
      equalAllocationShares: equalAllocationShares,
      proportionalAllocationShares: proportionalAllocationShares,
      applicationCount: applicationCount,
    );
  }

  Map<String, Object?> toJson() {
    return {
      'name': name,
      'offeredShares': offeredShares,
      'subscribedShares': subscribedShares,
      'offerPrice': offerPrice,
      'depositRate': depositRate,
      'feeKrw': feeKrw,
      'competitionRate': competitionRate,
      'equalCompetitionRate': equalCompetitionRate,
      'proportionalCompetitionRate': proportionalCompetitionRate,
      'equalAllocationShares': equalAllocationShares,
      'proportionalAllocationShares': proportionalAllocationShares,
      'applicationCount': applicationCount,
    };
  }

  double? get equalExpectedSharesPerAccount {
    final equalShares = equalAllocationShares;
    final accounts = applicationCount;
    if (equalShares == null || accounts == null || accounts <= 0) {
      return null;
    }
    return equalShares / accounts;
  }

  double? estimatedDepositForOneProportionalShare(int? stockOfferPrice) {
    final price = offerPrice ?? stockOfferPrice;
    final rate = proportionalCompetitionRate ?? competitionRate;
    final deposit = depositRate ?? 0.5;
    if (price == null || price <= 0 || rate == null || rate <= 0) {
      return null;
    }
    return price * deposit * rate;
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

int? readOptionalInt(Object? value) {
  if (value == null) {
    return null;
  }
  return readInt(value);
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

double? readRatio(Object? value) {
  final parsed = readDouble(value);
  if (parsed == null) {
    return null;
  }
  if (parsed > 1) {
    return parsed / 100;
  }
  return parsed;
}

void unawaited(Future<void> future) {}

IpoAnalysis analyzeStock(IpoCompetitionStock stock) {
  final latestRate = stock.latestSnapshot?.aggregate.competitionRate;
  final competitionScore = scoreCompetition(latestRate);
  final institutionScore = scoreInstitutionDemand(stock.fundamentals);
  final lockupScore = scoreLockup(stock.fundamentals.lockupCommitmentRate);
  final floatScore = scoreFloat(stock.fundamentals.floatRate);
  final pricingScore = scorePricing(stock.fundamentals);
  final marketScore = scoreMarket(stock.market);
  final managerScore = scoreLeadManagers(stock.leadManagers);
  final recencyScore = scoreRecency(stock.subscriptionEnd);
  final dataScore = scoreDataCompleteness(stock);
  final total = clampInt(
    competitionScore +
        institutionScore +
        lockupScore +
        floatScore +
        pricingScore +
        marketScore +
        managerScore +
        recencyScore +
        dataScore,
    0,
    100,
  );
  final confidence = confidenceFor(stock);
  final expectedGainRate = expectedGainRateFor(
    score: total,
    competitionRate: latestRate,
    confidence: confidence,
  );
  final offerPrice = stock.latestOfferPrice;
  final expectedAllocatedShares = expectedAllocatedSharesFor(
    offerPrice: offerPrice,
    competitionRate: latestRate,
  );
  final expectedProfit = expectedProfitFor(
    offerPrice: offerPrice,
    expectedGainRate: expectedGainRate,
    expectedAllocatedShares: expectedAllocatedShares,
  );
  final grade = gradeFor(total);
  final level = decisionLevelFor(total, confidence);

  return IpoAnalysis(
    score: IpoScore(
      overall: total,
      grade: grade,
      confidence: confidence,
      factors: {
        'competition': competitionScore,
        'institutionDemand': institutionScore,
        'lockupCommitment': lockupScore,
        'floatRate': floatScore,
        'pricing': pricingScore,
        'market': marketScore,
        'leadManagers': managerScore,
        'recency': recencyScore,
        'dataCompleteness': dataScore,
      },
    ),
    expectedReturn: IpoExpectedReturn(
      expectedListingGainRate: expectedGainRate,
      bearCaseListingGainRate: expectedGainRate - 0.22,
      baseCaseListingGainRate: expectedGainRate,
      bullCaseListingGainRate: expectedGainRate + 0.35,
      expectedAllocatedShares: expectedAllocatedShares,
      expectedProfitKrw: expectedProfit,
      assumptions: {
        'offerPrice': offerPrice,
        'competitionRate': latestRate,
        'feeKrw': 2000,
        'method': 'rule_based_v1_low_confidence',
      },
    ),
    decision: IpoDecision(
      level: level,
      label: decisionLabelFor(level),
      reasons: reasonsFor(stock, total, latestRate),
      warnings: warningsFor(stock, confidence, latestRate),
    ),
    brokerScores: brokerScoresFor(stock),
    inputs: {
      'latestCompetitionRate': latestRate,
      'snapshotCount': stock.snapshots.length,
      'leadManagerCount': stock.leadManagers.length,
      'market': stock.market,
      'hasOfferPrice': offerPrice != null,
      'institutionCompetitionRate':
          stock.fundamentals.institutionCompetitionRate,
      'lockupCommitmentRate': stock.fundamentals.lockupCommitmentRate,
      'floatRate': stock.fundamentals.floatRate,
      'hasOutcome': stock.outcome != null,
    },
    methodVersion: 'ipo-score-v1',
  );
}

class IpoAnalysis {
  const IpoAnalysis({
    required this.score,
    required this.expectedReturn,
    required this.decision,
    required this.brokerScores,
    required this.inputs,
    required this.methodVersion,
  });

  final IpoScore score;
  final IpoExpectedReturn expectedReturn;
  final IpoDecision decision;
  final List<IpoBrokerScore> brokerScores;
  final Map<String, Object?> inputs;
  final String methodVersion;

  Map<String, Object?> toJson() {
    return {
      'methodVersion': methodVersion,
      'score': score.toJson(),
      'expectedReturn': expectedReturn.toJson(),
      'decision': decision.toJson(),
      'brokerScores': brokerScores.map((score) => score.toJson()).toList(),
      'inputs': inputs,
      'disclaimer': '공개 데이터 기반 참고 지표이며 투자 권유가 아닙니다.',
    };
  }
}

class IpoBrokerScore {
  const IpoBrokerScore({
    required this.broker,
    required this.equalScore,
    required this.proportionalScore,
    required this.expectedEqualShares,
    required this.estimatedDepositForOneProportionalShare,
    required this.feeKrw,
    required this.dataQuality,
  });

  final String broker;
  final int equalScore;
  final int proportionalScore;
  final double? expectedEqualShares;
  final double? estimatedDepositForOneProportionalShare;
  final int? feeKrw;
  final String dataQuality;

  Map<String, Object?> toJson() {
    return {
      'broker': broker,
      'equalScore': equalScore,
      'proportionalScore': proportionalScore,
      'expectedEqualShares': expectedEqualShares == null
          ? null
          : roundDouble(expectedEqualShares!, 4),
      'estimatedDepositForOneProportionalShare':
          estimatedDepositForOneProportionalShare == null
              ? null
              : estimatedDepositForOneProportionalShare!.round(),
      'feeKrw': feeKrw,
      'dataQuality': dataQuality,
    };
  }
}

class IpoScore {
  const IpoScore({
    required this.overall,
    required this.grade,
    required this.confidence,
    required this.factors,
  });

  final int overall;
  final String grade;
  final double confidence;
  final Map<String, int> factors;

  Map<String, Object?> toJson() {
    return {
      'overall': overall,
      'grade': grade,
      'confidence': roundDouble(confidence, 2),
      'factors': factors,
    };
  }
}

class IpoExpectedReturn {
  const IpoExpectedReturn({
    required this.expectedListingGainRate,
    required this.bearCaseListingGainRate,
    required this.baseCaseListingGainRate,
    required this.bullCaseListingGainRate,
    required this.expectedAllocatedShares,
    required this.expectedProfitKrw,
    required this.assumptions,
  });

  final double expectedListingGainRate;
  final double bearCaseListingGainRate;
  final double baseCaseListingGainRate;
  final double bullCaseListingGainRate;
  final Map<String, double> expectedAllocatedShares;
  final Map<String, int> expectedProfitKrw;
  final Map<String, Object?> assumptions;

  Map<String, Object?> toJson() {
    return {
      'expectedListingGainRate': roundDouble(expectedListingGainRate, 4),
      'bearCaseListingGainRate': roundDouble(bearCaseListingGainRate, 4),
      'baseCaseListingGainRate': roundDouble(baseCaseListingGainRate, 4),
      'bullCaseListingGainRate': roundDouble(bullCaseListingGainRate, 4),
      'expectedAllocatedShares': expectedAllocatedShares.map(
        (key, value) => MapEntry(key, roundDouble(value, 3)),
      ),
      'expectedProfitKrw': expectedProfitKrw,
      'assumptions': assumptions,
    };
  }
}

class IpoDecision {
  const IpoDecision({
    required this.level,
    required this.label,
    required this.reasons,
    required this.warnings,
  });

  final String level;
  final String label;
  final List<String> reasons;
  final List<String> warnings;

  Map<String, Object?> toJson() {
    return {
      'level': level,
      'label': label,
      'reasons': reasons,
      'warnings': warnings,
    };
  }
}

extension IpoCompetitionStockAnalysisFields on IpoCompetitionStock {
  int? get latestOfferPrice {
    if (fundamentals.offerPrice != null && fundamentals.offerPrice! > 0) {
      return fundamentals.offerPrice;
    }
    for (final snapshot in snapshots.reversed) {
      for (final broker in snapshot.brokers) {
        if (broker.offerPrice != null && broker.offerPrice! > 0) {
          return broker.offerPrice;
        }
      }
    }
    return null;
  }
}

int scoreInstitutionDemand(IpoFundamentals fundamentals) {
  final rate = fundamentals.institutionCompetitionRate;
  if (rate == null) {
    return 5;
  }
  if (rate >= 1500) {
    return 16;
  }
  if (rate >= 1000) {
    return 14;
  }
  if (rate >= 700) {
    return 11;
  }
  if (rate >= 300) {
    return 7;
  }
  return 3;
}

int scoreLockup(double? rate) {
  if (rate == null) {
    return 4;
  }
  if (rate >= 0.5) {
    return 13;
  }
  if (rate >= 0.3) {
    return 10;
  }
  if (rate >= 0.15) {
    return 7;
  }
  if (rate >= 0.05) {
    return 4;
  }
  return 1;
}

int scoreFloat(double? rate) {
  if (rate == null) {
    return 4;
  }
  if (rate <= 0.2) {
    return 13;
  }
  if (rate <= 0.3) {
    return 10;
  }
  if (rate <= 0.4) {
    return 7;
  }
  if (rate <= 0.5) {
    return 4;
  }
  return 1;
}

int scorePricing(IpoFundamentals fundamentals) {
  final offer = fundamentals.offerPrice;
  final min = fundamentals.priceBandMin;
  final max = fundamentals.priceBandMax;
  if (offer == null || min == null || max == null || max <= min) {
    return 4;
  }
  final position = (offer - min) / (max - min);
  if (position > 1.0) {
    return 5;
  }
  if (position >= 0.85) {
    return 9;
  }
  if (position >= 0.45) {
    return 7;
  }
  return 4;
}

int scoreCompetition(double? rate) {
  if (rate == null) {
    return 8;
  }
  if (rate >= 1500) {
    return 24;
  }
  if (rate >= 800) {
    return 21;
  }
  if (rate >= 400) {
    return 18;
  }
  if (rate >= 150) {
    return 14;
  }
  if (rate >= 50) {
    return 9;
  }
  return 4;
}

int scoreMarket(String market) {
  final normalized = market.toUpperCase();
  if (normalized.contains('KOSPI')) {
    return 13;
  }
  if (normalized.contains('KOSDAQ')) {
    return 11;
  }
  return 8;
}

int scoreLeadManagers(List<String> managers) {
  if (managers.length >= 4) {
    return 13;
  }
  if (managers.length >= 2) {
    return 10;
  }
  if (managers.length == 1) {
    return 7;
  }
  return 4;
}

int scoreRecency(String? subscriptionEnd) {
  final end = parseDate(subscriptionEnd);
  if (end == null) {
    return 6;
  }
  final now = DateTime.now();
  final days = end.difference(DateTime(now.year, now.month, now.day)).inDays;
  if (days >= 0 && days <= 14) {
    return 14;
  }
  if (days > 14) {
    return 10;
  }
  if (days >= -30) {
    return 8;
  }
  return 5;
}

int scoreDataCompleteness(IpoCompetitionStock stock) {
  var score = 0;
  if (stock.snapshots.isNotEmpty) {
    score += 8;
  }
  if (stock.leadManagers.isNotEmpty) {
    score += 5;
  }
  if (stock.market.trim().isNotEmpty) {
    score += 4;
  }
  if (stock.subscriptionStart != null && stock.subscriptionEnd != null) {
    score += 4;
  }
  if (stock.fundamentals.offerPrice != null) {
    score += 3;
  }
  if (stock.fundamentals.institutionCompetitionRate != null) {
    score += 3;
  }
  if (stock.fundamentals.lockupCommitmentRate != null) {
    score += 2;
  }
  if (stock.fundamentals.floatRate != null) {
    score += 1;
  }
  return score;
}

double confidenceFor(IpoCompetitionStock stock) {
  var confidence = 0.25;
  if (stock.snapshots.isNotEmpty) {
    confidence += 0.25;
  }
  if (stock.latestSnapshot?.sourceUrl != null) {
    confidence += 0.15;
  }
  if (stock.leadManagers.isNotEmpty) {
    confidence += 0.1;
  }
  if (stock.latestOfferPrice != null) {
    confidence += 0.1;
  }
  if (stock.fundamentals.institutionCompetitionRate != null) {
    confidence += 0.1;
  }
  if (stock.fundamentals.lockupCommitmentRate != null) {
    confidence += 0.05;
  }
  if (stock.fundamentals.floatRate != null) {
    confidence += 0.05;
  }
  if (stock.latestSnapshot?.aggregate.competitionRate != null) {
    confidence += 0.05;
  }
  return clampDouble(confidence, 0.05, 0.95);
}

double expectedGainRateFor({
  required int score,
  required double? competitionRate,
  required double confidence,
}) {
  final scoreComponent = (score - 50) / 100;
  final competitionComponent = competitionRate == null
      ? 0.0
      : clampDouble((competitionRate - 300) / 2500, -0.12, 0.28);
  final raw = 0.12 + scoreComponent + competitionComponent;
  return clampDouble(raw * (0.65 + confidence * 0.35), -0.25, 1.2);
}

Map<String, double> expectedAllocatedSharesFor({
  required int? offerPrice,
  required double? competitionRate,
}) {
  final price = offerPrice ?? 30000;
  final rate = competitionRate ?? 800;
  double sharesFor(int amount) {
    final requestedShares = amount / price;
    return clampDouble(requestedShares / rate, 0, 100);
  }

  return {
    'minimumSubscription': sharesFor(price * 10),
    'oneMillionKrw': sharesFor(1000000),
    'fiveMillionKrw': sharesFor(5000000),
  };
}

Map<String, int> expectedProfitFor({
  required int? offerPrice,
  required double expectedGainRate,
  required Map<String, double> expectedAllocatedShares,
}) {
  final price = offerPrice ?? 30000;
  return expectedAllocatedShares.map((key, shares) {
    final profit = (shares * price * expectedGainRate - 2000).round();
    return MapEntry(key, profit);
  });
}

String gradeFor(int score) {
  if (score >= 90) {
    return 'A+';
  }
  if (score >= 82) {
    return 'A';
  }
  if (score >= 74) {
    return 'B+';
  }
  if (score >= 66) {
    return 'B';
  }
  if (score >= 58) {
    return 'C+';
  }
  if (score >= 50) {
    return 'C';
  }
  return 'D';
}

String decisionLevelFor(int score, double confidence) {
  if (confidence < 0.45) {
    return 'insufficient_data';
  }
  if (score >= 78) {
    return 'strong_watch';
  }
  if (score >= 65) {
    return 'consider';
  }
  if (score >= 52) {
    return 'neutral';
  }
  return 'caution';
}

String decisionLabelFor(String level) {
  switch (level) {
    case 'strong_watch':
      return '관심 높음';
    case 'consider':
      return '청약 고려';
    case 'neutral':
      return '중립';
    case 'caution':
      return '주의';
    default:
      return '데이터 부족';
  }
}

List<String> reasonsFor(
  IpoCompetitionStock stock,
  int score,
  double? competitionRate,
) {
  final reasons = <String>[];
  if (competitionRate != null) {
    reasons.add('최근 확인된 일반청약 경쟁률은 ${roundDouble(competitionRate, 2)}대 1입니다.');
  }
  if (stock.leadManagers.length >= 2) {
    reasons.add('복수 주관사가 참여해 청약 채널이 분산되어 있습니다.');
  }
  if (score >= 70) {
    reasons.add('현재 입력 데이터 기준 청약 매력도 점수가 평균 이상입니다.');
  }
  if (reasons.isEmpty) {
    reasons.add('아직 판단에 필요한 입력 데이터가 충분하지 않습니다.');
  }
  return reasons;
}

List<String> warningsFor(
  IpoCompetitionStock stock,
  double confidence,
  double? competitionRate,
) {
  final warnings = <String>[];
  if (confidence < 0.6) {
    warnings.add('기관 수요예측, 확약, 유통가능물량 등 핵심 입력이 부족해 신뢰도가 낮습니다.');
  }
  if (competitionRate != null && competitionRate >= 1000) {
    warnings.add('경쟁률이 높아 실제 배정 수량은 매우 작을 수 있습니다.');
  }
  if (stock.latestOfferPrice == null) {
    warnings.add('공모가가 없어 기대 수익은 3만원 가정값으로 계산했습니다.');
  }
  warnings.add('본 지표는 투자 권유가 아니라 공개 데이터 기반 참고값입니다.');
  return warnings;
}

List<IpoBrokerScore> brokerScoresFor(IpoCompetitionStock stock) {
  final offerPrice = stock.latestOfferPrice;
  final brokerMetrics = <String, IpoBrokerCompetition>{};
  for (final snapshot in stock.snapshots) {
    for (final broker in snapshot.brokers) {
      if (broker.name == '통합') {
        continue;
      }
      brokerMetrics[broker.name] = broker;
    }
  }
  final scores = brokerMetrics.values.map((broker) {
    final expectedEqual = broker.equalExpectedSharesPerAccount;
    final depositForOne = broker.estimatedDepositForOneProportionalShare(
      offerPrice,
    );
    final equalScore = expectedEqual == null
        ? 30
        : clampInt((expectedEqual * 80).round(), 0, 100);
    final proportionalScore = depositForOne == null
        ? 30
        : clampInt((100000000 / depositForOne).round(), 0, 100);
    final quality = broker.applicationCount != null &&
            (broker.proportionalCompetitionRate != null ||
                broker.competitionRate != null)
        ? 'broker_verified'
        : 'partial';
    return IpoBrokerScore(
      broker: broker.name,
      equalScore: equalScore,
      proportionalScore: proportionalScore,
      expectedEqualShares: expectedEqual,
      estimatedDepositForOneProportionalShare: depositForOne,
      feeKrw: broker.feeKrw,
      dataQuality: quality,
    );
  }).toList()
    ..sort((a, b) {
      final byEqual = b.equalScore.compareTo(a.equalScore);
      if (byEqual != 0) {
        return byEqual;
      }
      return b.proportionalScore.compareTo(a.proportionalScore);
    });
  return scores;
}

int clampInt(int value, int min, int max) {
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
}

double clampDouble(double value, double min, double max) {
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
}

double roundDouble(double value, int digits) {
  final factor = mathPow10(digits);
  return (value * factor).round() / factor;
}

double mathPow10(int digits) {
  var result = 1.0;
  for (var i = 0; i < digits; i += 1) {
    result *= 10;
  }
  return result;
}

Map<String, Object?> buildBacktestReport(
  List<IpoCompetitionStock> stocks,
  DateTime generatedAt,
) {
  final rows = stocks
      .map((stock) {
        final outcome = stock.outcome;
        if (outcome?.closeReturnRate == null) {
          return null;
        }
        final analysis = analyzeStock(stock);
        return <String, Object?>{
          'id': safeId(stock.id),
          'company': stock.company,
          'score': analysis.score.overall,
          'grade': analysis.score.grade,
          'confidence': roundDouble(analysis.score.confidence, 2),
          'expectedListingGainRate':
              roundDouble(analysis.expectedReturn.expectedListingGainRate, 4),
          'openReturnRate': outcome?.openReturnRate,
          'highReturnRate': outcome?.highReturnRate,
          'closeReturnRate': outcome?.closeReturnRate,
          'outcomeSourceUrl': outcome?.sourceUrl,
          'errorCloseVsExpected': outcome?.closeReturnRate == null
              ? null
              : roundDouble(
                  outcome!.closeReturnRate! -
                      analysis.expectedReturn.expectedListingGainRate,
                  4,
                ),
        };
      })
      .whereType<Map<String, Object?>>()
      .toList()
    ..sort((a, b) => (b['score'] as int).compareTo(a['score'] as int));

  return {
    'schemaVersion': schemaVersion,
    'generatedAt': generatedAt.toIso8601String(),
    'methodVersion': 'ipo-score-v1',
    'sampleCount': rows.length,
    'summary': summarizeBacktestRows(rows),
    'byGrade': summarizeByGrade(rows),
    'byScoreBucket': summarizeByScoreBucket(rows),
    'rows': rows,
    'note':
        'Backtest is exploratory. Sample size is currently too small for predictive calibration.',
  };
}

Map<String, Object?> summarizeBacktestRows(List<Map<String, Object?>> rows) {
  final closes = rows
      .map((row) => row['closeReturnRate'])
      .whereType<double>()
      .toList();
  final errors = rows
      .map((row) => row['errorCloseVsExpected'])
      .whereType<double>()
      .toList();
  return {
    'averageCloseReturnRate': average(closes),
    'medianCloseReturnRate': median(closes),
    'averageErrorCloseVsExpected': average(errors),
    'medianErrorCloseVsExpected': median(errors),
  };
}

Map<String, Object?> summarizeByGrade(List<Map<String, Object?>> rows) {
  final grouped = <String, List<Map<String, Object?>>>{};
  for (final row in rows) {
    final grade = '${row['grade']}';
    grouped.putIfAbsent(grade, () => []).add(row);
  }
  return grouped.map((grade, gradeRows) {
    final closes = gradeRows
        .map((row) => row['closeReturnRate'])
        .whereType<double>()
        .toList();
    return MapEntry(grade, {
      'sampleCount': gradeRows.length,
      'averageCloseReturnRate': average(closes),
      'medianCloseReturnRate': median(closes),
    });
  });
}

Map<String, Object?> summarizeByScoreBucket(List<Map<String, Object?>> rows) {
  final grouped = <String, List<Map<String, Object?>>>{};
  for (final row in rows) {
    final score = row['score'];
    if (score is! int) {
      continue;
    }
    final bucketStart = (score ~/ 10) * 10;
    final bucket = '$bucketStart-${bucketStart + 9}';
    grouped.putIfAbsent(bucket, () => []).add(row);
  }
  return grouped.map((bucket, bucketRows) {
    final closes = bucketRows
        .map((row) => row['closeReturnRate'])
        .whereType<double>()
        .toList();
    final errors = bucketRows
        .map((row) => row['errorCloseVsExpected'])
        .whereType<double>()
        .toList();
    return MapEntry(bucket, {
      'sampleCount': bucketRows.length,
      'averageCloseReturnRate': average(closes),
      'medianCloseReturnRate': median(closes),
      'averageErrorCloseVsExpected': average(errors),
    });
  });
}

double? average(List<double> values) {
  if (values.isEmpty) {
    return null;
  }
  final total = values.fold<double>(0, (sum, value) => sum + value);
  return roundDouble(total / values.length, 4);
}

double? median(List<double> values) {
  if (values.isEmpty) {
    return null;
  }
  final sorted = [...values]..sort();
  final middle = sorted.length ~/ 2;
  if (sorted.length.isOdd) {
    return roundDouble(sorted[middle], 4);
  }
  return roundDouble((sorted[middle - 1] + sorted[middle]) / 2, 4);
}

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
    fundamentals: const IpoFundamentals(
      offerPrice: null,
      priceBandMin: null,
      priceBandMax: null,
      institutionCompetitionRate: null,
      institutionParticipants: null,
      lockupCommitmentRate: null,
      floatRate: null,
      marketCapKrw: null,
      publicAllocationShares: null,
    ),
    outcome: null,
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
    fundamentals: const IpoFundamentals(
      offerPrice: null,
      priceBandMin: null,
      priceBandMax: null,
      institutionCompetitionRate: null,
      institutionParticipants: null,
      lockupCommitmentRate: null,
      floatRate: null,
      marketCapKrw: null,
      publicAllocationShares: null,
    ),
    outcome: null,
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
