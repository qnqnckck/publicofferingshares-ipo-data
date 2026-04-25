import 'dart:async';
import 'dart:convert';
import 'dart:io';

const schemaVersion = 1;
IpoAnalysisCalibration _analysisCalibration = const IpoAnalysisCalibration();

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
    required this.identifierPath,
    required this.discoveredPath,
    required this.outDir,
    required this.backfillYears,
    required this.interval,
    required this.discover,
    required this.discoverIdentifiers,
    required this.dartApiKeyEnv,
    required this.itickApiKeyEnv,
    required this.kisAppKeyEnv,
    required this.kisAppSecretEnv,
    required this.watch,
    required this.help,
  });

  final String seedPath;
  final String liveDir;
  final String outcomeDir;
  final String brokerSnapshotDir;
  final String identifierPath;
  final String discoveredPath;
  final String outDir;
  final int backfillYears;
  final Duration interval;
  final bool discover;
  final bool discoverIdentifiers;
  final String dartApiKeyEnv;
  final String itickApiKeyEnv;
  final String kisAppKeyEnv;
  final String kisAppSecretEnv;
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
  --identifier-path <path>    Identifier crosswalk JSON path. Default: data/identifiers/ipo_identifiers.json
  --discovered <path>         Auto-discovered stock JSON path. Default: data/discovered/ipo_events.json
  --out <dir>                 Output directory. Default: ipo_competition_data
  --backfill-years <years>    Include IPOs from the last N years. Default: 3
  --interval-minutes <min>    Watch interval. Default: 10
  --dart-api-key-env <name>   Environment variable for DART API key. Default: DART_API_KEY
  --itick-api-key-env <name>  Environment variable for iTick API key. Default: ITICK_API_KEY
  --kis-app-key-env <name>    Environment variable for KIS app key. Default: KIS_APP_KEY
  --kis-app-secret-env <name> Environment variable for KIS app secret. Default: KIS_APP_SECRET
  --no-discover               Skip remote discovery and only normalize local input files.
  --no-identifier-discover    Skip DART company-code backfill and only use local identifier crosswalk.
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
      brokerSnapshotDir: valueAfter(
        '--broker-snapshot-dir',
        'data/broker_snapshots',
      ),
      identifierPath: valueAfter(
        '--identifier-path',
        'data/identifiers/ipo_identifiers.json',
      ),
      discoveredPath: valueAfter(
        '--discovered',
        'data/discovered/ipo_events.json',
      ),
      outDir: valueAfter('--out', 'ipo_competition_data'),
      backfillYears: intAfter('--backfill-years', 3),
      interval: Duration(minutes: intAfter('--interval-minutes', 10)),
      discover: !args.contains('--no-discover'),
      discoverIdentifiers: !args.contains('--no-identifier-discover'),
      dartApiKeyEnv: valueAfter('--dart-api-key-env', 'DART_API_KEY'),
      itickApiKeyEnv: valueAfter('--itick-api-key-env', 'ITICK_API_KEY'),
      kisAppKeyEnv: valueAfter('--kis-app-key-env', 'KIS_APP_KEY'),
      kisAppSecretEnv: valueAfter('--kis-app-secret-env', 'KIS_APP_SECRET'),
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
      final supplementStocks = mergeStocks([
        ...stocksWithoutExternalOutcomes,
        ...await _discoverIpoKoreaSupplementStocks(
          stocksWithoutExternalOutcomes,
          generatedAt,
        ),
      ]);
      final sourceEnhancedStocks = mergeStocks([
        ...supplementStocks,
        ...buildKnownLeadManagerOverrideStocks(supplementStocks),
        ...await _discoverArticleLeadManagerStocks(supplementStocks),
      ]);
      final stocks = mergeOutcomes(
        sourceEnhancedStocks,
        await _loadOutcomeRows(),
      );
      final localIdentifierRows = await _loadIdentifierRows();
      final identifierRows = mergeIdentifierRowsByKey([
        ...localIdentifierRows,
        if (options.discoverIdentifiers)
          ...await _discoverDartIdentifierRows(
            mergeIdentifierRows(stocks, localIdentifierRows),
          ),
      ]);
      await _writeIdentifierRows(identifierRows);
      final identifiedStocks = mergeIdentifierRows(stocks, identifierRows);
      final brokerSnapshotRows = [
        ...await _loadBrokerSnapshotRows(),
        ...await _collectPublicLiveBrokerSnapshots(
          identifiedStocks,
          generatedAt,
        ),
        ...buildEstimatedBrokerSnapshotRows(identifiedStocks, generatedAt),
        ...buildEstimatedBrokerRateOnlyRows(identifiedStocks, generatedAt),
      ];
      final enrichedStocks = mergeBrokerSnapshots(
        identifiedStocks,
        brokerSnapshotRows,
      );
      final cutoff = DateTime(
        generatedAt.year - options.backfillYears,
        generatedAt.month,
        generatedAt.day,
      );
      final selected =
          enrichedStocks.where((stock) {
            final end = parseDate(stock.subscriptionEnd);
            return end == null || !end.isBefore(cutoff);
          }).toList()..sort((a, b) {
            final byDate = (b.subscriptionStart ?? '').compareTo(
              a.subscriptionStart ?? '',
            );
            if (byDate != 0) {
              return byDate;
            }
            return a.company.compareTo(b.company);
          });

      _analysisCalibration = buildAnalysisCalibration(selected);

      await Directory('${options.outDir}/stocks').create(recursive: true);
      final indexStocks = <Map<String, Object?>>[];

      for (final stock in selected) {
        final normalized = stock.normalized();
        final path = 'stocks/${stock.id}.json';
        await File(
          '${options.outDir}/$path',
        ).writeAsString(prettyJson(normalized.toJson()));
        indexStocks.add(normalized.toIndexJson(path));
      }

      final index = <String, Object?>{
        'schemaVersion': schemaVersion,
        'generatedAt': generatedAt.toIso8601String(),
        'stocks': indexStocks,
      };
      await File(
        '${options.outDir}/index.json',
      ).writeAsString(prettyJson(index));
      await writeLightweightFeeds(
        outDir: options.outDir,
        generatedAt: generatedAt,
        stocks: selected,
      );
      await File(
        '${options.outDir}/backtest_report.json',
      ).writeAsString(prettyJson(buildBacktestReport(selected, generatedAt)));
      await File('${options.outDir}/calibration_report.json').writeAsString(
        prettyJson(
          buildCalibrationReport(
            stocks: selected,
            generatedAt: generatedAt,
            calibration: _analysisCalibration,
          ),
        ),
      );
      await File('${options.outDir}/coverage_report.json').writeAsString(
        prettyJson(
          buildCoverageReport(
            generatedAt: generatedAt,
            cutoff: cutoff,
            discoveredStocks: discoveredStocks,
            mergedStocks: enrichedStocks,
            selectedStocks: selected,
          ),
        ),
      );
      await File(
        '${options.outDir}/broker_metrics_missing_report.json',
      ).writeAsString(
        prettyJson(
          buildBrokerMetricsMissingReport(
            generatedAt: generatedAt,
            stocks: selected,
          ),
        ),
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
          (decoded['stocks'] as List).whereType<Map<String, Object?>>().map(
            IpoCompetitionStock.fromJson,
          ),
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
          (decoded['outcomes'] as List).whereType<Map<String, Object?>>().map(
            IpoOutcomeRow.fromJson,
          ),
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
          (decoded['snapshots'] as List).whereType<Map<String, Object?>>().map(
            IpoBrokerSnapshotRow.fromJson,
          ),
        );
      } else if (decoded is Map<String, Object?>) {
        rows.add(IpoBrokerSnapshotRow.fromJson(decoded));
      }
    }
    return rows;
  }

  Future<List<IpoIdentifierRow>> _loadIdentifierRows() async {
    final file = File(options.identifierPath);
    if (!await file.exists()) {
      return const [];
    }
    final decoded = jsonDecode(await file.readAsString());
    if (decoded is! Map<String, Object?> || decoded['identifiers'] is! List) {
      return const [];
    }
    return (decoded['identifiers'] as List)
        .whereType<Map<String, Object?>>()
        .map(IpoIdentifierRow.fromJson)
        .toList();
  }

  Future<void> _writeIdentifierRows(List<IpoIdentifierRow> rows) async {
    final file = File(options.identifierPath);
    await file.parent.create(recursive: true);
    await file.writeAsString(
      prettyJson({
        'schemaVersion': schemaVersion,
        'generatedAt': DateTime.now().toIso8601String(),
        'identifiers': rows.map((row) => row.toJson()).toList()
          ..sort((a, b) {
            final aCompany = '${a['company'] ?? ''}';
            final bCompany = '${b['company'] ?? ''}';
            return aCompany.compareTo(bCompany);
          }),
      }),
    );
  }

  Future<List<IpoIdentifierRow>> _discoverDartIdentifierRows(
    List<IpoCompetitionStock> stocks,
  ) async {
    final rows = <IpoIdentifierRow>[];
    for (final stock in stocks) {
      if (stock.identifiers.corpCode != null &&
          stock.identifiers.corpCode!.trim().isNotEmpty) {
        rows.add(
          IpoIdentifierRow(
            id: stock.id,
            company: stock.company,
            identifiers: stock.identifiers,
          ),
        );
        continue;
      }
      final corpCode = await _fetchDartCorpCode(stock.company);
      if (corpCode == null || corpCode.trim().isEmpty) {
        continue;
      }
      rows.add(
        IpoIdentifierRow(
          id: stock.id,
          company: stock.company,
          identifiers: stock.identifiers.merge(
            IpoStockIdentifiers(
              subscriptionKey: '',
              normalizedCompany: '',
              corpCode: corpCode,
              stockCode: null,
              kindCode: null,
              isin: null,
            ),
          ),
        ),
      );
    }
    return rows;
  }

  Future<String?> _fetchDartCorpCode(String company) async {
    final body = await httpPostText(
      Uri.parse('https://dart.fss.or.kr/corp/searchCorp.ax'),
      {'textCrpNm': company},
    );
    if (body == null || body.trim().isEmpty) {
      return null;
    }
    final hidden = RegExp(
      r'''name=["']hiddenCikCD1["'][^>]*value=["'](\d+)["']''',
      caseSensitive: false,
    ).firstMatch(body);
    if (hidden != null) {
      return hidden.group(1);
    }
    final select = RegExp(
      r'''select\(["'](\d+)["']\)''',
      caseSensitive: false,
    ).firstMatch(body);
    return select?.group(1);
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
        'stocks': stocks.map((stock) => stock.normalized().toJson()).toList()
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
    _noteKisCredentialsIfConfigured();
    return discovered;
  }

  Future<List<IpoCompetitionStock>> _discoverIpoKoreaSupplementStocks(
    List<IpoCompetitionStock> stocks,
    DateTime now,
  ) async {
    final today = DateTime(now.year, now.month, now.day);

    bool isCompleted(IpoCompetitionStock stock) {
      final end =
          parseDate(stock.subscriptionEnd) ??
          parseDate(stock.subscriptionStart);
      return end != null && !end.isAfter(today);
    }

    bool needsSupplement(IpoCompetitionStock stock) {
      return stock.fundamentals.institutionParticipants == null ||
          stock.fundamentals.lockupCommitmentRate == null ||
          stock.fundamentals.publicAllocationShares == null ||
          stock.latestSnapshot?.aggregate.competitionRate == null;
    }

    final candidates = stocks.where(isCompleted).where(needsSupplement).toList()
      ..sort(
        (a, b) => (b.subscriptionEnd ?? b.subscriptionStart ?? '').compareTo(
          a.subscriptionEnd ?? a.subscriptionStart ?? '',
        ),
      );

    final supplements = <IpoCompetitionStock>[];
    for (final stock in candidates.take(12)) {
      final sourceUrl =
          'https://ipokorea.kr/ipo/${Uri.encodeComponent(stock.company)}';
      final body = await httpGetFirstText([sourceUrl]);
      if (body == null || body.trim().isEmpty) {
        continue;
      }
      final supplement = parseIpoKoreaSupplement(
        stock: stock,
        text: body,
        sourceUrl: sourceUrl,
      );
      if (supplement != null) {
        supplements.add(supplement);
      }
    }
    return supplements;
  }

  Future<List<IpoCompetitionStock>> _discoverArticleLeadManagerStocks(
    List<IpoCompetitionStock> stocks,
  ) async {
    final candidates =
        stocks.where((stock) {
          if (stock.leadManagers.isNotEmpty) {
            return false;
          }
          final sourceUrl = stock.latestSnapshot?.sourceUrl;
          return sourceUrl != null &&
              sourceUrl.trim().startsWith(RegExp(r'https?://'));
        }).toList()..sort(
          (a, b) => (b.subscriptionEnd ?? b.subscriptionStart ?? '').compareTo(
            a.subscriptionEnd ?? a.subscriptionStart ?? '',
          ),
        );

    final supplements = <IpoCompetitionStock>[];
    for (final stock in candidates.take(24)) {
      final sourceUrl = stock.latestSnapshot?.sourceUrl;
      if (sourceUrl == null || sourceUrl.trim().isEmpty) {
        continue;
      }
      final body = await httpGetFirstText([sourceUrl]);
      if (body == null || body.trim().isEmpty) {
        continue;
      }
      final leadManagers = extractKnownBrokerNames(body);
      if (leadManagers.isEmpty) {
        continue;
      }
      supplements.add(
        IpoCompetitionStock(
          id: stock.id,
          company: stock.company,
          market: stock.market,
          subscriptionStart: stock.subscriptionStart,
          subscriptionEnd: stock.subscriptionEnd,
          leadManagers: leadManagers,
          sourceIdentifiers: stock.identifiers,
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
        ),
      );
    }
    return supplements;
  }

  void _noteKisCredentialsIfConfigured() {
    final appKey = Platform.environment[options.kisAppKeyEnv]?.trim() ?? '';
    final appSecret =
        Platform.environment[options.kisAppSecretEnv]?.trim() ?? '';
    if (appKey.isEmpty && appSecret.isEmpty) {
      return;
    }
    if (appKey.isEmpty || appSecret.isEmpty) {
      stderr.writeln(
        'KIS OpenAPI credentials are partially configured. Set both ${options.kisAppKeyEnv} and ${options.kisAppSecretEnv}.',
      );
      return;
    }
    stderr.writeln(
      'KIS OpenAPI credentials detected. IPO subscription competition adapter is not enabled until a verified KIS endpoint is added.',
    );
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

  Future<List<IpoBrokerSnapshotRow>> _collectPublicLiveBrokerSnapshots(
    List<IpoCompetitionStock> stocks,
    DateTime now,
  ) async {
    final today = DateTime(now.year, now.month, now.day);
    final active = stocks.where((stock) {
      final start = parseDate(stock.subscriptionStart);
      final end = parseDate(stock.subscriptionEnd) ?? start;
      if (start == null || end == null) {
        return false;
      }
      return !today.isBefore(start) && !today.isAfter(end);
    }).toList();
    if (active.isEmpty) {
      return const [];
    }

    final rows = <IpoBrokerSnapshotRow>[];
    for (final stock in active) {
      rows.addAll(await _fetchPublicLiveSnapshots(stock, now));
    }
    return rows;
  }

  Future<List<IpoBrokerSnapshotRow>> _fetchPublicLiveSnapshots(
    IpoCompetitionStock stock,
    DateTime now,
  ) async {
    final rows = <IpoBrokerSnapshotRow>[];
    final collectors = [
      () => _fetchShinhanLiveSnapshot(stock, now),
      () => _fetchDaishinLiveSnapshot(stock, now),
      () => _fetchIpostockLiveSnapshot(stock, now),
      () => _fetch38NewsLiveSnapshot(stock, now),
    ];
    for (final collect in collectors) {
      try {
        final row = await collect();
        if (row != null) {
          rows.add(row);
        }
      } catch (error) {
        stderr.writeln(
          'Live competition collector failed for ${stock.company}: $error',
        );
      }
    }
    return rows;
  }

  Future<IpoBrokerSnapshotRow?> _fetchShinhanLiveSnapshot(
    IpoCompetitionStock stock,
    DateTime now,
  ) async {
    final candidates = [
      parseDate(stock.subscriptionEnd),
      parseDate(stock.subscriptionStart),
      now,
    ].whereType<DateTime>();
    for (final date in candidates) {
      final response = await httpPostJson(
        Uri.parse(
          'https://www.shinhansec.com/siw/banking-lending/subscribe/596001/data.do',
        ),
        {'logined': 'false', 'eDate': compactDate(date)},
      );
      final body = response['body'];
      final list = body is Map<String, Object?> ? body['list2'] : null;
      if (list is! List) {
        continue;
      }
      final stockKey = normalizeLookup(stock.company);
      for (final item in list.whereType<Map<String, Object?>>()) {
        final title =
            '${item['subEvent'] ?? item['eventName'] ?? item['eventNm'] ?? ''}';
        final titleKey = normalizeLookup(title);
        if (titleKey.isEmpty ||
            !(titleKey.contains(stockKey) || stockKey.contains(titleKey))) {
          continue;
        }
        final rate = parseCompetitionRate(
          '${item['ourCompetition'] ?? item['competitionRate'] ?? ''}',
        );
        final applicationCount = parseCountValue(
          '${item['applyCnt'] ?? item['applicationCount'] ?? ''}',
        );
        final allocationShares = parseCountValue(
          '${item['ourAssignStockCnt'] ?? item['assignStockCnt'] ?? ''}',
        );
        if (rate <= 0 && applicationCount == null && allocationShares == null) {
          continue;
        }
        final offered =
            allocationShares ?? stock.fundamentals.publicAllocationShares ?? 0;
        return IpoBrokerSnapshotRow(
          id: stock.id,
          company: stock.company,
          capturedAt: now.toIso8601String(),
          source: 'shinhan_live',
          sourceUrl:
              'https://www.shinhansec.com/siw/banking-lending/subscribe/596001/view.do',
          brokers: [
            IpoBrokerCompetition(
              name: '신한투자증권',
              offeredShares: offered,
              subscribedShares: rate > 0 && offered > 0
                  ? (offered * rate).round()
                  : 0,
              offerPrice: stock.fundamentals.offerPrice,
              depositRate: 0.5,
              feeKrw: null,
              competitionRate: rate > 0 ? rate : null,
              equalCompetitionRate: null,
              proportionalCompetitionRate: rate > 0 ? rate : null,
              equalAllocationShares: offered > 0 ? (offered / 2).round() : null,
              proportionalAllocationShares: offered > 0
                  ? (offered / 2).round()
                  : null,
              applicationCount: applicationCount,
            ),
          ],
        );
      }
    }
    return null;
  }

  Future<IpoBrokerSnapshotRow?> _fetchDaishinLiveSnapshot(
    IpoCompetitionStock stock,
    DateTime now,
  ) async {
    final body = await httpGetFirstText([
      'https://www.daishin.com/g.ds?m=194&p=1031&v=681',
    ]);
    if (body == null) {
      return null;
    }
    final rows = extractHtmlTableRows(body);
    final stockKey = normalizeLookup(stock.company);
    for (final row in rows) {
      if (row.length < 6) {
        continue;
      }
      final joined = row.join(' ');
      if (!normalizeLookup(joined).contains(stockKey)) {
        continue;
      }
      final rates = row
          .map(parseCompetitionRate)
          .where((rate) => rate > 0)
          .toList();
      if (rates.isEmpty) {
        continue;
      }
      final rate = rates.length >= 2 ? rates[1] : rates.first;
      final countCandidates = row
          .map(parseCountValue)
          .whereType<int>()
          .where((value) => value > 100)
          .toList();
      final applicationCount = countCandidates.isEmpty
          ? null
          : countCandidates.last;
      final offered = stock.fundamentals.publicAllocationShares ?? 0;
      return IpoBrokerSnapshotRow(
        id: stock.id,
        company: stock.company,
        capturedAt: now.toIso8601String(),
        source: 'daishin_live',
        sourceUrl: 'https://www.daishin.com/g.ds?m=194&p=1031&v=681',
        brokers: [
          IpoBrokerCompetition(
            name: '대신증권',
            offeredShares: offered,
            subscribedShares: offered > 0 ? (offered * rate).round() : 0,
            offerPrice: stock.fundamentals.offerPrice,
            depositRate: 0.5,
            feeKrw: null,
            competitionRate: rate,
            equalCompetitionRate: null,
            proportionalCompetitionRate: rate,
            equalAllocationShares: offered > 0 ? (offered / 2).round() : null,
            proportionalAllocationShares: offered > 0
                ? (offered / 2).round()
                : null,
            applicationCount: applicationCount,
          ),
        ],
      );
    }
    return null;
  }

  Future<IpoBrokerSnapshotRow?> _fetchIpostockLiveSnapshot(
    IpoCompetitionStock stock,
    DateTime now,
  ) async {
    final baseDate = parseDate(stock.subscriptionStart) ?? now;
    final listUrls = [
      'http://www.ipostock.co.kr/sub03/ipo04.asp?str1=${baseDate.year}&str2=${baseDate.month}',
      'http://www.ipostock.co.kr/sub03/ipo04.asp',
    ];
    final listBody = await httpGetFirstText(listUrls);
    if (listBody == null) {
      return null;
    }

    final detailPath = extractCommunityDetailPath(
      html: listBody,
      company: stock.company,
      pathPattern: RegExp(
        r"""((?:https?://(?:www\.)?ipostock\.co\.kr)?/view_pg/view_0[24]\.asp\?code=[A-Za-z0-9]+[^'\"\s\)]*)""",
        caseSensitive: false,
      ),
    );
    if (detailPath == null) {
      return null;
    }

    final normalizedPath = detailPath.contains('view_04.asp')
        ? detailPath
        : detailPath.contains('?')
        ? '${detailPath.replaceFirst('view_02.asp', 'view_04.asp')}&schk=2'
        : '${detailPath.replaceFirst('view_02.asp', 'view_04.asp')}?schk=2';
    final detailUrl = Uri.parse(
      'http://www.ipostock.co.kr',
    ).resolve(normalizedPath.replaceAll('&amp;', '&')).toString();
    final detailBody = await httpGetFirstText([detailUrl]);
    if (detailBody == null) {
      return null;
    }

    final snapshot = parseIpostockLiveSnapshot(
      stock: stock,
      capturedAt: now.toIso8601String(),
      sourceUrl: detailUrl,
      html: detailBody,
    );
    return snapshot;
  }

  Future<IpoBrokerSnapshotRow?> _fetch38NewsLiveSnapshot(
    IpoCompetitionStock stock,
    DateTime now,
  ) async {
    final searchBody = await httpGetFirstText([
      'https://www.38.co.kr/html/news/?m=nostock&key=${Uri.encodeQueryComponent(stock.company)}',
      'http://www.38.co.kr/html/news/?m=nostock&key=${Uri.encodeQueryComponent(stock.company)}',
    ]);
    if (searchBody == null) {
      return null;
    }
    final newsPath = extractCommunityDetailPath(
      html: searchBody,
      company: stock.company,
      pathPattern: RegExp(
        r"""((?:https?://(?:www\.)?38\.co\.kr)?/html/news/(?:\?o=v[^'\"\s\)]*no=\d+[^'\"\s\)]*|readnews\.php3\?[^'\"\s\)]*no=\d+[^'\"\s\)]*))""",
        caseSensitive: false,
      ),
    );
    if (newsPath == null) {
      return null;
    }
    final newsUrl = Uri.parse(
      'https://www.38.co.kr',
    ).resolve(newsPath).toString();
    final newsBody = await httpGetFirstText([newsUrl]);
    if (newsBody == null) {
      return null;
    }
    final brokers = parse38NewsBrokerCompetitions(stock: stock, text: newsBody);
    if (brokers.isEmpty) {
      return null;
    }
    return IpoBrokerSnapshotRow(
      id: stock.id,
      company: stock.company,
      capturedAt: now.toIso8601String(),
      source: '38_news_live',
      sourceUrl: newsUrl,
      brokers: brokers,
    );
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
    required this.sourceIdentifiers,
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
  final IpoStockIdentifiers? sourceIdentifiers;
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
      sourceIdentifiers: json['identifiers'] is Map<String, Object?>
          ? IpoStockIdentifiers.fromJson(
              json['identifiers'] as Map<String, Object?>,
            )
          : null,
      fundamentals: IpoFundamentals.fromJson(
        json['fundamentals'] is Map<String, Object?>
            ? json['fundamentals'] as Map<String, Object?>
            : const {},
      ),
      outcome: json['outcome'] is Map<String, Object?>
          ? IpoOutcome.fromJson(json['outcome'] as Map<String, Object?>)
          : null,
      snapshots: readObjectList(
        json['snapshots'],
      ).map(IpoCompetitionSnapshot.fromJson).toList(),
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
      sourceIdentifiers: identifiers,
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
      'identifiers': identifiers.toJson(),
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
      'identifiers': identifiers.toJson(),
      'company': company,
      'market': market,
      'subscriptionStart': subscriptionStart,
      'subscriptionEnd': subscriptionEnd,
      'leadManagers': leadManagers,
      'offerPrice': fundamentals.offerPrice,
      'priceBandMin': fundamentals.priceBandMin,
      'priceBandMax': fundamentals.priceBandMax,
      'listingDate': outcome?.listingDate,
      'latestCompetitionRate': latest?.aggregate.competitionRate,
      'latestSnapshotAt': latest?.capturedAt,
      'score': analysis.score.overall,
      'grade': analysis.score.grade,
      'decisionLevel': analysis.decision.level,
      'expectedGainRate': analysis.expectedReturn.expectedListingGainRate,
      'path': path,
    };
  }

  IpoStockIdentifiers get identifiers {
    final fallback = IpoStockIdentifiers(
      subscriptionKey: subscriptionKeyFor(
        company: company,
        subscriptionStart: subscriptionStart,
        subscriptionEnd: subscriptionEnd,
      ),
      normalizedCompany: normalizeLookup(company),
      corpCode: null,
      stockCode: null,
      kindCode: null,
      isin: null,
    );
    return fallback.merge(sourceIdentifiers);
  }
}

class IpoStockIdentifiers {
  const IpoStockIdentifiers({
    required this.subscriptionKey,
    required this.normalizedCompany,
    required this.corpCode,
    required this.stockCode,
    required this.kindCode,
    required this.isin,
  });

  final String subscriptionKey;
  final String normalizedCompany;
  final String? corpCode;
  final String? stockCode;
  final String? kindCode;
  final String? isin;

  factory IpoStockIdentifiers.fromJson(Map<String, Object?> json) {
    return IpoStockIdentifiers(
      subscriptionKey: readString(json, 'subscriptionKey') ?? '',
      normalizedCompany: readString(json, 'normalizedCompany') ?? '',
      corpCode: readString(json, 'corpCode'),
      stockCode: readString(json, 'stockCode'),
      kindCode: readString(json, 'kindCode'),
      isin: readString(json, 'isin'),
    );
  }

  IpoStockIdentifiers merge(IpoStockIdentifiers? other) {
    if (other == null) {
      return this;
    }
    String? clean(String? value) {
      final trimmed = value?.trim();
      return trimmed == null || trimmed.isEmpty ? null : trimmed;
    }

    return IpoStockIdentifiers(
      subscriptionKey:
          clean(other.subscriptionKey) ?? clean(subscriptionKey) ?? '',
      normalizedCompany:
          clean(other.normalizedCompany) ?? clean(normalizedCompany) ?? '',
      corpCode: clean(other.corpCode) ?? clean(corpCode),
      stockCode: clean(other.stockCode) ?? clean(stockCode),
      kindCode: clean(other.kindCode) ?? clean(kindCode),
      isin: clean(other.isin) ?? clean(isin),
    );
  }

  Map<String, Object?> toJson() {
    return {
      'subscriptionKey': subscriptionKey,
      'normalizedCompany': normalizedCompany,
      'corpCode': corpCode,
      'stockCode': stockCode,
      'kindCode': kindCode,
      'isin': isin,
    };
  }
}

String subscriptionKeyFor({
  required String company,
  required String? subscriptionStart,
  required String? subscriptionEnd,
}) {
  final start = (normalizeDate(subscriptionStart) ?? subscriptionStart ?? '')
      .replaceAll('-', '');
  final end = (normalizeDate(subscriptionEnd) ?? subscriptionEnd ?? '')
      .replaceAll('-', '');
  return [
    normalizeLookup(company),
    start,
    end,
  ].where((value) => value.isNotEmpty).join('_');
}

Future<void> writeLightweightFeeds({
  required String outDir,
  required DateTime generatedAt,
  required List<IpoCompetitionStock> stocks,
}) async {
  final today = DateTime(generatedAt.year, generatedAt.month, generatedAt.day);
  final normalized = stocks.map((stock) => stock.normalized()).toList();

  bool isActive(IpoCompetitionStock stock) {
    final start = parseDate(stock.subscriptionStart);
    final end = parseDate(stock.subscriptionEnd) ?? start;
    if (start == null || end == null) {
      return false;
    }
    return !today.isBefore(start) && !today.isAfter(end);
  }

  bool isUpcoming(IpoCompetitionStock stock) {
    final start = parseDate(stock.subscriptionStart);
    return start != null && start.isAfter(today);
  }

  bool isRecent(IpoCompetitionStock stock) {
    final end =
        parseDate(stock.subscriptionEnd) ??
        parseDate(stock.outcome?.listingDate) ??
        parseDate(stock.subscriptionStart);
    return end != null && !end.isAfter(today);
  }

  List<Map<String, Object?>> feedItems(
    Iterable<IpoCompetitionStock> source,
    int limit,
  ) {
    return source
        .take(limit)
        .map((stock) => stock.toIndexJson('stocks/${stock.id}.json'))
        .toList();
  }

  Future<void> writeFeed(String path, List<Map<String, Object?>> items) async {
    await File('$outDir/$path').writeAsString(
      prettyJson({
        'schemaVersion': schemaVersion,
        'generatedAt': generatedAt.toIso8601String(),
        'stocks': items,
      }),
    );
  }

  final active = normalized.where(isActive).toList()
    ..sort(
      (a, b) => (a.subscriptionEnd ?? '').compareTo(b.subscriptionEnd ?? ''),
    );
  final upcoming = normalized.where(isUpcoming).toList()
    ..sort(
      (a, b) =>
          (a.subscriptionStart ?? '').compareTo(b.subscriptionStart ?? ''),
    );
  final recent = normalized.where(isRecent).toList()
    ..sort((a, b) {
      final aDate =
          a.subscriptionEnd ??
          a.outcome?.listingDate ??
          a.subscriptionStart ??
          '';
      final bDate =
          b.subscriptionEnd ??
          b.outcome?.listingDate ??
          b.subscriptionStart ??
          '';
      return bDate.compareTo(aDate);
    });

  await writeFeed('active.json', feedItems(active, 30));
  await writeFeed('upcoming.json', feedItems(upcoming, 60));
  await writeFeed('recent.json', feedItems(recent, 60));

  final yearlyDir = Directory('$outDir/yearly');
  await yearlyDir.create(recursive: true);
  final byYear = <int, List<IpoCompetitionStock>>{};
  for (final stock in normalized) {
    final date =
        parseDate(stock.subscriptionStart) ??
        parseDate(stock.subscriptionEnd) ??
        parseDate(stock.outcome?.listingDate);
    if (date == null) {
      continue;
    }
    byYear.putIfAbsent(date.year, () => []).add(stock);
  }
  for (final entry in byYear.entries) {
    final yearly = entry.value
      ..sort(
        (a, b) =>
            (b.subscriptionStart ?? '').compareTo(a.subscriptionStart ?? ''),
      );
    await writeFeed(
      'yearly/${entry.key}.json',
      feedItems(yearly, yearly.length),
    );
  }
}

Map<String, Object?> buildCoverageReport({
  required DateTime generatedAt,
  required DateTime cutoff,
  required List<IpoCompetitionStock> discoveredStocks,
  required List<IpoCompetitionStock> mergedStocks,
  required List<IpoCompetitionStock> selectedStocks,
}) {
  final today = DateTime(generatedAt.year, generatedAt.month, generatedAt.day);
  final normalizedDiscovered = discoveredStocks
      .map((stock) => stock.normalized())
      .toList();
  final normalizedMerged = mergedStocks
      .map((stock) => stock.normalized())
      .toList();
  final normalizedSelected = selectedStocks
      .map((stock) => stock.normalized())
      .toList();
  final selectedKeys = normalizedSelected
      .map((stock) => stock.identifiers.subscriptionKey)
      .where((key) => key.isNotEmpty)
      .toSet();
  final selectedIds = normalizedSelected.map((stock) => stock.id).toSet();

  bool isWithinBackfill(IpoCompetitionStock stock) {
    final end = parseDate(stock.subscriptionEnd);
    return end == null || !end.isBefore(cutoff);
  }

  bool isCompleted(IpoCompetitionStock stock) {
    final end =
        parseDate(stock.subscriptionEnd) ?? parseDate(stock.subscriptionStart);
    return end != null && !end.isAfter(today);
  }

  List<String> issuesFor(IpoCompetitionStock stock) {
    final issues = <String>[];
    final fundamentals = stock.fundamentals;
    final latest = stock.latestSnapshot;
    final identifiers = stock.identifiers;
    if (fundamentals.offerPrice == null) {
      issues.add('missing_offer_price');
    }
    if (fundamentals.institutionCompetitionRate == null) {
      issues.add('missing_institution_competition_rate');
    }
    if (fundamentals.institutionParticipants == null) {
      issues.add('missing_institution_participants');
    }
    if (fundamentals.lockupCommitmentRate == null) {
      issues.add('missing_lockup_commitment_rate');
    }
    if (latest?.aggregate.competitionRate == null && isCompleted(stock)) {
      issues.add('missing_retail_competition_rate');
    }
    if (latest == null && isCompleted(stock)) {
      issues.add('missing_competition_snapshot');
    }
    final hasBrokerDetail =
        latest?.brokers.any((broker) {
          final isAggregateName =
              broker.name == '통합' || broker.name == 'aggregate';
          return !isAggregateName &&
              (broker.offeredShares > 0 ||
                  broker.competitionRate != null ||
                  broker.equalCompetitionRate != null ||
                  broker.proportionalCompetitionRate != null);
        }) ??
        false;
    if (!hasBrokerDetail && isCompleted(stock)) {
      issues.add('missing_broker_level_competition');
    }
    if (identifiers.corpCode == null &&
        identifiers.stockCode == null &&
        identifiers.kindCode == null &&
        identifiers.isin == null) {
      issues.add('missing_external_identifier');
    }
    return issues;
  }

  Map<String, Object?> stockIssueJson(
    IpoCompetitionStock stock,
    List<String> issues,
  ) {
    return {
      'id': stock.id,
      'company': stock.company,
      'subscriptionStart': stock.subscriptionStart,
      'subscriptionEnd': stock.subscriptionEnd,
      'leadManagers': stock.leadManagers,
      'path': 'stocks/${stock.id}.json',
      'issues': issues,
    };
  }

  final discoveredMissingFromGenerated = normalizedDiscovered
      .where(isWithinBackfill)
      .where((stock) {
        final key = stock.identifiers.subscriptionKey;
        return !selectedIds.contains(stock.id) &&
            (key.isEmpty || !selectedKeys.contains(key));
      })
      .map((stock) => stockIssueJson(stock, ['discovered_not_generated']))
      .toList();

  final qualityRows = <Map<String, Object?>>[];
  final issueCounts = <String, int>{};
  for (final stock in normalizedSelected) {
    final issues = issuesFor(stock);
    if (issues.isEmpty) {
      continue;
    }
    for (final issue in issues) {
      issueCounts[issue] = (issueCounts[issue] ?? 0) + 1;
    }
    final analysis = analyzeStock(stock);
    qualityRows.add({
      ...stockIssueJson(stock, issues),
      'issueCount': issues.length,
      'latestCompetitionRate': stock.latestSnapshot?.aggregate.competitionRate,
      'institutionCompetitionRate':
          stock.fundamentals.institutionCompetitionRate,
      'lockupCommitmentRate': stock.fundamentals.lockupCommitmentRate,
      'score': analysis.score.overall,
      'grade': analysis.score.grade,
    });
  }
  qualityRows.sort((a, b) {
    final byCount = (b['issueCount'] as int).compareTo(a['issueCount'] as int);
    if (byCount != 0) {
      return byCount;
    }
    return '${b['subscriptionEnd'] ?? ''}'.compareTo(
      '${a['subscriptionEnd'] ?? ''}',
    );
  });

  final byKey = <String, List<IpoCompetitionStock>>{};
  for (final stock in normalizedMerged) {
    final key = stock.identifiers.subscriptionKey;
    if (key.isEmpty) {
      continue;
    }
    byKey.putIfAbsent(key, () => []).add(stock);
  }
  final duplicateCandidates = byKey.entries
      .where((entry) => entry.value.map((stock) => stock.id).toSet().length > 1)
      .map((entry) {
        return {
          'subscriptionKey': entry.key,
          'stocks': entry.value
              .map(
                (stock) => {
                  'id': stock.id,
                  'company': stock.company,
                  'subscriptionStart': stock.subscriptionStart,
                  'subscriptionEnd': stock.subscriptionEnd,
                },
              )
              .toList(),
        };
      })
      .toList();

  return {
    'schemaVersion': schemaVersion,
    'generatedAt': generatedAt.toIso8601String(),
    'backfillCutoff': cutoff.toIso8601String(),
    'totals': {
      'discovered': normalizedDiscovered.length,
      'merged': normalizedMerged.length,
      'generated': normalizedSelected.length,
      'discoveredMissingFromGenerated': discoveredMissingFromGenerated.length,
      'stocksWithQualityIssues': qualityRows.length,
      'duplicateCandidates': duplicateCandidates.length,
    },
    'issueCounts': issueCounts,
    'discoveredMissingFromGenerated': discoveredMissingFromGenerated,
    'qualityIssues': qualityRows,
    'duplicateCandidates': duplicateCandidates,
  };
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
      sourceIdentifiers: existing.identifiers.merge(stock.identifiers),
      fundamentals: existing.fundamentals.merge(stock.fundamentals),
      outcome: stock.outcome ?? existing.outcome,
      snapshots: [...existing.snapshots, ...stock.snapshots],
    );
  }
  return byId.values.toList();
}

List<IpoCompetitionStock> buildKnownLeadManagerOverrideStocks(
  List<IpoCompetitionStock> stocks,
) {
  const overrides = <String, List<String>>{
    'mnc_solution_2024': ['KB증권', '삼성증권', '키움증권'],
    'wits_2024': ['신한투자증권'],
    'toprun_total_solution_2024': ['KB증권'],
    'yj_link_2024': ['KB증권'],
    'iron_device_2024': ['대신증권'],
    'next_biomedical_2024': ['한국투자증권'],
    'k3i_2024': ['하나증권'],
    'higen_rnm_2024': ['한국투자증권'],
    'seers_technology_2024': ['한국투자증권'],
    'imbdx_2024': ['미래에셋증권'],
    'samhyun_2024': ['한국투자증권'],
    'osang_healthcare_2024': ['NH투자증권'],
    'posbank_2024': ['하나증권'],
  };

  return stocks
      .where((stock) => stock.leadManagers.isEmpty)
      .where((stock) => overrides.containsKey(safeId(stock.id)))
      .map((stock) {
        return IpoCompetitionStock(
          id: stock.id,
          company: stock.company,
          market: stock.market,
          subscriptionStart: stock.subscriptionStart,
          subscriptionEnd: stock.subscriptionEnd,
          leadManagers: overrides[safeId(stock.id)]!,
          sourceIdentifiers: stock.identifiers,
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
      })
      .toList();
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
      sourceIdentifiers: stock.identifiers,
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
    final seen = <IpoBrokerSnapshotRow>{};
    final matches = <IpoBrokerSnapshotRow>[];
    for (final row in [
      ...?byId[safeId(stock.id)],
      ...?byCompany[normalizeLookup(stock.company)],
    ]) {
      if (seen.add(row)) {
        matches.add(row);
      }
    }
    final hasVerifiedBrokerRow = matches.any(
      (row) =>
          !row.source.startsWith('estimated_') &&
          row.brokers.any(
            (broker) =>
                normalizeLookup(broker.name) != normalizeLookup('통합') &&
                ((broker.applicationCount ?? 0) > 0 ||
                    (broker.equalAllocationShares ?? 0) > 0 ||
                    (broker.proportionalAllocationShares ?? 0) > 0 ||
                    broker.proportionalCompetitionRate != null ||
                    broker.equalCompetitionRate != null),
          ),
    );
    if (hasVerifiedBrokerRow) {
      matches.removeWhere((row) => row.source.startsWith('estimated_'));
    }
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
      sourceIdentifiers: stock.identifiers,
      fundamentals: stock.fundamentals,
      outcome: stock.outcome,
      snapshots: [...stock.snapshots, ...extraSnapshots],
    );
  }).toList();
}

List<IpoBrokerSnapshotRow> buildEstimatedBrokerSnapshotRows(
  List<IpoCompetitionStock> stocks,
  DateTime generatedAt,
) {
  final rows = <IpoBrokerSnapshotRow>[];
  for (final stock in stocks) {
    final latest = stock.latestSnapshot;
    if (latest == null) {
      continue;
    }
    final rate = latest.aggregate.competitionRate;
    final allocation = stock.fundamentals.publicAllocationShares;
    if (rate == null || rate <= 0 || allocation == null || allocation <= 0) {
      continue;
    }
    final hasBrokerDetail = stock.snapshots.any(
      (snapshot) => snapshot.brokers.any((broker) {
        final key = normalizeLookup(broker.name);
        final isAggregate = key == normalizeLookup('통합') || key == 'aggregate';
        return !isAggregate &&
            (broker.offeredShares > 0 ||
                broker.competitionRate != null ||
                broker.proportionalCompetitionRate != null ||
                broker.equalAllocationShares != null ||
                broker.proportionalAllocationShares != null);
      }),
    );
    if (hasBrokerDetail) {
      continue;
    }

    final leadManagers =
        stock.leadManagers
            .map(canonicalBrokerName)
            .where((broker) => broker.trim().isNotEmpty)
            .toSet()
            .toList()
          ..sort();
    if (leadManagers.isEmpty) {
      continue;
    }

    final baseAllocation = allocation ~/ leadManagers.length;
    var remainder = allocation % leadManagers.length;
    final brokers = <IpoBrokerCompetition>[];
    for (final brokerName in leadManagers) {
      final brokerAllocation = baseAllocation + (remainder > 0 ? 1 : 0);
      if (remainder > 0) {
        remainder -= 1;
      }
      final equalShares = brokerAllocation ~/ 2;
      final proportionalShares = brokerAllocation - equalShares;
      brokers.add(
        IpoBrokerCompetition(
          name: brokerName,
          offeredShares: brokerAllocation,
          subscribedShares: (brokerAllocation * rate).round(),
          offerPrice: stock.fundamentals.offerPrice,
          depositRate: null,
          feeKrw: null,
          competitionRate: rate,
          equalCompetitionRate: null,
          proportionalCompetitionRate: rate,
          equalAllocationShares: equalShares,
          proportionalAllocationShares: proportionalShares,
          applicationCount: null,
        ),
      );
    }
    rows.add(
      IpoBrokerSnapshotRow(
        id: stock.id,
        company: stock.company,
        capturedAt: generatedAt.toIso8601String(),
        source: 'estimated_broker_split',
        sourceUrl: latest.sourceUrl,
        brokers: brokers,
      ),
    );
  }
  return rows;
}

List<IpoBrokerSnapshotRow> buildEstimatedBrokerRateOnlyRows(
  List<IpoCompetitionStock> stocks,
  DateTime generatedAt,
) {
  final rows = <IpoBrokerSnapshotRow>[];
  for (final stock in stocks) {
    final latest = stock.latestSnapshot;
    if (latest == null) {
      continue;
    }
    final rate = latest.aggregate.competitionRate;
    if (rate == null || rate <= 0) {
      continue;
    }
    final allocation = stock.fundamentals.publicAllocationShares;
    if (allocation != null && allocation > 0) {
      continue;
    }
    final hasBrokerDetail = stock.snapshots.any(
      (snapshot) => snapshot.brokers.any((broker) {
        final key = normalizeLookup(broker.name);
        final isAggregate = key == normalizeLookup('통합') || key == 'aggregate';
        return !isAggregate &&
            (broker.offeredShares > 0 ||
                broker.competitionRate != null ||
                broker.proportionalCompetitionRate != null ||
                broker.equalAllocationShares != null ||
                broker.proportionalAllocationShares != null);
      }),
    );
    if (hasBrokerDetail) {
      continue;
    }

    final leadManagers =
        stock.leadManagers
            .map(canonicalBrokerName)
            .where((broker) => broker.trim().isNotEmpty)
            .toSet()
            .toList()
          ..sort();
    if (leadManagers.isEmpty) {
      continue;
    }

    rows.add(
      IpoBrokerSnapshotRow(
        id: stock.id,
        company: stock.company,
        capturedAt: generatedAt.toIso8601String(),
        source: 'estimated_broker_rate_only',
        sourceUrl: latest.sourceUrl,
        aggregateCompetitionRate: rate,
        brokers: leadManagers
            .map(
              (brokerName) => IpoBrokerCompetition(
                name: brokerName,
                offeredShares: 0,
                subscribedShares: 0,
                offerPrice: stock.fundamentals.offerPrice,
                depositRate: null,
                feeKrw: null,
                competitionRate: rate,
                equalCompetitionRate: null,
                proportionalCompetitionRate: rate,
                equalAllocationShares: null,
                proportionalAllocationShares: null,
                applicationCount: null,
              ),
            )
            .toList(),
      ),
    );
  }
  return rows;
}

List<IpoCompetitionStock> mergeIdentifierRows(
  List<IpoCompetitionStock> stocks,
  List<IpoIdentifierRow> rows,
) {
  if (rows.isEmpty) {
    return stocks;
  }
  final byId = <String, IpoIdentifierRow>{
    for (final row in rows)
      if (row.id != null) safeId(row.id!): row,
  };
  final bySubscriptionKey = <String, IpoIdentifierRow>{
    for (final row in rows)
      if (row.identifiers.subscriptionKey.trim().isNotEmpty)
        row.identifiers.subscriptionKey: row,
  };
  final byCompany = <String, IpoIdentifierRow>{
    for (final row in rows)
      if (row.company != null) normalizeLookup(row.company!): row,
  };

  return stocks.map((stock) {
    final row =
        byId[safeId(stock.id)] ??
        bySubscriptionKey[stock.identifiers.subscriptionKey] ??
        byCompany[normalizeLookup(stock.company)];
    if (row == null) {
      return stock;
    }
    return IpoCompetitionStock(
      id: stock.id,
      company: stock.company,
      market: stock.market,
      subscriptionStart: stock.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd,
      leadManagers: stock.leadManagers,
      sourceIdentifiers: stock.identifiers.merge(row.identifiers),
      fundamentals: stock.fundamentals,
      outcome: stock.outcome,
      snapshots: stock.snapshots,
    );
  }).toList();
}

List<IpoIdentifierRow> mergeIdentifierRowsByKey(List<IpoIdentifierRow> rows) {
  final byKey = <String, IpoIdentifierRow>{};
  for (final row in rows) {
    final key = row.id != null && row.id!.trim().isNotEmpty
        ? 'id:${safeId(row.id!)}'
        : row.identifiers.subscriptionKey.trim().isNotEmpty
        ? 'sub:${row.identifiers.subscriptionKey}'
        : row.company != null
        ? 'company:${normalizeLookup(row.company!)}'
        : '';
    if (key.isEmpty) {
      continue;
    }
    final existing = byKey[key];
    if (existing == null) {
      byKey[key] = row;
      continue;
    }
    byKey[key] = IpoIdentifierRow(
      id: row.id ?? existing.id,
      company: row.company ?? existing.company,
      identifiers: existing.identifiers.merge(row.identifiers),
    );
  }
  return byKey.values.toList();
}

class IpoIdentifierRow {
  const IpoIdentifierRow({
    required this.id,
    required this.company,
    required this.identifiers,
  });

  final String? id;
  final String? company;
  final IpoStockIdentifiers identifiers;

  factory IpoIdentifierRow.fromJson(Map<String, Object?> json) {
    final nested = json['identifiers'];
    final nestedMap = nested is Map<String, Object?>
        ? nested
        : const <String, Object?>{};
    return IpoIdentifierRow(
      id: readString(json, 'id'),
      company: readString(json, 'company'),
      identifiers: IpoStockIdentifiers.fromJson({
        ...nestedMap,
        'subscriptionKey':
            readString(json, 'subscriptionKey') ?? nestedMap['subscriptionKey'],
        'normalizedCompany':
            readString(json, 'normalizedCompany') ??
            nestedMap['normalizedCompany'],
        'corpCode': readString(json, 'corpCode') ?? nestedMap['corpCode'],
        'stockCode': readString(json, 'stockCode') ?? nestedMap['stockCode'],
        'kindCode': readString(json, 'kindCode') ?? nestedMap['kindCode'],
        'isin': readString(json, 'isin') ?? nestedMap['isin'],
      }),
    );
  }

  Map<String, Object?> toJson() {
    return {'id': id, 'company': company, 'identifiers': identifiers.toJson()};
  }
}

IpoBrokerSnapshotRow? parseIpostockLiveSnapshot({
  required IpoCompetitionStock stock,
  required String capturedAt,
  required String sourceUrl,
  required String html,
}) {
  final text = plainText(html);
  final companyKey = normalizeLookup(stock.company);
  if (!normalizeLookup(text).contains(companyKey)) {
    return null;
  }

  final competitionRate = parseCompetitionRate(
    RegExp(
          r'청약\s*경쟁률[^\d]{0,24}(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:대|:|：)\s*1',
        ).firstMatch(text)?.group(1) ??
        RegExp(
          r'최종\s*청약\s*경쟁[율률][^\d]{0,24}(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:대|:|：)\s*1',
        ).firstMatch(text)?.group(1) ??
        '',
  );
  if (competitionRate <= 0) {
    return null;
  }

  final offerPrice =
      parseCountValue(
        RegExp(
              r'\(확정\)\s*공모가격[^\d]{0,24}(\d+(?:,\d{3})*)\s*원',
            ).firstMatch(text)?.group(1) ??
            '',
      ) ??
      stock.fundamentals.offerPrice;
  final depositRate = parseDepositRate(text);
  final generalShares = parseCountValue(
    RegExp(
          r'일반\s*청약자[^\d]{0,24}(\d+(?:,\d{3})*)\s*주',
        ).firstMatch(text)?.group(1) ??
        '',
  );
  final brokers = parseIpostockBrokerAllocations(text);

  final resolvedBrokers = <IpoBrokerCompetition>[];
  if (brokers.isNotEmpty) {
    for (final entry in brokers.entries) {
      final allocation = entry.value;
      resolvedBrokers.add(
        IpoBrokerCompetition(
          name: entry.key,
          offeredShares: allocation,
          subscribedShares: (allocation * competitionRate).round(),
          offerPrice: offerPrice,
          depositRate: depositRate,
          feeKrw: null,
          competitionRate: competitionRate,
          equalCompetitionRate: null,
          proportionalCompetitionRate: competitionRate,
          equalAllocationShares: (allocation / 2).round(),
          proportionalAllocationShares: (allocation / 2).round(),
        ),
      );
    }
  } else if (generalShares != null && generalShares > 0) {
    resolvedBrokers.add(
      IpoBrokerCompetition(
        name: stock.leadManagers.length == 1 ? stock.leadManagers.first : '통합',
        offeredShares: generalShares,
        subscribedShares: (generalShares * competitionRate).round(),
        offerPrice: offerPrice,
        depositRate: depositRate,
        feeKrw: null,
        competitionRate: competitionRate,
        equalCompetitionRate: null,
        proportionalCompetitionRate: competitionRate,
        equalAllocationShares: (generalShares / 2).round(),
        proportionalAllocationShares: (generalShares / 2).round(),
      ),
    );
  }

  if (resolvedBrokers.isEmpty) {
    return null;
  }

  return IpoBrokerSnapshotRow(
    id: stock.id,
    company: stock.company,
    capturedAt: capturedAt,
    source: 'ipostock_live',
    sourceUrl: sourceUrl,
    brokers: resolvedBrokers,
  );
}

Map<String, int> parseIpostockBrokerAllocations(String text) {
  final result = <String, int>{};
  for (final broker in knownBrokerNames) {
    final pattern = RegExp(
      '${RegExp.escape(broker)}[^\\d]{0,32}(\\d(?:,?\\d){0,14})\\s*주',
      caseSensitive: false,
    );
    final match = pattern.firstMatch(text);
    if (match == null) {
      continue;
    }
    final value = parseCountValue(match.group(1) ?? '');
    if (value == null || value <= 0) {
      continue;
    }
    result[canonicalBrokerName(broker)] = value;
  }
  return result;
}

List<IpoBrokerCompetition> parse38NewsBrokerCompetitions({
  required IpoCompetitionStock stock,
  required String text,
}) {
  final normalized = plainText(text);
  final result = <String, IpoBrokerCompetition>{};
  final pattern = RegExp(
    r'청약\s*경쟁률\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:[:：]|대)\s*1\s*,?\s*비례\s*경쟁률(?:이)?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:[:：]|대)\s*1\s*\(([^\)]+)\)',
    caseSensitive: false,
  );
  for (final match in pattern.allMatches(normalized)) {
    final totalRate = parseCompetitionRate(match.group(1) ?? '');
    final proportionalRate = parseCompetitionRate(match.group(2) ?? '');
    final brokerName = canonicalBrokerName(match.group(3) ?? '');
    final rate = proportionalRate > 0 ? proportionalRate : totalRate;
    if (brokerName.isEmpty || rate <= 0) {
      continue;
    }
    final offered = stock.fundamentals.publicAllocationShares ?? 0;
    result[normalizeLookup(brokerName)] = IpoBrokerCompetition(
      name: brokerName,
      offeredShares: offered,
      subscribedShares: offered > 0 ? (offered * rate).round() : 0,
      offerPrice: stock.fundamentals.offerPrice,
      depositRate: 0.5,
      feeKrw: null,
      competitionRate: totalRate > 0 ? totalRate : rate,
      equalCompetitionRate: null,
      proportionalCompetitionRate: rate,
      equalAllocationShares: offered > 0 ? (offered / 2).round() : null,
      proportionalAllocationShares: offered > 0 ? (offered / 2).round() : null,
    );
  }
  return result.values.toList();
}

List<List<String>> extractHtmlTableRows(String html) {
  final rows = <List<String>>[];
  final rowPattern = RegExp(r'<tr[^>]*>([\s\S]*?)</tr>', caseSensitive: false);
  final cellPattern = RegExp(
    r'<t[dh][^>]*>([\s\S]*?)</t[dh]>',
    caseSensitive: false,
  );
  for (final rowMatch in rowPattern.allMatches(html)) {
    final rowHtml = rowMatch.group(1) ?? '';
    final cells = cellPattern
        .allMatches(rowHtml)
        .map((match) => plainText(match.group(1) ?? ''))
        .where((cell) => cell.isNotEmpty)
        .toList();
    if (cells.isNotEmpty) {
      rows.add(cells);
    }
  }
  return rows;
}

String? extractCommunityDetailPath({
  required String html,
  required String company,
  required RegExp pathPattern,
}) {
  final companyKey = normalizeLookup(company);
  for (final match in pathPattern.allMatches(html)) {
    final path = match.group(1);
    if (path == null) {
      continue;
    }
    final start = mathMax(0, match.start - 320);
    final end = mathMin(html.length, match.end + 320);
    final near = html.substring(start, end);
    if (normalizeLookup(near).contains(companyKey)) {
      return path.replaceAll('&amp;', '&');
    }
  }
  return null;
}

String plainText(String html) {
  return html
      .replaceAll('&nbsp;', ' ')
      .replaceAll('&amp;', '&')
      .replaceAll('&lt;', '<')
      .replaceAll('&gt;', '>')
      .replaceAll('&quot;', '"')
      .replaceAll(RegExp(r'<br\s*/?>', caseSensitive: false), ' / ')
      .replaceAll(RegExp(r'<[^>]+>'), ' ')
      .replaceAll(RegExp(r'\s+'), ' ')
      .trim();
}

int mathMax(int a, int b) => a > b ? a : b;

int mathMin(int a, int b) => a < b ? a : b;

double parseCompetitionRate(String text) {
  final match = RegExp(
    r'(\d+(?:,\d{3})*(?:\.\d+)?)',
  ).firstMatch(text.replaceAll(',', ''));
  if (match == null) {
    return 0;
  }
  return double.tryParse(match.group(1) ?? '') ?? 0;
}

int? parseCountValue(String text) {
  final digits = text.replaceAll(RegExp(r'[^0-9]'), '');
  if (digits.isEmpty) {
    return null;
  }
  return int.tryParse(digits);
}

double? parseDepositRate(String text) {
  final match = RegExp(
    r'청약\s*증거금율[^\d]{0,24}개인\s*(\d+(?:\.\d+)?)\s*%',
  ).firstMatch(text);
  if (match == null) {
    return 0.5;
  }
  final percent = double.tryParse(match.group(1) ?? '');
  if (percent == null || percent <= 0) {
    return 0.5;
  }
  return percent / 100;
}

String canonicalBrokerName(String raw) {
  final key = normalizeLookup(raw);
  if (key == normalizeLookup('엔에이치투자증권') || key == normalizeLookup('NH증권')) {
    return 'NH투자증권';
  }
  if (key == normalizeLookup('케이비증권')) {
    return 'KB증권';
  }
  for (final broker in knownBrokerNames) {
    if (normalizeLookup(broker) == key) {
      return broker;
    }
  }
  return raw.trim();
}

const knownBrokerNames = <String>[
  'KB증권',
  '케이비증권',
  'NH투자증권',
  '엔에이치투자증권',
  '미래에셋증권',
  '한국투자증권',
  '신한투자증권',
  '대신증권',
  '삼성증권',
  '키움증권',
  '하나증권',
  'IBK투자증권',
  '유안타증권',
  '한화투자증권',
  'SK증권',
  'DB증권',
  '교보증권',
  '유진투자증권',
  '현대차증권',
  'BNK투자증권',
  'LS증권',
  'iM증권',
  '다올투자증권',
  '메리츠증권',
  '신영증권',
  '부국증권',
  '유화증권',
  '케이프투자증권',
  '상상인증권',
  '한양증권',
];

class IpoBrokerSnapshotRow {
  const IpoBrokerSnapshotRow({
    required this.id,
    required this.company,
    required this.capturedAt,
    required this.source,
    required this.sourceUrl,
    this.aggregateCompetitionRate,
    required this.brokers,
  });

  final String? id;
  final String? company;
  final String capturedAt;
  final String source;
  final String? sourceUrl;
  final double? aggregateCompetitionRate;
  final List<IpoBrokerCompetition> brokers;

  factory IpoBrokerSnapshotRow.fromJson(Map<String, Object?> json) {
    return IpoBrokerSnapshotRow(
      id: readString(json, 'id'),
      company: readString(json, 'company'),
      capturedAt:
          readString(json, 'capturedAt') ?? DateTime.now().toIso8601String(),
      source: readString(json, 'source') ?? 'broker_snapshot',
      sourceUrl: readString(json, 'sourceUrl'),
      aggregateCompetitionRate: readDouble(json['aggregateCompetitionRate']),
      brokers: readObjectList(
        json['brokers'],
      ).map(IpoBrokerCompetition.fromJson).toList(),
    );
  }

  IpoCompetitionSnapshot toSnapshot() {
    return IpoCompetitionSnapshot(
      capturedAt: capturedAt,
      source: source,
      sourceUrl: sourceUrl,
      aggregateCompetitionRate: aggregateCompetitionRate,
      brokers: brokers,
    );
  }
}

String normalizeLookup(String value) {
  return value.replaceAll(RegExp(r'\s+'), '').toLowerCase();
}

List<String> extractKnownBrokerNames(String text) {
  final normalized = normalizeLookup(plainText(text));
  final found = <String>{};
  for (final broker in knownBrokerNames) {
    if (normalized.contains(normalizeLookup(broker))) {
      found.add(canonicalBrokerName(broker));
    }
  }
  final result = found.where((broker) => broker.trim().isNotEmpty).toList()
    ..sort();
  return result;
}

IpoCompetitionStock? parseIpoKoreaSupplement({
  required IpoCompetitionStock stock,
  required String text,
  required String sourceUrl,
}) {
  final offerPrice = parseLabeledWon(text, ['확정공모가', '확정 공모가']);
  final institutionCompetitionRate = parseLabeledRate(text, [
    '기관 경쟁률',
    '기관경쟁률',
  ]);
  final institutionParticipants = parseLabeledInt(text, [
    '참여건수',
    '참여 건수',
    '참여기관',
    '참여 기관',
  ]);
  final lockupCommitmentRate = parseLabeledPercent(text, [
    '의무보유확약 비율',
    '의무보유확약률',
    '의무보유 확약',
  ]);
  final retailCompetitionRate = parseLabeledRate(text, [
    '일반 청약 경쟁률',
    '일반청약 경쟁률',
    '청약 경쟁률',
  ]);
  final publicAllocationShares = parseLabeledShares(text, [
    '일반투자자 배정',
    '일반 투자자 배정',
    '일반청약자 배정',
    '일반 청약자 배정',
  ]);
  final marketCapKrw = parseLabeledWon(text, ['예상 시가총액', '시가총액']);

  final hasFundamentalSupplement =
      offerPrice != null ||
      institutionCompetitionRate != null ||
      institutionParticipants != null ||
      lockupCommitmentRate != null ||
      publicAllocationShares != null ||
      marketCapKrw != null;
  final hasSnapshotSupplement = retailCompetitionRate != null;
  if (!hasFundamentalSupplement && !hasSnapshotSupplement) {
    return null;
  }

  final snapshots = <IpoCompetitionSnapshot>[];
  if (retailCompetitionRate != null) {
    final offeredShares = publicAllocationShares ?? 0;
    snapshots.add(
      IpoCompetitionSnapshot(
        capturedAt:
            '${stock.subscriptionEnd ?? stock.subscriptionStart ?? DateTime.now().toIso8601String()}T16:00:00+09:00',
        source: 'ipokorea_supplement',
        sourceUrl: sourceUrl,
        aggregateCompetitionRate: retailCompetitionRate,
        brokers: [
          IpoBrokerCompetition(
            name: '통합',
            offeredShares: offeredShares,
            subscribedShares: offeredShares <= 0
                ? 0
                : (offeredShares * retailCompetitionRate).round(),
            offerPrice: offerPrice,
            depositRate: null,
            feeKrw: null,
            competitionRate: retailCompetitionRate,
            equalCompetitionRate: null,
            proportionalCompetitionRate: null,
          ),
        ],
      ),
    );
  }

  return IpoCompetitionStock(
    id: stock.id,
    company: stock.company,
    market: stock.market,
    subscriptionStart: stock.subscriptionStart,
    subscriptionEnd: stock.subscriptionEnd,
    leadManagers: const [],
    sourceIdentifiers: stock.identifiers,
    fundamentals: IpoFundamentals(
      offerPrice: offerPrice,
      priceBandMin: null,
      priceBandMax: null,
      institutionCompetitionRate: institutionCompetitionRate,
      institutionParticipants: institutionParticipants,
      lockupCommitmentRate: lockupCommitmentRate,
      floatRate: null,
      marketCapKrw: marketCapKrw,
      publicAllocationShares: publicAllocationShares,
    ),
    outcome: null,
    snapshots: snapshots,
  );
}

double? parseLabeledRate(String text, List<String> labels) {
  return parseLabeledDouble(text, labels);
}

double? parseLabeledPercent(String text, List<String> labels) {
  final parsed = parseLabeledDouble(text, labels);
  if (parsed == null) {
    return null;
  }
  return parsed > 1 ? parsed / 100 : parsed;
}

int? parseLabeledWon(String text, List<String> labels) {
  final normalized = normalizeSourceText(text);
  for (final label in labels) {
    final match = RegExp(
      '${RegExp.escape(label)}[^0-9]{0,40}([0-9][0-9,]*(?:\\.[0-9]+)?)\\s*(조|억|만|원)?',
      caseSensitive: false,
    ).firstMatch(normalized);
    if (match == null) {
      continue;
    }
    final value = parseNumericToken(match.group(1));
    if (value == null) {
      continue;
    }
    final unit = match.group(2) ?? '';
    if (unit == '조') {
      return (value * 1000000000000).round();
    }
    if (unit == '억') {
      return (value * 100000000).round();
    }
    if (unit == '만') {
      return (value * 10000).round();
    }
    return value.round();
  }
  return null;
}

int? parseLabeledShares(String text, List<String> labels) {
  final normalized = normalizeSourceText(text);
  for (final label in labels) {
    final match = RegExp(
      '${RegExp.escape(label)}[^0-9]{0,40}([0-9][0-9,]*(?:\\.[0-9]+)?)\\s*(억|만|주)?',
      caseSensitive: false,
    ).firstMatch(normalized);
    if (match == null) {
      continue;
    }
    final value = parseNumericToken(match.group(1));
    if (value == null) {
      continue;
    }
    final unit = match.group(2) ?? '';
    if (unit == '억') {
      return (value * 100000000).round();
    }
    if (unit == '만') {
      return (value * 10000).round();
    }
    return value.round();
  }
  return null;
}

int? parseLabeledInt(String text, List<String> labels) {
  final parsed = parseLabeledDouble(text, labels);
  return parsed?.round();
}

double? parseLabeledDouble(String text, List<String> labels) {
  final normalized = normalizeSourceText(text);
  for (final label in labels) {
    final match = RegExp(
      '${RegExp.escape(label)}[^0-9]{0,40}([0-9][0-9,]*(?:\\.[0-9]+)?)',
      caseSensitive: false,
    ).firstMatch(normalized);
    final parsed = parseNumericToken(match?.group(1));
    if (parsed != null) {
      return parsed;
    }
  }
  return null;
}

double? parseNumericToken(String? value) {
  if (value == null) {
    return null;
  }
  return double.tryParse(value.replaceAll(',', '').trim());
}

String normalizeSourceText(String value) {
  return value.replaceAll('&nbsp;', ' ').replaceAll(RegExp(r'\s+'), ' ').trim();
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
      institutionCompetitionRate: readDouble(
        json['institutionCompetitionRate'],
      ),
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
      institutionParticipants:
          other.institutionParticipants ?? institutionParticipants,
      lockupCommitmentRate: other.lockupCommitmentRate ?? lockupCommitmentRate,
      floatRate: other.floatRate ?? floatRate,
      marketCapKrw: other.marketCapKrw ?? marketCapKrw,
      publicAllocationShares:
          other.publicAllocationShares ?? publicAllocationShares,
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
      aggregateCompetitionRate:
          readDouble(json['aggregateCompetitionRate']) ??
          (aggregate is Map<String, Object?>
              ? readDouble(aggregate['competitionRate'])
              : null),
      brokers: readObjectList(
        json['brokers'],
      ).map(IpoBrokerCompetition.fromJson).toList(),
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
      competitionRate:
          aggregateCompetitionRate ??
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
      competitionRate:
          readDouble(json['competitionRate']) ??
          (offeredShares <= 0 ? null : subscribedShares / offeredShares),
      equalCompetitionRate: readDouble(json['equalCompetitionRate']),
      proportionalCompetitionRate: readDouble(
        json['proportionalCompetitionRate'],
      ),
      equalAllocationShares:
          readOptionalInt(json['equalAllocationShares']) ??
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

IpoAnalysis analyzeStock(
  IpoCompetitionStock stock, {
  IpoAnalysisCalibration? calibration,
}) {
  final effectiveCalibration = calibration ?? _analysisCalibration;
  final isSpac = isSpacStock(stock);
  final latestRate = stock.latestSnapshot?.aggregate.competitionRate;
  final competitionScore = scoreCompetition(latestRate);
  final institutionScore = scoreInstitutionDemand(stock.fundamentals);
  final spacMomentumScore = isSpac ? scoreSpacMomentum(stock) : 0;
  final spacVolatilityScore = isSpac ? scoreSpacVolatility(stock) : 0;
  final lockupScore = isSpac
      ? 0
      : scoreLockup(stock.fundamentals.lockupCommitmentRate);
  final floatScore = isSpac ? 0 : scoreFloat(stock.fundamentals.floatRate);
  final pricingScore = scorePricing(stock.fundamentals);
  final marketScore = scoreMarket(stock.market);
  final managerScore = scoreLeadManagers(stock.leadManagers);
  final recencyScore = scoreRecency(stock.subscriptionEnd);
  final dataScore = scoreDataCompleteness(stock);
  final factors = <String, int>{
    'competition': competitionScore,
    'institutionDemand': institutionScore,
    if (isSpac) ...{
      'spacMomentum': spacMomentumScore,
      'spacVolatility': spacVolatilityScore,
    } else ...{
      'lockupCommitment': lockupScore,
      'floatRate': floatScore,
    },
    'pricing': pricingScore,
    'market': marketScore,
    'leadManagers': managerScore,
    'recency': recencyScore,
    'dataCompleteness': dataScore,
  };
  final total = clampInt(
    factors.values.fold<int>(0, (sum, value) => sum + value),
    0,
    100,
  );
  final confidence = confidenceFor(stock);
  final expectedReturnProfile = expectedReturnProfileFor(
    stock: stock,
    score: total,
    competitionRate: latestRate,
    confidence: confidence,
    calibration: effectiveCalibration,
  );
  final expectedGainRate = expectedReturnProfile.expectedListingGainRate;
  final offerPrice = stock.latestOfferPrice;
  final expectedAllocatedShares = expectedAllocatedSharesFor(
    stock: stock,
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
      factors: factors,
    ),
    expectedReturn: IpoExpectedReturn(
      expectedListingGainRate: expectedGainRate,
      bearCaseListingGainRate: expectedReturnProfile.bearCaseListingGainRate,
      baseCaseListingGainRate: expectedReturnProfile.baseCaseListingGainRate,
      bullCaseListingGainRate: expectedReturnProfile.bullCaseListingGainRate,
      expectedAllocatedShares: expectedAllocatedShares,
      expectedProfitKrw: expectedProfit,
      assumptions: {
        'offerPrice': offerPrice,
        'competitionRate': latestRate,
        'feeKrw': 2000,
        ...expectedReturnProfile.assumptions,
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
    methodVersion: 'ipo-score-v4',
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

class IpoAnalysisCalibration {
  const IpoAnalysisCalibration({this.spac});

  final IpoSpacCalibration? spac;

  bool get hasSpac => spac != null && spac!.sampleCount > 0;

  Map<String, Object?> toJson() {
    return {'spac': spac?.toJson()};
  }
}

class IpoSpacCalibration {
  const IpoSpacCalibration({
    required this.sampleCount,
    required this.averageReferenceError,
    required this.medianReferenceError,
    required this.dampedAdjustment,
    required this.maxAdjustment,
    required this.byCompetitionBucket,
  });

  final int sampleCount;
  final double? averageReferenceError;
  final double? medianReferenceError;
  final double dampedAdjustment;
  final double maxAdjustment;
  final Map<String, IpoBucketCalibration> byCompetitionBucket;

  IpoBucketCalibration? bucketFor(double? competitionRate) {
    if (competitionRate == null) {
      return null;
    }
    return byCompetitionBucket[competitionBucketFor(competitionRate)];
  }

  Map<String, Object?> toJson() {
    return {
      'sampleCount': sampleCount,
      'averageReferenceError': averageReferenceError,
      'medianReferenceError': medianReferenceError,
      'dampedAdjustment': dampedAdjustment,
      'maxAdjustment': maxAdjustment,
      'byCompetitionBucket': byCompetitionBucket.map(
        (key, value) => MapEntry(key, value.toJson()),
      ),
    };
  }
}

class IpoBucketCalibration {
  const IpoBucketCalibration({
    required this.bucket,
    required this.sampleCount,
    required this.averageReferenceError,
    required this.medianReferenceError,
    required this.dampedAdjustment,
  });

  final String bucket;
  final int sampleCount;
  final double? averageReferenceError;
  final double? medianReferenceError;
  final double dampedAdjustment;

  Map<String, Object?> toJson() {
    return {
      'bucket': bucket,
      'sampleCount': sampleCount,
      'averageReferenceError': averageReferenceError,
      'medianReferenceError': medianReferenceError,
      'dampedAdjustment': dampedAdjustment,
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
          estimatedDepositForOneProportionalShare?.round(),
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

class IpoExpectedReturnProfile {
  const IpoExpectedReturnProfile({
    required this.expectedListingGainRate,
    required this.bearCaseListingGainRate,
    required this.baseCaseListingGainRate,
    required this.bullCaseListingGainRate,
    required this.assumptions,
  });

  final double expectedListingGainRate;
  final double bearCaseListingGainRate;
  final double baseCaseListingGainRate;
  final double bullCaseListingGainRate;
  final Map<String, Object?> assumptions;
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
    return 0;
  }
  if (rate >= 1500) {
    return 24;
  }
  if (rate >= 1000) {
    return 20;
  }
  if (rate >= 700) {
    return 15;
  }
  if (rate >= 300) {
    return 8;
  }
  if (rate >= 100) {
    return 3;
  }
  return 1;
}

int scoreLockup(double? rate) {
  if (rate == null) {
    return 0;
  }
  if (rate >= 0.5) {
    return 18;
  }
  if (rate >= 0.3) {
    return 14;
  }
  if (rate >= 0.15) {
    return 9;
  }
  if (rate >= 0.05) {
    return 4;
  }
  return 0;
}

int scoreFloat(double? rate) {
  if (rate == null) {
    return 0;
  }
  if (rate <= 0.2) {
    return 12;
  }
  if (rate <= 0.3) {
    return 9;
  }
  if (rate <= 0.4) {
    return 6;
  }
  if (rate <= 0.5) {
    return 3;
  }
  return 0;
}

int scoreSpacMomentum(IpoCompetitionStock stock) {
  final retailRate = stock.latestSnapshot?.aggregate.competitionRate;
  final proportionalRate = maxProportionalCompetitionRate(stock);
  final institutionRate = stock.fundamentals.institutionCompetitionRate;
  var score = 0;
  if (retailRate != null) {
    if (retailRate >= 2000) {
      score += 7;
    } else if (retailRate >= 1500) {
      score += 6;
    } else if (retailRate >= 1000) {
      score += 4;
    } else if (retailRate >= 500) {
      score += 2;
    }
  }
  if (proportionalRate != null) {
    if (proportionalRate >= 3500) {
      score += 6;
    } else if (proportionalRate >= 2500) {
      score += 5;
    } else if (proportionalRate >= 1500) {
      score += 3;
    } else if (proportionalRate >= 800) {
      score += 1;
    }
  }
  if (institutionRate != null) {
    if (institutionRate >= 1200) {
      score += 4;
    } else if (institutionRate >= 800) {
      score += 3;
    } else if (institutionRate >= 400) {
      score += 1;
    }
  }
  final offerPrice = stock.latestOfferPrice;
  if (offerPrice != null && offerPrice <= 2500) {
    score += 1;
  }
  return clampInt(score, 0, 16);
}

int scoreSpacVolatility(IpoCompetitionStock stock) {
  final lockupRate = stock.fundamentals.lockupCommitmentRate;
  final retailRate = stock.latestSnapshot?.aggregate.competitionRate;
  var score = 2;
  if (lockupRate == null) {
    score += 1;
  } else if (lockupRate <= 0.01) {
    score += 2;
  } else if (lockupRate <= 0.05) {
    score += 1;
  }
  if (retailRate != null && retailRate >= 1500) {
    score += 1;
  }
  return clampInt(score, 0, 4);
}

int scorePricing(IpoFundamentals fundamentals) {
  final offer = fundamentals.offerPrice;
  final min = fundamentals.priceBandMin;
  final max = fundamentals.priceBandMax;
  if (offer == null || min == null || max == null || max <= min) {
    return 3;
  }
  final position = (offer - min) / (max - min);
  if (position > 1.0) {
    return 2;
  }
  if (position >= 0.85) {
    return 5;
  }
  if (position >= 0.45) {
    return 7;
  }
  return 10;
}

int scoreCompetition(double? rate) {
  if (rate == null) {
    return 0;
  }
  if (rate >= 2500) {
    return 14;
  }
  if (rate >= 1500) {
    return 16;
  }
  if (rate >= 800) {
    return 13;
  }
  if (rate >= 400) {
    return 9;
  }
  if (rate >= 150) {
    return 5;
  }
  if (rate >= 50) {
    return 2;
  }
  return 0;
}

int scoreMarket(String market) {
  final normalized = market.toUpperCase();
  if (normalized.contains('KOSPI')) {
    return 6;
  }
  if (normalized.contains('KOSDAQ')) {
    return 5;
  }
  return 3;
}

int scoreLeadManagers(List<String> managers) {
  if (managers.length >= 4) {
    return 6;
  }
  if (managers.length >= 2) {
    return 5;
  }
  if (managers.length == 1) {
    return 3;
  }
  return 0;
}

int scoreRecency(String? subscriptionEnd) {
  final end = parseDate(subscriptionEnd);
  if (end == null) {
    return 2;
  }
  final now = DateTime.now();
  final days = end.difference(DateTime(now.year, now.month, now.day)).inDays;
  if (days >= 0 && days <= 14) {
    return 4;
  }
  if (days > 14) {
    return 3;
  }
  if (days >= -30) {
    return 2;
  }
  return 0;
}

int scoreDataCompleteness(IpoCompetitionStock stock) {
  var score = 0;
  if (stock.snapshots.isNotEmpty) {
    score += 2;
  }
  if (stock.leadManagers.isNotEmpty) {
    score += 1;
  }
  if (stock.market.trim().isNotEmpty) {
    score += 1;
  }
  if (stock.subscriptionStart != null && stock.subscriptionEnd != null) {
    score += 1;
  }
  if (stock.fundamentals.offerPrice != null) {
    score += 1;
  }
  if (stock.fundamentals.institutionCompetitionRate != null) {
    score += 1;
  }
  if (stock.fundamentals.lockupCommitmentRate != null) {
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

IpoExpectedReturnProfile expectedReturnProfileFor({
  required IpoCompetitionStock stock,
  required int score,
  required double? competitionRate,
  required double confidence,
  required IpoAnalysisCalibration calibration,
}) {
  if (isSpacStock(stock)) {
    return spacExpectedReturnProfileFor(
      stock: stock,
      score: score,
      competitionRate: competitionRate,
      confidence: confidence,
      calibration: calibration.spac,
    );
  }
  final expected = generalExpectedGainRateFor(
    score: score,
    competitionRate: competitionRate,
    confidence: confidence,
  );
  return IpoExpectedReturnProfile(
    expectedListingGainRate: expected,
    bearCaseListingGainRate: expected - 0.22,
    baseCaseListingGainRate: expected,
    bullCaseListingGainRate: expected + 0.35,
    assumptions: const {'method': 'ipo_score_v2_general_rule_based'},
  );
}

double generalExpectedGainRateFor({
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

IpoExpectedReturnProfile spacExpectedReturnProfileFor({
  required IpoCompetitionStock stock,
  required int score,
  required double? competitionRate,
  required double confidence,
  required IpoSpacCalibration? calibration,
}) {
  final proportionalRate = maxProportionalCompetitionRate(stock);
  final institutionRate = stock.fundamentals.institutionCompetitionRate;
  final lockupRate = stock.fundamentals.lockupCommitmentRate;
  final offerPrice = stock.latestOfferPrice;
  final retailBoost = competitionRate == null
      ? 0.0
      : clampDouble((competitionRate - 500) / 3000, -0.06, 0.28);
  final proportionalBoost = proportionalRate == null
      ? 0.0
      : clampDouble((proportionalRate - 1000) / 7000, 0, 0.26);
  final institutionBoost = institutionRate == null
      ? 0.0
      : clampDouble((institutionRate - 700) / 2500, 0, 0.16);
  final fixedPriceBoost = offerPrice != null && offerPrice <= 2500 ? 0.08 : 0.0;
  final scarcityBoost =
      (stock.fundamentals.publicAllocationShares ?? 0) >= 1000000 ? 0.03 : 0.0;
  final lowLockupVolatility = lockupRate != null && lockupRate <= 0.01;
  final lowLockupBaseBoost = lowLockupVolatility ? 0.05 : 0.0;
  final scoreComponent = clampDouble((score - 50) / 220, -0.08, 0.16);
  final rawBase =
      0.1 +
      retailBoost +
      proportionalBoost +
      institutionBoost +
      fixedPriceBoost +
      scarcityBoost +
      lowLockupBaseBoost +
      scoreComponent;
  final confidenceFactor = 0.78 + confidence * 0.22;
  final uncalibratedBase = clampDouble(rawBase * confidenceFactor, -0.05, 1.6);
  final bucketCalibration = calibration?.bucketFor(competitionRate);
  final calibrationAdjustment = clampDouble(
    bucketCalibration?.dampedAdjustment ?? calibration?.dampedAdjustment ?? 0,
    -(calibration?.maxAdjustment ?? 0.22),
    calibration?.maxAdjustment ?? 0.22,
  );
  final base = clampDouble(
    uncalibratedBase + calibrationAdjustment,
    -0.05,
    1.6,
  );
  final volatilityPremium =
      0.28 +
      (lowLockupVolatility ? 0.42 : 0.12) +
      (proportionalRate != null && proportionalRate >= 3000 ? 0.22 : 0.0);
  final bear = clampDouble(
    base - (lowLockupVolatility ? 0.65 : 0.42),
    -0.2,
    1.0,
  );
  final bull = clampDouble(base + volatilityPremium, 0, 2.8);
  return IpoExpectedReturnProfile(
    expectedListingGainRate: base,
    bearCaseListingGainRate: bear,
    baseCaseListingGainRate: base,
    bullCaseListingGainRate: bull,
    assumptions: {
      'method': 'ipo_score_v4_spac_listing_day_momentum',
      'spacModel': true,
      'proportionalCompetitionRate': proportionalRate,
      'institutionCompetitionRate': institutionRate,
      'lowLockupVolatility': lowLockupVolatility,
      'uncalibratedBaseGainRate': uncalibratedBase,
      'calibrationApplied': calibrationAdjustment,
      'calibrationSampleCount': calibration?.sampleCount ?? 0,
      'calibrationCompetitionBucket': bucketCalibration?.bucket,
    },
  );
}

bool isSpacStock(IpoCompetitionStock stock) {
  final normalizedCompany = normalizeLookup(stock.company);
  return normalizedCompany.contains('스팩') || normalizedCompany.contains('spac');
}

double? maxProportionalCompetitionRate(IpoCompetitionStock stock) {
  double? maxRate;
  for (final snapshot in stock.snapshots) {
    for (final broker in snapshot.brokers) {
      final rate = broker.proportionalCompetitionRate;
      if (rate != null && rate > 0 && (maxRate == null || rate > maxRate)) {
        maxRate = rate;
      }
    }
  }
  return maxRate;
}

Map<String, double> expectedAllocatedSharesFor({
  required IpoCompetitionStock stock,
  required int? offerPrice,
  required double? competitionRate,
}) {
  final price = offerPrice ?? 30000;
  final rate = competitionRate ?? 800;
  final equalShares = bestEqualExpectedSharesPerAccount(stock) ?? 0;
  double sharesFor(int amount) {
    final requestedShares = amount / price;
    final proportionalShares = clampDouble(requestedShares / rate, 0, 100);
    return clampDouble(equalShares + proportionalShares, 0, 200);
  }

  return {
    'minimumSubscription': sharesFor(price * 10),
    'oneMillionKrw': sharesFor(1000000),
    'fiveMillionKrw': sharesFor(5000000),
  };
}

double? bestEqualExpectedSharesPerAccount(IpoCompetitionStock stock) {
  double? best;
  for (final snapshot in stock.snapshots) {
    for (final broker in snapshot.brokers) {
      final expected = broker.equalExpectedSharesPerAccount;
      if (expected != null &&
          expected > 0 &&
          (best == null || expected > best)) {
        best = expected;
      }
    }
  }
  return best;
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
  if (isSpacStock(stock)) {
    final proportionalRate = maxProportionalCompetitionRate(stock);
    if (proportionalRate != null) {
      reasons.add(
        '스팩 전용 보정으로 비례 경쟁률 ${roundDouble(proportionalRate, 2)}대 1을 상장일 수급 요인에 반영했습니다.',
      );
    } else {
      reasons.add('스팩 전용 보정으로 일반 IPO 확약률 대신 상장일 수급 요인을 반영했습니다.');
    }
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
  if (isSpacStock(stock)) {
    warnings.add('스팩의 낮은 기관 확약률은 일반 IPO 안정성 감점이 아니라 상장일 변동성 위험으로 해석합니다.');
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
  final scores =
      brokerMetrics.values.map((broker) {
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
        final hasPositiveApplicationCount =
            broker.applicationCount != null && broker.applicationCount! > 0;
        final quality =
            hasPositiveApplicationCount &&
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
      }).toList()..sort((a, b) {
        final byEqual = b.equalScore.compareTo(a.equalScore);
        if (byEqual != 0) {
          return byEqual;
        }
        return b.proportionalScore.compareTo(a.proportionalScore);
      });
  return scores;
}

Map<String, Object?> buildBrokerMetricsMissingReport({
  required DateTime generatedAt,
  required List<IpoCompetitionStock> stocks,
}) {
  final rows = <Map<String, Object?>>[];
  final reasonCounts = <String, int>{};
  final today = DateTime(generatedAt.year, generatedAt.month, generatedAt.day);

  bool isCompleted(IpoCompetitionStock stock) {
    final end =
        parseDate(stock.subscriptionEnd) ?? parseDate(stock.subscriptionStart);
    return end != null && !end.isAfter(today);
  }

  bool hasBrokerDetail(IpoCompetitionStock stock) {
    return stock.snapshots.any(
      (snapshot) => snapshot.brokers.any((broker) {
        final key = normalizeLookup(broker.name);
        final isAggregate = key == normalizeLookup('통합') || key == 'aggregate';
        return !isAggregate &&
            (broker.offeredShares > 0 ||
                broker.competitionRate != null ||
                broker.proportionalCompetitionRate != null ||
                broker.equalAllocationShares != null ||
                broker.proportionalAllocationShares != null);
      }),
    );
  }

  for (final stock in stocks.where(isCompleted)) {
    if (hasBrokerDetail(stock)) {
      continue;
    }
    final latest = stock.latestSnapshot;
    final reasons = <String>[];
    if (latest == null) {
      reasons.add('no_snapshot');
    }
    if (latest?.aggregate.competitionRate == null) {
      reasons.add('no_retail_competition_rate');
    }
    if (stock.fundamentals.publicAllocationShares == null) {
      reasons.add('no_public_allocation');
    }
    if (stock.leadManagers.isEmpty) {
      reasons.add('no_lead_manager');
    }
    final reasonKey = reasons.isEmpty ? 'unknown' : reasons.join('+');
    reasonCounts[reasonKey] = (reasonCounts[reasonKey] ?? 0) + 1;
    rows.add({
      'id': stock.id,
      'company': stock.company,
      'subscriptionStart': stock.subscriptionStart,
      'subscriptionEnd': stock.subscriptionEnd,
      'leadManagers': stock.leadManagers,
      'reason': reasonKey,
      'retailCompetitionRate': latest?.aggregate.competitionRate,
      'publicAllocationShares': stock.fundamentals.publicAllocationShares,
      'latestSnapshotSource': latest?.source,
      'latestSnapshotSourceUrl': latest?.sourceUrl,
    });
  }

  rows.sort((a, b) {
    final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
      '${a['subscriptionStart'] ?? ''}',
    );
    if (byDate != 0) {
      return byDate;
    }
    return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
  });

  return {
    'schemaVersion': schemaVersion,
    'generatedAt': generatedAt.toIso8601String(),
    'totalMissing': rows.length,
    'reasonCounts': reasonCounts,
    'missingBrokerMetrics': rows,
  };
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

IpoAnalysisCalibration buildAnalysisCalibration(
  List<IpoCompetitionStock> stocks,
) {
  final rows = stocks
      .where((stock) => isSpacStock(stock))
      .map((stock) {
        final referenceReturn = referenceReturnRateForBacktest(stock);
        if (referenceReturn == null) {
          return null;
        }
        final rawAnalysis = analyzeStock(
          stock,
          calibration: const IpoAnalysisCalibration(),
        );
        final competitionRate = stock.latestSnapshot?.aggregate.competitionRate;
        return {
          'id': safeId(stock.id),
          'competitionBucket': competitionBucketFor(competitionRate),
          'referenceError': roundDouble(
            referenceReturn -
                rawAnalysis.expectedReturn.expectedListingGainRate,
            4,
          ),
        };
      })
      .whereType<Map<String, Object?>>()
      .toList();
  if (rows.isEmpty) {
    return const IpoAnalysisCalibration();
  }
  final errors = rows
      .map((row) => row['referenceError'])
      .whereType<double>()
      .toList();
  final bucketed = <String, List<double>>{};
  for (final row in rows) {
    final bucket = '${row['competitionBucket']}';
    final error = row['referenceError'];
    if (error is! double) {
      continue;
    }
    bucketed.putIfAbsent(bucket, () => []).add(error);
  }
  final byCompetitionBucket = bucketed.map((bucket, values) {
    return MapEntry(
      bucket,
      IpoBucketCalibration(
        bucket: bucket,
        sampleCount: values.length,
        averageReferenceError: average(values),
        medianReferenceError: median(values),
        dampedAdjustment: dampedCalibrationAdjustment(values),
      ),
    );
  });
  return IpoAnalysisCalibration(
    spac: IpoSpacCalibration(
      sampleCount: rows.length,
      averageReferenceError: average(errors),
      medianReferenceError: median(errors),
      dampedAdjustment: dampedCalibrationAdjustment(errors),
      maxAdjustment: 0.22,
      byCompetitionBucket: byCompetitionBucket,
    ),
  );
}

Map<String, Object?> buildCalibrationReport({
  required List<IpoCompetitionStock> stocks,
  required DateTime generatedAt,
  required IpoAnalysisCalibration calibration,
}) {
  final spacRows = stocks
      .where((stock) => isSpacStock(stock))
      .map((stock) {
        final referenceReturn = referenceReturnRateForBacktest(stock);
        if (referenceReturn == null) {
          return null;
        }
        final rawAnalysis = analyzeStock(
          stock,
          calibration: const IpoAnalysisCalibration(),
        );
        final competitionRate = stock.latestSnapshot?.aggregate.competitionRate;
        return {
          'id': safeId(stock.id),
          'company': stock.company,
          'competitionRate': competitionRate,
          'competitionBucket': competitionBucketFor(competitionRate),
          'expectedListingGainRateRaw': roundDouble(
            rawAnalysis.expectedReturn.expectedListingGainRate,
            4,
          ),
          'referenceReturnRate': referenceReturn,
          'referenceError': roundDouble(
            referenceReturn -
                rawAnalysis.expectedReturn.expectedListingGainRate,
            4,
          ),
        };
      })
      .whereType<Map<String, Object?>>()
      .toList();
  spacRows.sort((a, b) => '${b['id']}'.compareTo('${a['id']}'));
  return {
    'schemaVersion': schemaVersion,
    'generatedAt': generatedAt.toIso8601String(),
    'methodVersion': 'ipo-score-v4',
    'calibration': calibration.toJson(),
    'spacHistoricalRows': spacRows,
    'note':
        'Calibration is intentionally weak and sample-size damped. It is used only as a lightweight adjustment layer.',
  };
}

double dampedCalibrationAdjustment(List<double> errors) {
  final avg = average(errors);
  if (avg == null) {
    return 0;
  }
  final sampleWeight = clampDouble(errors.length / 5, 0.15, 0.6);
  return roundDouble(avg * sampleWeight, 4);
}

double? referenceReturnRateForBacktest(IpoCompetitionStock stock) {
  final outcome = stock.outcome;
  if (outcome == null) {
    return null;
  }
  if (isSpacStock(stock)) {
    final weighted = weightedAverage([
      (outcome.openReturnRate, 0.45),
      (outcome.highReturnRate, 0.35),
      (outcome.closeReturnRate, 0.20),
    ]);
    if (weighted != null) {
      return weighted;
    }
  }
  return outcome.closeReturnRate ??
      outcome.openReturnRate ??
      outcome.highReturnRate;
}

String competitionBucketFor(double? competitionRate) {
  if (competitionRate == null) {
    return 'unknown';
  }
  if (competitionRate >= 2000) {
    return '2000+';
  }
  if (competitionRate >= 1500) {
    return '1500-1999';
  }
  if (competitionRate >= 1000) {
    return '1000-1499';
  }
  if (competitionRate >= 500) {
    return '500-999';
  }
  return '0-499';
}

double? weightedAverage(List<(double?, double)> values) {
  var totalWeight = 0.0;
  var total = 0.0;
  for (final (value, weight) in values) {
    if (value == null) {
      continue;
    }
    total += value * weight;
    totalWeight += weight;
  }
  if (totalWeight == 0) {
    return null;
  }
  return roundDouble(total / totalWeight, 4);
}

Map<String, Object?> buildBacktestReport(
  List<IpoCompetitionStock> stocks,
  DateTime generatedAt,
) {
  final rows =
      stocks
          .map((stock) {
            final outcome = stock.outcome;
            if (outcome?.closeReturnRate == null) {
              return null;
            }
            final analysis = analyzeStock(
              stock,
              calibration: const IpoAnalysisCalibration(),
            );
            return <String, Object?>{
              'id': safeId(stock.id),
              'company': stock.company,
              'isSpac': isSpacStock(stock),
              'score': analysis.score.overall,
              'grade': analysis.score.grade,
              'confidence': roundDouble(analysis.score.confidence, 2),
              'expectedListingGainRate': roundDouble(
                analysis.expectedReturn.expectedListingGainRate,
                4,
              ),
              'openReturnRate': outcome?.openReturnRate,
              'highReturnRate': outcome?.highReturnRate,
              'closeReturnRate': outcome?.closeReturnRate,
              'referenceReturnRate': referenceReturnRateForBacktest(stock),
              'outcomeSourceUrl': outcome?.sourceUrl,
              'errorCloseVsExpected': outcome?.closeReturnRate == null
                  ? null
                  : roundDouble(
                      outcome!.closeReturnRate! -
                          analysis.expectedReturn.expectedListingGainRate,
                      4,
                    ),
              'errorReferenceVsExpected':
                  referenceReturnRateForBacktest(stock) == null
                  ? null
                  : roundDouble(
                      referenceReturnRateForBacktest(stock)! -
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
    'methodVersion': 'ipo-score-v4',
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
  final referenceReturns = rows
      .map((row) => row['referenceReturnRate'])
      .whereType<double>()
      .toList();
  final referenceErrors = rows
      .map((row) => row['errorReferenceVsExpected'])
      .whereType<double>()
      .toList();
  return {
    'averageCloseReturnRate': average(closes),
    'medianCloseReturnRate': median(closes),
    'averageErrorCloseVsExpected': average(errors),
    'medianErrorCloseVsExpected': median(errors),
    'averageReferenceReturnRate': average(referenceReturns),
    'medianReferenceReturnRate': median(referenceReturns),
    'averageErrorReferenceVsExpected': average(referenceErrors),
    'medianErrorReferenceVsExpected': median(referenceErrors),
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
    final referenceErrors = bucketRows
        .map((row) => row['errorReferenceVsExpected'])
        .whereType<double>()
        .toList();
    return MapEntry(bucket, {
      'sampleCount': bucketRows.length,
      'averageCloseReturnRate': average(closes),
      'medianCloseReturnRate': median(closes),
      'averageErrorCloseVsExpected': average(errors),
      'averageErrorReferenceVsExpected': average(referenceErrors),
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

Future<Map<String, Object?>> httpPostJson(
  Uri uri,
  Map<String, String> body,
) async {
  final client = HttpClient();
  try {
    final request = await client.postUrl(uri);
    request.headers.set(HttpHeaders.acceptHeader, 'application/json');
    request.headers.set(
      HttpHeaders.contentTypeHeader,
      'application/x-www-form-urlencoded; charset=UTF-8',
    );
    request.headers.set(HttpHeaders.userAgentHeader, 'Mozilla/5.0');
    request.write(
      body.entries
          .map(
            (entry) =>
                '${Uri.encodeQueryComponent(entry.key)}=${Uri.encodeQueryComponent(entry.value)}',
          )
          .join('&'),
    );
    final response = await request.close();
    final responseBody = await utf8.decodeStream(response);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw HttpException(
        'HTTP ${response.statusCode}: $responseBody',
        uri: uri,
      );
    }
    final decoded = jsonDecode(responseBody);
    if (decoded is! Map<String, Object?>) {
      return const {};
    }
    return decoded;
  } finally {
    client.close(force: true);
  }
}

Future<String?> httpPostText(Uri uri, Map<String, String> body) async {
  final client = HttpClient();
  try {
    final request = await client.postUrl(uri);
    request.headers.set(
      HttpHeaders.contentTypeHeader,
      'application/x-www-form-urlencoded; charset=UTF-8',
    );
    request.headers.set(HttpHeaders.userAgentHeader, 'Mozilla/5.0');
    request.write(
      body.entries
          .map(
            (entry) =>
                '${Uri.encodeQueryComponent(entry.key)}=${Uri.encodeQueryComponent(entry.value)}',
          )
          .join('&'),
    );
    final response = await request.close();
    final responseBody = await utf8.decodeStream(response);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      return null;
    }
    return responseBody;
  } catch (_) {
    return null;
  } finally {
    client.close(force: true);
  }
}

Future<String?> httpGetFirstText(List<String> urls) async {
  final expanded = <String>[];
  for (final rawUrl in urls) {
    expanded.add(rawUrl);
    if (rawUrl.startsWith('http://') || rawUrl.startsWith('https://')) {
      final noScheme = rawUrl.replaceFirst(RegExp(r'^https?://'), '');
      expanded.add('https://r.jina.ai/http://$noScheme');
    }
  }

  for (final rawUrl in expanded) {
    final client = HttpClient();
    try {
      final uri = Uri.parse(rawUrl);
      final request = await client.getUrl(uri);
      request.headers.set(HttpHeaders.userAgentHeader, 'Mozilla/5.0');
      request.headers.set(HttpHeaders.acceptHeader, 'text/html,*/*');
      final response = await request.close();
      final body = await utf8.decodeStream(response);
      if (response.statusCode >= 200 &&
          response.statusCode < 300 &&
          body.trim().isNotEmpty &&
          !RegExp('�{3,}').hasMatch(body)) {
        return body;
      }
    } catch (_) {
      // Try the next URL/mirror.
    } finally {
      client.close(force: true);
    }
  }
  return null;
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
    sourceIdentifiers: IpoStockIdentifiers(
      subscriptionKey: '',
      normalizedCompany: '',
      corpCode: firstNonEmptyString(row, ['corp_code', 'corpCode', 'corpCd']),
      stockCode: firstNonEmptyString(row, [
        'stock_code',
        'stockCode',
        'isu_cd',
      ]),
      kindCode: firstNonEmptyString(row, ['kindCode', 'kind_code']),
      isin: firstNonEmptyString(row, ['isin', 'isinCd', 'isin_code']),
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
    sourceIdentifiers: IpoStockIdentifiers(
      subscriptionKey: '',
      normalizedCompany: '',
      corpCode: firstNonEmptyString(row, ['corpCode', 'corp_code']),
      stockCode: firstNonEmptyString(row, [
        'stockCode',
        'stock_code',
        'symbol',
      ]),
      kindCode: firstNonEmptyString(row, ['kindCode', 'kind_code']),
      isin: firstNonEmptyString(row, ['isin', 'isinCode']),
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
