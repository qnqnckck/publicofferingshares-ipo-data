import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

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
    required this.manualFundamentalsPath,
    required this.interval,
    required this.discover,
    required this.discoverFinuts,
    required this.discoverIdentifiers,
    required this.discoverIpoKoreaSupplement,
    required this.discoverArticleLeadManagers,
    required this.collectPublicLive,
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
  final String manualFundamentalsPath;
  final Duration interval;
  final bool discover;
  final bool discoverFinuts;
  final bool discoverIdentifiers;
  final bool discoverIpoKoreaSupplement;
  final bool discoverArticleLeadManagers;
  final bool collectPublicLive;
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
  --manual-fundamentals-path <path> Optional JSON override file path for manual fundamentals patching. Default: data/manual_fundamentals.json
  --backfill-years <years>    Include IPOs from the last N years. Default: 3
  --interval-minutes <min>    Watch interval. Default: 10
  --dart-api-key-env <name>   Environment variable for DART API key. Default: DART_API_KEY
  --itick-api-key-env <name>  Environment variable for iTick API key. Default: ITICK_API_KEY
  --kis-app-key-env <name>    Environment variable for KIS app key. Default: KIS_APP_KEY
  --kis-app-secret-env <name> Environment variable for KIS app secret. Default: KIS_APP_SECRET
  --no-discover               Skip remote discovery and only normalize local input files.
  --no-finuts-discover        Skip Finuts discovery while keeping other configured discovery sources.
  --no-identifier-discover    Skip DART company-code backfill and only use local identifier crosswalk.
  --no-ipo-korea-supplement   Skip IPOKorea supplement scraping.
  --no-article-lead-manager-discover Skip article lead-manager scraping.
  --no-public-live-collect    Skip public live competition collectors such as Naver/Shinhan/Daishin/IPOSTOCK/38.
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
      manualFundamentalsPath: valueAfter(
        '--manual-fundamentals-path',
        'data/manual_fundamentals.json',
      ),
      backfillYears: intAfter('--backfill-years', 3),
      interval: Duration(minutes: intAfter('--interval-minutes', 10)),
      discover: !args.contains('--no-discover'),
      discoverFinuts: !args.contains('--no-finuts-discover'),
      discoverIdentifiers: !args.contains('--no-identifier-discover'),
      discoverIpoKoreaSupplement: !args.contains('--no-ipo-korea-supplement'),
      discoverArticleLeadManagers: !args.contains(
        '--no-article-lead-manager-discover',
      ),
      collectPublicLive: !args.contains('--no-public-live-collect'),
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
      final cachedDiscoveredStocks = await _loadDiscoveredStocks();
      final remotelyDiscoveredStocks = options.discover
          ? await _discoverRemoteStocks(generatedAt)
          : const <IpoCompetitionStock>[];
      final discoveredStocks = mergeStocks([
        ...cachedDiscoveredStocks,
        ...remotelyDiscoveredStocks,
      ]);
      await _writeDiscoveredStocks(discoveredStocks, generatedAt: generatedAt);
      await _writeDiscoveredDeltaReport(
        generatedAt: generatedAt,
        cachedStocks: cachedDiscoveredStocks,
        remoteStocks: remotelyDiscoveredStocks,
        mergedStocks: discoveredStocks,
      );
      final seedStocks = await _loadSeedStocks();
      final liveStocks = await _loadLiveStocks();
      final autoCoreBaseStocks = mergeStocks([
        ...discoveredStocks,
        ...liveStocks,
      ]);

      final stocksWithoutExternalOutcomes = mergeStocks([
        ...seedStocks,
        ...discoveredStocks,
        ...liveStocks,
      ]);
      final supplementStocks = mergeStocks([
        ...stocksWithoutExternalOutcomes,
        if (options.discoverIpoKoreaSupplement)
          ...await _discoverIpoKoreaSupplementStocks(
            stocksWithoutExternalOutcomes,
            generatedAt,
          ),
      ]);
      final sourceEnhancedStocks = mergeStocks([
        ...supplementStocks,
        ...buildKnownLeadManagerOverrideStocks(supplementStocks),
        if (options.discoverArticleLeadManagers)
          ...await _discoverArticleLeadManagerStocks(supplementStocks),
      ]);
      final outcomeRows = await _loadOutcomeRows();
      final stocks = mergeOutcomes(sourceEnhancedStocks, outcomeRows);
      final localIdentifierRows = alignIdentifierRowsToStocks(
        stocks,
        await _loadIdentifierRows(),
      );
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
        if (options.collectPublicLive)
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
      final autoSupplementStocks = mergeStocks([
        ...autoCoreBaseStocks,
        if (options.discoverIpoKoreaSupplement)
          ...await _discoverIpoKoreaSupplementStocks(
            autoCoreBaseStocks,
            generatedAt,
          ),
      ]);
      final autoSourceEnhancedStocks = mergeStocks([
        ...autoSupplementStocks,
        ...buildKnownLeadManagerOverrideStocks(autoSupplementStocks),
        if (options.discoverArticleLeadManagers)
          ...await _discoverArticleLeadManagerStocks(autoSupplementStocks),
      ]);
      final autoStocks = mergeOutcomes(autoSourceEnhancedStocks, outcomeRows);
      final autoIdentifiedStocks = mergeIdentifierRows(
        autoStocks,
        identifierRows,
      );
      final autoEnrichedStocks = mergeBrokerSnapshots(
        autoIdentifiedStocks,
        brokerSnapshotRows,
      );
      final manualFundamentalsRows = await _loadManualFundamentalsRows(
        options.manualFundamentalsPath,
      );
      final autoManualFundamentalsPatchedStocks =
          mergeManualFundamentalsOverrides(
            autoEnrichedStocks,
            manualFundamentalsRows,
          );
      final manualFundamentalsPatchedStocks = mergeManualFundamentalsOverrides(
        enrichedStocks,
        manualFundamentalsRows,
      );
      final cutoff = DateTime(
        generatedAt.year - options.backfillYears,
        generatedAt.month,
        generatedAt.day,
      );
      final autoMergedByIdentityStocks = mergeStocksByIdentity(
        autoManualFundamentalsPatchedStocks,
      );
      final autoConsolidatedStocks = applyGeneralSharesBackfill(
        autoMergedByIdentityStocks,
      );
      final autoSelected =
          autoConsolidatedStocks.where((stock) {
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
      final mergedByIdentityStocks = mergeStocksByIdentity(
        manualFundamentalsPatchedStocks,
      );
      final consolidatedStocks = applyGeneralSharesBackfill(
        mergedByIdentityStocks,
      );
      final selected =
          consolidatedStocks.where((stock) {
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
      final previousGeneratedStocks = await _loadGeneratedIndexStocks();
      await _writeScheduleChangesReport(
        generatedAt: generatedAt,
        previousStocks: previousGeneratedStocks,
        currentStocks: selected,
      );
      await _writeSeedDependencyReport(
        generatedAt: generatedAt,
        seedStocks: seedStocks,
        autoCoreStocks: autoSelected,
        finalStocks: selected,
      );
      await _writeAutoCoreReconciliationReport(
        generatedAt: generatedAt,
        seedStocks: seedStocks,
        autoCoreStocks: autoSelected,
      );
      await _writeHeuristicGeneralSharesReport(
        generatedAt: generatedAt,
        preBackfillStocks: mergedByIdentityStocks,
        finalStocks: selected,
      );

      _analysisCalibration = buildAnalysisCalibration(selected);

      final stockDir = Directory('${options.outDir}/stocks');
      await stockDir.create(recursive: true);
      final indexStocks = <Map<String, Object?>>[];
      final selectedIds = selected.map((stock) => safeId(stock.id)).toSet();
      await deleteOrphanedStockFiles(stockDir, selectedIds);

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
      await writeDashboardFeed(
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
            mergedStocks: consolidatedStocks,
            selectedStocks: selected,
          ),
        ),
      );
      await _writeFieldCoverageReports(
        generatedAt: generatedAt,
        stocks: selected,
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
      await File('${options.outDir}/service_health_report.json').writeAsString(
        prettyJson(
          buildServiceHealthReport(generatedAt: generatedAt, stocks: selected),
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

  Future<List<IpoCompetitionStock>> _loadGeneratedIndexStocks() async {
    final file = File('${options.outDir}/index.json');
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

  Future<void> _writeDiscoveredStocks(
    List<IpoCompetitionStock> stocks, {
    required DateTime generatedAt,
  }) async {
    final file = File(options.discoveredPath);
    await file.parent.create(recursive: true);
    await file.writeAsString(
      prettyJson({
        'schemaVersion': schemaVersion,
        'generatedAt': generatedAt.toIso8601String(),
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

  Future<void> _writeDiscoveredDeltaReport({
    required DateTime generatedAt,
    required List<IpoCompetitionStock> cachedStocks,
    required List<IpoCompetitionStock> remoteStocks,
    required List<IpoCompetitionStock> mergedStocks,
  }) async {
    final reportDir = Directory('${options.outDir}/reports');
    await reportDir.create(recursive: true);

    final normalizedCached = cachedStocks
        .map((stock) => stock.normalized())
        .toList();
    final normalizedRemote = remoteStocks
        .map((stock) => stock.normalized())
        .toList();
    final normalizedMerged = mergedStocks
        .map((stock) => stock.normalized())
        .toList();

    final cachedByKey = {
      for (final stock in normalizedCached) _discoveredReportKey(stock): stock,
    };
    final remoteByKey = {
      for (final stock in normalizedRemote) _discoveredReportKey(stock): stock,
    };
    final mergedByKey = {
      for (final stock in normalizedMerged) _discoveredReportKey(stock): stock,
    };

    List<Map<String, Object?>> stockRows(Iterable<IpoCompetitionStock> stocks) {
      final rows = stocks.map(_discoveredReportRow).toList()
        ..sort((a, b) {
          final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
            '${a['subscriptionStart'] ?? ''}',
          );
          if (byDate != 0) {
            return byDate;
          }
          return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
        });
      return rows;
    }

    final remoteOnlyKeys = remoteByKey.keys
        .where((key) => !cachedByKey.containsKey(key))
        .toList();
    final cachedOnlyKeys = cachedByKey.keys
        .where((key) => !remoteByKey.containsKey(key))
        .toList();

    final refreshedByRemote =
        remoteByKey.entries
            .where((entry) {
              final cached = cachedByKey[entry.key];
              return cached != null &&
                  cached.toJson().toString() != entry.value.toJson().toString();
            })
            .map((entry) {
              final previous = cachedByKey[entry.key]!;
              final current = entry.value;
              return {
                'id': safeId(current.id),
                'company': current.company,
                'subscriptionStart': current.subscriptionStart,
                'subscriptionEnd': current.subscriptionEnd,
                'previous': _discoveredReportRow(previous),
                'current': _discoveredReportRow(current),
              };
            })
            .toList()
          ..sort((a, b) {
            return '${b['subscriptionStart'] ?? ''}'.compareTo(
              '${a['subscriptionStart'] ?? ''}',
            );
          });

    final report = {
      'schemaVersion': schemaVersion,
      'generatedAt': generatedAt.toIso8601String(),
      'paths': {
        'discoveredInput': options.discoveredPath,
        'report': '${options.outDir}/reports/discovered_delta.json',
      },
      'sourceCounts': {
        'cachedFinuts': _countFinutsDiscoveredStocks(normalizedCached),
        'remoteFinuts': _countFinutsDiscoveredStocks(normalizedRemote),
        'mergedFinuts': _countFinutsDiscoveredStocks(normalizedMerged),
      },
      'totals': {
        'cached': normalizedCached.length,
        'remote': normalizedRemote.length,
        'merged': normalizedMerged.length,
        'remoteOnly': remoteOnlyKeys.length,
        'cachedOnly': cachedOnlyKeys.length,
        'refreshedByRemote': refreshedByRemote.length,
      },
      'remoteOnly': stockRows(
        remoteOnlyKeys
            .map((key) => remoteByKey[key]!)
            .whereType<IpoCompetitionStock>(),
      ),
      'cachedOnly': stockRows(
        cachedOnlyKeys
            .map((key) => cachedByKey[key]!)
            .whereType<IpoCompetitionStock>(),
      ),
      'refreshedByRemote': refreshedByRemote,
      'merged': stockRows(mergedByKey.values),
    };

    await File(
      '${options.outDir}/reports/discovered_delta.json',
    ).writeAsString(prettyJson(report));
  }

  int _countFinutsDiscoveredStocks(List<IpoCompetitionStock> stocks) {
    return stocks
        .where((stock) => safeId(stock.id).startsWith('finuts_'))
        .length;
  }

  Future<void> _writeScheduleChangesReport({
    required DateTime generatedAt,
    required List<IpoCompetitionStock> previousStocks,
    required List<IpoCompetitionStock> currentStocks,
  }) async {
    final reportDir = Directory('${options.outDir}/reports');
    await reportDir.create(recursive: true);

    final previousByKey = {
      for (final stock in previousStocks.map((stock) => stock.normalized()))
        _scheduleReportKey(stock): stock,
    };
    final currentByKey = {
      for (final stock in currentStocks.map((stock) => stock.normalized()))
        _scheduleReportKey(stock): stock,
    };

    final previousKeys = previousByKey.keys.toSet();
    final currentKeys = currentByKey.keys.toSet();

    List<Map<String, Object?>> stockRows(Iterable<IpoCompetitionStock> stocks) {
      final rows = stocks.map(_scheduleReportRow).toList()
        ..sort((a, b) {
          final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
            '${a['subscriptionStart'] ?? ''}',
          );
          if (byDate != 0) {
            return byDate;
          }
          return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
        });
      return rows;
    }

    final added = stockRows(
      currentKeys.difference(previousKeys).map((key) => currentByKey[key]!),
    );
    final removed = stockRows(
      previousKeys.difference(currentKeys).map((key) => previousByKey[key]!),
    );

    final changed = <Map<String, Object?>>[];
    final typeChanged = <Map<String, Object?>>[];
    for (final key in currentKeys.intersection(previousKeys)) {
      final previous = previousByKey[key]!;
      final current = currentByKey[key]!;
      final fieldChanges = _scheduleFieldChanges(previous, current);
      if (fieldChanges.isEmpty) {
        continue;
      }
      final row = {
        'id': safeId(current.id),
        'company': current.company,
        'subscriptionStart': current.subscriptionStart,
        'subscriptionEnd': current.subscriptionEnd,
        'changes': fieldChanges,
        'previous': _scheduleReportRow(previous),
        'current': _scheduleReportRow(current),
      };
      changed.add(row);
      if (fieldChanges.any((change) => change['field'] == 'securityType')) {
        typeChanged.add(row);
      }
    }
    changed.sort((a, b) {
      final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
        '${a['subscriptionStart'] ?? ''}',
      );
      if (byDate != 0) {
        return byDate;
      }
      return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
    });

    final report = {
      'schemaVersion': schemaVersion,
      'generatedAt': generatedAt.toIso8601String(),
      'paths': {
        'previousIndex': '${options.outDir}/index.json',
        'report': '${options.outDir}/reports/schedule_changes.json',
      },
      'totals': {
        'previous': previousByKey.length,
        'current': currentByKey.length,
        'added': added.length,
        'removed': removed.length,
        'changed': changed.length,
        'typeChanged': typeChanged.length,
      },
      'added': added,
      'removed': removed,
      'changed': changed,
      'typeChanged': typeChanged,
    };

    await File(
      '${options.outDir}/reports/schedule_changes.json',
    ).writeAsString(prettyJson(report));
  }

  Future<void> _writeFieldCoverageReports({
    required DateTime generatedAt,
    required List<IpoCompetitionStock> stocks,
  }) async {
    final reportDir = Directory('${options.outDir}/reports');
    await reportDir.create(recursive: true);

    final normalizedStocks = stocks.map((stock) => stock.normalized()).toList();
    final fieldExtractors = <String, Object? Function(IpoCompetitionStock)>{
      'industry': (stock) => stock.industry,
      'listingDate': (stock) => stock.resolvedListingDate,
      'leadManagers': (stock) => stock.leadManagers,
      'institutionCompetitionRate': (stock) =>
          stock.fundamentals.institutionCompetitionRate,
      'institutionParticipants': (stock) =>
          stock.fundamentals.institutionParticipants,
      'lockupCommitmentRate': (stock) =>
          stock.fundamentals.lockupCommitmentRate,
      'floatRate': (stock) => stock.fundamentals.floatRate,
      'marketCapKrw': (stock) => stock.fundamentals.marketCapKrw,
      'publicAllocationShares': (stock) =>
          stock.fundamentals.publicAllocationShares,
      'generalSharesDate': (stock) => stock.normalizedGeneralSharesDate,
      'securityType': (stock) => stock.normalizedSecurityType,
      'corpCode': (stock) => stock.identifiers.corpCode,
      'stockCode': (stock) => stock.identifiers.stockCode,
      'latestSnapshotAt': (stock) => stock.latestSnapshot?.capturedAt,
    };

    bool hasValue(Object? value) {
      if (value == null) {
        return false;
      }
      if (value is String) {
        return value.trim().isNotEmpty;
      }
      if (value is List) {
        return value.isNotEmpty;
      }
      return true;
    }

    final fieldCoverage = <String, Object?>{};
    final missingByField = <String, List<Map<String, Object?>>>{};
    for (final entry in fieldExtractors.entries) {
      final missing =
          normalizedStocks
              .where((stock) => !hasValue(entry.value(stock)))
              .map(
                (stock) => {
                  ..._scheduleReportRow(stock),
                  'market': stock.market,
                  'missingField': entry.key,
                },
              )
              .toList()
            ..sort((a, b) {
              final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
                '${a['subscriptionStart'] ?? ''}',
              );
              if (byDate != 0) {
                return byDate;
              }
              return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
            });
      final presentCount = normalizedStocks.length - missing.length;
      fieldCoverage[entry.key] = {
        'present': presentCount,
        'missing': missing.length,
        'coverageRate': normalizedStocks.isEmpty
            ? 0
            : double.parse(
                ((presentCount / normalizedStocks.length) * 100)
                    .toStringAsFixed(1),
              ),
      };
      missingByField[entry.key] = missing;
    }

    final fieldCoverageReport = {
      'schemaVersion': schemaVersion,
      'generatedAt': generatedAt.toIso8601String(),
      'totalStocks': normalizedStocks.length,
      'fields': fieldCoverage,
    };
    final missingAppFieldsReport = {
      'schemaVersion': schemaVersion,
      'generatedAt': generatedAt.toIso8601String(),
      'totalStocks': normalizedStocks.length,
      'missingByField': missingByField,
    };

    await File(
      '${options.outDir}/reports/field_coverage.json',
    ).writeAsString(prettyJson(fieldCoverageReport));
    await File(
      '${options.outDir}/reports/missing_app_fields.json',
    ).writeAsString(prettyJson(missingAppFieldsReport));
  }

  Future<void> _writeSeedDependencyReport({
    required DateTime generatedAt,
    required List<IpoCompetitionStock> seedStocks,
    required List<IpoCompetitionStock> autoCoreStocks,
    required List<IpoCompetitionStock> finalStocks,
  }) async {
    final reportDir = Directory('${options.outDir}/reports');
    await reportDir.create(recursive: true);

    final seedByKey = {
      for (final stock in seedStocks.map((stock) => stock.normalized()))
        _scheduleReportKey(stock): stock,
    };
    final autoCoreByKey = {
      for (final stock in autoCoreStocks.map((stock) => stock.normalized()))
        _scheduleReportKey(stock): stock,
    };
    final finalNormalized = finalStocks
        .map((stock) => stock.normalized())
        .toList();

    final presenceDependent = <Map<String, Object?>>[];
    final fieldDependent = <Map<String, Object?>>[];

    for (final stock in finalNormalized) {
      final key = _scheduleReportKey(stock);
      final autoCore = autoCoreByKey[key];
      final seed = seedByKey[key];
      if (seed == null) {
        continue;
      }
      if (autoCore == null) {
        presenceDependent.add({
          ..._scheduleReportRow(stock),
          'dependency': 'presence',
          'category': classifySeedOnlyGap(stock, generatedAt),
        });
        continue;
      }

      final missingFields = _appFieldDependencyDiff(autoCore, stock);
      if (missingFields.isEmpty) {
        continue;
      }
      fieldDependent.add({
        ..._scheduleReportRow(stock),
        'dependency': 'fields',
        'category': classifySeedOnlyGap(stock, generatedAt),
        'fieldsRecoveredOutsideAutoCore': missingFields,
      });
    }

    presenceDependent.sort((a, b) {
      final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
        '${a['subscriptionStart'] ?? ''}',
      );
      if (byDate != 0) {
        return byDate;
      }
      return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
    });
    fieldDependent.sort((a, b) {
      final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
        '${a['subscriptionStart'] ?? ''}',
      );
      if (byDate != 0) {
        return byDate;
      }
      return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
    });

    final report = {
      'schemaVersion': schemaVersion,
      'generatedAt': generatedAt.toIso8601String(),
      'totals': {
        'seedStocks': seedByKey.length,
        'autoCoreStocks': autoCoreByKey.length,
        'finalStocks': finalNormalized.length,
        'presenceDependent': presenceDependent.length,
        'fieldDependent': fieldDependent.length,
        'presenceByCategory': _categoryCounts(presenceDependent),
        'fieldByCategory': _categoryCounts(fieldDependent),
        'presenceByYear': _yearCounts(presenceDependent),
        'fieldByYear': _yearCounts(fieldDependent),
        'fieldRecoveredFrequency': _fieldRecoveredCounts(fieldDependent),
      },
      'presenceDependent': presenceDependent,
      'fieldDependent': fieldDependent,
    };

    await File(
      '${options.outDir}/reports/seed_dependent_stocks.json',
    ).writeAsString(prettyJson(report));
  }

  Future<void> _writeHeuristicGeneralSharesReport({
    required DateTime generatedAt,
    required List<IpoCompetitionStock> preBackfillStocks,
    required List<IpoCompetitionStock> finalStocks,
  }) async {
    final reportDir = Directory('${options.outDir}/reports');
    await reportDir.create(recursive: true);

    final preBackfillByKey = {
      for (final stock in preBackfillStocks.map((stock) => stock.normalized()))
        _scheduleReportKey(stock): stock,
    };

    final heuristicRows = <Map<String, Object?>>[];
    for (final stock in finalStocks.map((stock) => stock.normalized())) {
      if (stock.normalizedSecurityType != 'GENERAL_SHARES') {
        continue;
      }
      final previous = preBackfillByKey[_scheduleReportKey(stock)];
      if (previous == null) {
        continue;
      }
      final inferredFields = <String>[];
      if (previous.normalizedGeneralSharesDate == null &&
          stock.normalizedGeneralSharesDate != null) {
        inferredFields.add('generalSharesDate');
      }
      if (previous.normalizedSecurityType == null &&
          stock.normalizedSecurityType != null) {
        inferredFields.add('securityType');
      }
      if (inferredFields.isEmpty) {
        continue;
      }
      heuristicRows.add({
        ..._scheduleReportRow(stock),
        'inferredFields': inferredFields,
        'beforeBackfill': _scheduleReportRow(previous),
      });
    }

    heuristicRows.sort((a, b) {
      final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
        '${a['subscriptionStart'] ?? ''}',
      );
      if (byDate != 0) {
        return byDate;
      }
      return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
    });

    final report = {
      'schemaVersion': schemaVersion,
      'generatedAt': generatedAt.toIso8601String(),
      'totals': {
        'generalSharesInFinal': finalStocks
            .map((stock) => stock.normalized())
            .where((stock) => stock.normalizedSecurityType == 'GENERAL_SHARES')
            .length,
        'heuristicRows': heuristicRows.length,
      },
      'heuristicRows': heuristicRows,
    };

    await File(
      '${options.outDir}/reports/heuristic_general_shares.json',
    ).writeAsString(prettyJson(report));
  }

  Future<void> _writeAutoCoreReconciliationReport({
    required DateTime generatedAt,
    required List<IpoCompetitionStock> seedStocks,
    required List<IpoCompetitionStock> autoCoreStocks,
  }) async {
    final reportDir = Directory('${options.outDir}/reports');
    await reportDir.create(recursive: true);

    final seedByKey = {
      for (final stock in seedStocks.map((stock) => stock.normalized()))
        _scheduleReportKey(stock): stock,
    };
    final autoCoreByKey = {
      for (final stock in autoCoreStocks.map((stock) => stock.normalized()))
        _scheduleReportKey(stock): stock,
    };

    List<Map<String, Object?>> stockRows(Iterable<IpoCompetitionStock> stocks) {
      final rows = stocks.map(_scheduleReportRow).toList()
        ..sort((a, b) {
          final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
            '${a['subscriptionStart'] ?? ''}',
          );
          if (byDate != 0) {
            return byDate;
          }
          return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
        });
      return rows;
    }

    final seedOnly =
        stockRows(
          seedByKey.keys
              .where((key) => !autoCoreByKey.containsKey(key))
              .map((key) => seedByKey[key]!),
        ).map((row) {
          final key = 'subscription:${row['subscriptionKey'] ?? ''}';
          final stock =
              seedByKey[key] ?? seedByKey['id:${safeId('${row['id'] ?? ''}')}'];
          return {
            ...row,
            'category': stock == null
                ? 'unknown'
                : classifySeedOnlyGap(stock, generatedAt),
          };
        }).toList();
    final autoOnly = stockRows(
      autoCoreByKey.keys
          .where((key) => !seedByKey.containsKey(key))
          .map((key) => autoCoreByKey[key]!),
    );

    final sharedWithDifferences = <Map<String, Object?>>[];
    for (final key in seedByKey.keys.where(autoCoreByKey.containsKey)) {
      final seed = seedByKey[key]!;
      final auto = autoCoreByKey[key]!;
      final differences = _autoCoreReconciliationDiff(seed, auto);
      if (differences.isEmpty) {
        continue;
      }
      sharedWithDifferences.add({
        'id': safeId(seed.id),
        'company': seed.company,
        'subscriptionStart': seed.subscriptionStart,
        'subscriptionEnd': seed.subscriptionEnd,
        'differences': differences,
        'seed': _scheduleReportRow(seed),
        'autoCore': _scheduleReportRow(auto),
      });
    }
    sharedWithDifferences.sort((a, b) {
      final byDate = '${b['subscriptionStart'] ?? ''}'.compareTo(
        '${a['subscriptionStart'] ?? ''}',
      );
      if (byDate != 0) {
        return byDate;
      }
      return '${a['company'] ?? ''}'.compareTo('${b['company'] ?? ''}');
    });

    final report = {
      'schemaVersion': schemaVersion,
      'generatedAt': generatedAt.toIso8601String(),
      'totals': {
        'seed': seedByKey.length,
        'autoCore': autoCoreByKey.length,
        'seedOnly': seedOnly.length,
        'autoOnly': autoOnly.length,
        'sharedWithDifferences': sharedWithDifferences.length,
        'seedOnlyByCategory': _categoryCounts(seedOnly),
      },
      'seedOnly': seedOnly,
      'autoOnly': autoOnly,
      'sharedWithDifferences': sharedWithDifferences,
    };

    await File(
      '${options.outDir}/reports/auto_core_reconciliation.json',
    ).writeAsString(prettyJson(report));
  }

  String _discoveredReportKey(IpoCompetitionStock stock) {
    final subscriptionKey = canonicalSubscriptionKey(
      stock.identifiers.subscriptionKey,
    );
    if (subscriptionKey.isNotEmpty) {
      return 'subscription:$subscriptionKey';
    }
    final identifierKey = preferredIdentifierKey(stock.identifiers);
    if (identifierKey != null) {
      return identifierKey;
    }
    final company = normalizeCompanyIdentity(stock.company);
    final start = stock.subscriptionStart ?? '';
    final end = stock.subscriptionEnd ?? '';
    return 'company:$company|$start|$end';
  }

  Map<String, Object?> _discoveredReportRow(IpoCompetitionStock stock) {
    final normalized = stock.normalized();
    return {
      'id': safeId(normalized.id),
      'company': normalized.company,
      'subscriptionStart': normalized.subscriptionStart,
      'subscriptionEnd': normalized.subscriptionEnd,
      'listingDate': normalized.listingDate,
      'generalSharesDate': normalized.generalSharesDate,
      'securityType': normalized.securityType,
      'market': normalized.market,
      'leadManagers': normalized.leadManagers,
      'subscriptionKey': normalized.identifiers.subscriptionKey,
      'path': 'stocks/${safeId(normalized.id)}.json',
    };
  }

  String _scheduleReportKey(IpoCompetitionStock stock) {
    final subscriptionKey = canonicalSubscriptionKey(
      stock.identifiers.subscriptionKey,
    );
    if (subscriptionKey.isNotEmpty) {
      return 'subscription:$subscriptionKey';
    }
    final identifierKey = preferredIdentifierKey(stock.identifiers);
    if (identifierKey != null) {
      return identifierKey;
    }
    return 'id:${safeId(stock.id)}';
  }

  Map<String, Object?> _scheduleReportRow(IpoCompetitionStock stock) {
    final normalized = stock.normalized();
    return {
      'id': safeId(normalized.id),
      'company': normalized.company,
      'subscriptionStart': normalized.subscriptionStart,
      'subscriptionEnd': normalized.subscriptionEnd,
      'listingDate': normalized.resolvedListingDate,
      'generalSharesDate': normalized.normalizedGeneralSharesDate,
      'securityType': normalized.normalizedSecurityType,
      'subscriptionKey': normalized.identifiers.subscriptionKey,
      'path': 'stocks/${safeId(normalized.id)}.json',
    };
  }

  List<Map<String, Object?>> _scheduleFieldChanges(
    IpoCompetitionStock previous,
    IpoCompetitionStock current,
  ) {
    final changes = <Map<String, Object?>>[];

    void addChange(String field, Object? before, Object? after) {
      final normalizedBefore = before is String ? before.trim() : before;
      final normalizedAfter = after is String ? after.trim() : after;
      if (normalizedBefore == normalizedAfter) {
        return;
      }
      changes.add({
        'field': field,
        'before': normalizedBefore,
        'after': normalizedAfter,
      });
    }

    addChange(
      'subscriptionStart',
      previous.subscriptionStart,
      current.subscriptionStart,
    );
    addChange(
      'subscriptionEnd',
      previous.subscriptionEnd,
      current.subscriptionEnd,
    );
    addChange(
      'listingDate',
      previous.resolvedListingDate,
      current.resolvedListingDate,
    );
    addChange(
      'generalSharesDate',
      previous.normalizedGeneralSharesDate,
      current.normalizedGeneralSharesDate,
    );
    addChange(
      'securityType',
      previous.normalizedSecurityType,
      current.normalizedSecurityType,
    );
    return changes;
  }

  List<String> _appFieldDependencyDiff(
    IpoCompetitionStock autoCore,
    IpoCompetitionStock current,
  ) {
    final missingFields = <String>[];

    bool hasValue(Object? value) {
      if (value == null) {
        return false;
      }
      if (value is String) {
        return value.trim().isNotEmpty;
      }
      if (value is List) {
        return value.isNotEmpty;
      }
      return true;
    }

    void addIfRecovered(String field, Object? before, Object? after) {
      if (!hasValue(before) && hasValue(after)) {
        missingFields.add(field);
      }
    }

    addIfRecovered('industry', autoCore.industry, current.industry);
    addIfRecovered(
      'listingDate',
      autoCore.resolvedListingDate,
      current.resolvedListingDate,
    );
    addIfRecovered('leadManagers', autoCore.leadManagers, current.leadManagers);
    addIfRecovered(
      'institutionCompetitionRate',
      autoCore.fundamentals.institutionCompetitionRate,
      current.fundamentals.institutionCompetitionRate,
    );
    addIfRecovered(
      'institutionParticipants',
      autoCore.fundamentals.institutionParticipants,
      current.fundamentals.institutionParticipants,
    );
    addIfRecovered(
      'lockupCommitmentRate',
      autoCore.fundamentals.lockupCommitmentRate,
      current.fundamentals.lockupCommitmentRate,
    );
    addIfRecovered(
      'floatRate',
      autoCore.fundamentals.floatRate,
      current.fundamentals.floatRate,
    );
    addIfRecovered(
      'marketCapKrw',
      autoCore.fundamentals.marketCapKrw,
      current.fundamentals.marketCapKrw,
    );
    addIfRecovered(
      'publicAllocationShares',
      autoCore.fundamentals.publicAllocationShares,
      current.fundamentals.publicAllocationShares,
    );
    addIfRecovered(
      'generalSharesDate',
      autoCore.normalizedGeneralSharesDate,
      current.normalizedGeneralSharesDate,
    );
    addIfRecovered(
      'securityType',
      autoCore.normalizedSecurityType,
      current.normalizedSecurityType,
    );
    addIfRecovered(
      'corpCode',
      autoCore.identifiers.corpCode,
      current.identifiers.corpCode,
    );
    addIfRecovered(
      'stockCode',
      autoCore.identifiers.stockCode,
      current.identifiers.stockCode,
    );
    return missingFields;
  }

  List<Map<String, Object?>> _autoCoreReconciliationDiff(
    IpoCompetitionStock seed,
    IpoCompetitionStock autoCore,
  ) {
    final differences = <Map<String, Object?>>[];

    void addDifference(String field, Object? seedValue, Object? autoValue) {
      final normalizedSeed = seedValue is String ? seedValue.trim() : seedValue;
      final normalizedAuto = autoValue is String ? autoValue.trim() : autoValue;
      if (normalizedSeed == normalizedAuto) {
        return;
      }
      differences.add({
        'field': field,
        'seed': normalizedSeed,
        'autoCore': normalizedAuto,
      });
    }

    addDifference('market', seed.market, autoCore.market);
    addDifference('industry', seed.industry, autoCore.industry);
    addDifference(
      'subscriptionStart',
      seed.subscriptionStart,
      autoCore.subscriptionStart,
    );
    addDifference(
      'subscriptionEnd',
      seed.subscriptionEnd,
      autoCore.subscriptionEnd,
    );
    addDifference(
      'listingDate',
      seed.resolvedListingDate,
      autoCore.resolvedListingDate,
    );
    addDifference(
      'generalSharesDate',
      seed.normalizedGeneralSharesDate,
      autoCore.normalizedGeneralSharesDate,
    );
    addDifference(
      'securityType',
      seed.normalizedSecurityType,
      autoCore.normalizedSecurityType,
    );
    addDifference(
      'institutionCompetitionRate',
      seed.fundamentals.institutionCompetitionRate,
      autoCore.fundamentals.institutionCompetitionRate,
    );
    return differences;
  }

  String classifySeedOnlyGap(IpoCompetitionStock stock, DateTime generatedAt) {
    if (stock.normalizedSecurityType == 'GENERAL_SHARES' ||
        stock.normalizedGeneralSharesDate != null) {
      return 'general_shares';
    }
    final today = DateTime(
      generatedAt.year,
      generatedAt.month,
      generatedAt.day,
    );
    final end =
        parseDate(stock.subscriptionEnd) ??
        parseDate(stock.subscriptionStart) ??
        parseDate(stock.resolvedListingDate);
    if (end != null && end.isBefore(today)) {
      return 'historical';
    }
    final company = stock.company;
    if (company.contains('스팩') || stock.normalizedSecurityType == 'SPAC') {
      return 'spac_gap';
    }
    return 'current_finuts_gap';
  }

  Map<String, int> _categoryCounts(List<Map<String, Object?>> rows) {
    final counts = <String, int>{};
    for (final row in rows) {
      final category = '${row['category'] ?? 'unknown'}';
      counts[category] = (counts[category] ?? 0) + 1;
    }
    return counts;
  }

  Map<String, int> _yearCounts(List<Map<String, Object?>> rows) {
    final counts = <String, int>{};
    for (final row in rows) {
      final raw =
          '${row['subscriptionStart'] ?? row['listingDate'] ?? row['generalSharesDate'] ?? ''}'
              .trim();
      final year = raw.length >= 4 ? raw.substring(0, 4) : 'unknown';
      counts[year] = (counts[year] ?? 0) + 1;
    }
    return counts;
  }

  Map<String, int> _fieldRecoveredCounts(List<Map<String, Object?>> rows) {
    final counts = <String, int>{};
    for (final row in rows) {
      final fields = row['fieldsRecoveredOutsideAutoCore'] is List
          ? row['fieldsRecoveredOutsideAutoCore'] as List
          : const [];
      for (final field in fields) {
        final key = '$field'.trim();
        if (key.isEmpty) {
          continue;
        }
        counts[key] = (counts[key] ?? 0) + 1;
      }
    }
    return counts;
  }

  Future<List<IpoCompetitionStock>> _discoverRemoteStocks(DateTime now) async {
    final discovered = <IpoCompetitionStock>[];
    if (options.discoverFinuts) {
      discovered.addAll(await _discoverFinutsStocks());
    }
    discovered.addAll(await _discoverDartStocks(now));
    discovered.addAll(await _discoverItickStocks());
    _noteKisCredentialsIfConfigured();
    return discovered;
  }

  Future<List<IpoCompetitionStock>> _discoverFinutsStocks() async {
    final uri = Uri.parse(
      'https://www.finuts.co.kr/html/task/ipo/ipoListQuery.php',
    );
    try {
      final response = await httpPostJson(uri, {
        'active': 'ipo-011',
        'search_text': '',
      });
      final rows = response['data'];
      if (rows is! List) {
        return const [];
      }

      final byIpoSn = <String, List<Map<String, Object?>>>{};
      for (final row in rows.whereType<Map<String, Object?>>()) {
        final ipoSn = (row['IPO_SN'] ?? '').toString().trim();
        final company = (row['ENT_NM'] ?? '').toString().trim();
        if (ipoSn.isEmpty || company.isEmpty) {
          continue;
        }
        byIpoSn.putIfAbsent(ipoSn, () => <Map<String, Object?>>[]).add(row);
      }

      return byIpoSn.values
          .map(stockFromFinutsRows)
          .whereType<IpoCompetitionStock>()
          .toList();
    } catch (error) {
      stderr.writeln('Finuts discovery failed: $error');
      return const [];
    }
  }

  Future<List<IpoCompetitionStock>> _discoverIpoKoreaSupplementStocks(
    List<IpoCompetitionStock> stocks,
    DateTime now,
  ) async {
    const supplementCandidateLimit = 48;
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
    for (final stock in candidates.take(supplementCandidateLimit)) {
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
          industry: stock.industry,
          subscriptionStart: stock.subscriptionStart,
          subscriptionEnd: stock.subscriptionEnd,
          demandForecastStart: stock.demandForecastStart,
          demandForecastEnd: stock.demandForecastEnd,
          refundDate: stock.refundDate,
          listingDate: stock.listingDate,
          lockupReleaseDate: stock.lockupReleaseDate,
          generalSharesDate: stock.generalSharesDate,
          cbBwDate: stock.cbBwDate,
          securityType: stock.securityType,
          leadManagers: leadManagers,
          sourceIdentifiers: stock.identifiers,
          fundamentals: const IpoFundamentals(
            offerPrice: null,
            priceBandMin: null,
            priceBandMax: null,
            topBandConfirmation: null,
            institutionCompetitionRate: null,
            institutionParticipants: null,
            lockupCommitmentRate: null,
            floatRate: null,
            marketCapKrw: null,
            publicAllocationShares: null,
            hasPutbackRight: false,
            putbackSummary: null,
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

    final naverCalculatorCodes = await _fetchNaverCalculatorActiveCodes();
    final rows = <IpoBrokerSnapshotRow>[];
    for (final stock in active) {
      rows.addAll(
        await _fetchPublicLiveSnapshots(
          stock,
          now,
          naverCalculatorCodes: naverCalculatorCodes,
        ),
      );
    }
    return rows;
  }

  Future<List<IpoBrokerSnapshotRow>> _fetchPublicLiveSnapshots(
    IpoCompetitionStock stock,
    DateTime now, {
    Map<String, String> naverCalculatorCodes = const <String, String>{},
  }) async {
    final naverSnapshot = await _fetchNaverCalculatorSnapshot(
      stock,
      now,
      activeCodes: naverCalculatorCodes,
    );
    if (naverSnapshot != null) {
      return [naverSnapshot];
    }

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

  Future<Map<String, String>> _fetchNaverCalculatorActiveCodes() async {
    try {
      final response = await httpGetJson(
        Uri.parse(
          'https://m.stock.naver.com/front-api/ipo/calculator/activeItems',
        ),
      );
      final rawItems = response['result'];
      if (rawItems is! List) {
        return const <String, String>{};
      }
      final result = <String, String>{};
      for (final item in rawItems.whereType<Map<String, Object?>>()) {
        final company = readString(item, 'compName');
        final code = _normalizeNaverIpoCode(readString(item, 'ipoCode'));
        if (company == null || code == null) {
          continue;
        }
        result[normalizeCompanyIdentity(company)] = code;
      }
      return result;
    } catch (error) {
      stderr.writeln('Naver calculator activeItems fetch failed: $error');
      return const <String, String>{};
    }
  }

  Future<IpoBrokerSnapshotRow?> _fetchNaverCalculatorSnapshot(
    IpoCompetitionStock stock,
    DateTime now, {
    required Map<String, String> activeCodes,
  }) async {
    final code =
        _normalizeNaverIpoCode(stock.identifiers.stockCode) ??
        activeCodes[normalizeCompanyIdentity(stock.company)];
    if (code == null) {
      return null;
    }

    final uri = Uri.parse(
      'https://m.stock.naver.com/front-api/ipo/calculator/operands?code=$code',
    );
    final response = await httpGetJson(uri);
    final payload = response['result'];
    if (payload is! Map<String, Object?>) {
      return null;
    }

    final fixPubPrice = readOptionalInt(payload['fixPubPrice']);
    final rawDepositRate = readDouble(payload['sbscMrgnRatio']);
    final depositRate = rawDepositRate == null || rawDepositRate <= 0
        ? null
        : rawDepositRate / 100;
    final capturedAt = readString(payload, 'baseTime') ?? now.toIso8601String();
    final rawManagers = payload['joinManagers'];
    if (rawManagers is! List) {
      return null;
    }

    final brokers = <IpoBrokerCompetition>[];
    for (final manager in rawManagers.whereType<Map<String, Object?>>()) {
      final brokerName = canonicalBrokerName(
        readString(manager, 'orgNm') ?? '',
      );
      if (brokerName.trim().isEmpty) {
        continue;
      }

      final offeredShares = readOptionalInt(manager['orgAllcShares']) ?? 0;
      final equalAllocationShares = readOptionalInt(manager['orgEqlShares']);
      final proportionalAllocationShares = readOptionalInt(
        manager['orgPrtShares'],
      );
      final totalRate = readDouble(manager['ttlCmptRatio']);
      final proportionalRate = readDouble(manager['prtCmptRatio']);
      final applicationCount = readOptionalInt(manager['sbscNum']);
      final expectedEqualShares =
          equalAllocationShares != null &&
              applicationCount != null &&
              applicationCount > 0
          ? equalAllocationShares / applicationCount
          : null;

      if (offeredShares <= 0 &&
          equalAllocationShares == null &&
          proportionalAllocationShares == null &&
          totalRate == null &&
          proportionalRate == null &&
          applicationCount == null) {
        continue;
      }

      final subscribedShares = totalRate != null && offeredShares > 0
          ? (offeredShares * totalRate).round()
          : proportionalRate != null &&
                proportionalAllocationShares != null &&
                proportionalAllocationShares > 0
          ? (proportionalAllocationShares * proportionalRate).round()
          : 0;

      brokers.add(
        IpoBrokerCompetition(
          name: brokerName,
          offeredShares: offeredShares,
          subscribedShares: subscribedShares,
          offerPrice: fixPubPrice,
          depositRate: depositRate,
          feeKrw: null,
          competitionRate: totalRate,
          equalCompetitionRate: null,
          proportionalCompetitionRate: proportionalRate ?? totalRate,
          equalAllocationShares: equalAllocationShares,
          proportionalAllocationShares: proportionalAllocationShares,
          expectedEqualShares: expectedEqualShares,
          applicationCount: applicationCount,
        ),
      );
    }

    if (brokers.isEmpty) {
      return null;
    }

    return IpoBrokerSnapshotRow(
      id: stock.id,
      company: stock.company,
      capturedAt: capturedAt,
      source: 'naver_calculator_live',
      sourceUrl: uri.toString(),
      brokers: brokers,
    );
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
    required this.industry,
    required this.subscriptionStart,
    required this.subscriptionEnd,
    this.demandForecastStart,
    this.demandForecastEnd,
    this.refundDate,
    this.listingDate,
    this.lockupReleaseDate,
    this.generalSharesDate,
    this.cbBwDate,
    this.securityType,
    required this.leadManagers,
    required this.sourceIdentifiers,
    required this.fundamentals,
    required this.outcome,
    required this.snapshots,
  });

  final String id;
  final String company;
  final String market;
  final String industry;
  final String? subscriptionStart;
  final String? subscriptionEnd;
  final String? demandForecastStart;
  final String? demandForecastEnd;
  final String? refundDate;
  final String? listingDate;
  final String? lockupReleaseDate;
  final String? generalSharesDate;
  final String? cbBwDate;
  final String? securityType;
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
      industry: readString(json, 'industry') ?? '',
      subscriptionStart: readString(json, 'subscriptionStart'),
      subscriptionEnd: readString(json, 'subscriptionEnd'),
      demandForecastStart:
          readString(json, 'demandForecastStart') ??
          readString(json, 'demandForecastDate'),
      demandForecastEnd:
          readString(json, 'demandForecastEnd') ??
          readString(json, 'demandForecastDate'),
      refundDate: readString(json, 'refundDate'),
      listingDate: readString(json, 'listingDate'),
      lockupReleaseDate: readString(json, 'lockupReleaseDate'),
      generalSharesDate: readString(json, 'generalSharesDate'),
      cbBwDate: readString(json, 'cbBwDate'),
      securityType: readString(json, 'securityType'),
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
    final normalizedLeadManagers = mergeOrderedStrings(leadManagers, const []);
    final allowedBrokerNames = normalizedLeadManagers
        .map(canonicalBrokerName)
        .where((name) => name.isNotEmpty)
        .toSet();
    final normalizedSnapshots =
        snapshots
            .map((snapshot) => snapshot.normalized())
            .map((snapshot) {
              if (allowedBrokerNames.isEmpty) {
                return snapshot;
              }
              final brokers = snapshot.brokers.where((broker) {
                final name = canonicalBrokerName(broker.name);
                return name == '통합' || allowedBrokerNames.contains(name);
              }).toList();
              return IpoCompetitionSnapshot(
                capturedAt: snapshot.capturedAt,
                source: snapshot.source,
                sourceUrl: snapshot.sourceUrl,
                aggregateCompetitionRate: snapshot.aggregateCompetitionRate,
                brokers: brokers,
              );
            })
            .where((snapshot) => snapshot.brokers.isNotEmpty)
            .toList()
          ..sort((a, b) => a.capturedAt.compareTo(b.capturedAt));
    final seenSnapshotKeys = <String>{};
    final dedupedSnapshots = <IpoCompetitionSnapshot>[];
    for (final snapshot in normalizedSnapshots) {
      final key = _competitionSnapshotKey(snapshot);
      if (seenSnapshotKeys.add(key)) {
        dedupedSnapshots.add(snapshot);
      }
    }
    return IpoCompetitionStock(
      id: safeId(id),
      company: company.trim(),
      market: market.trim(),
      industry: industry.trim(),
      subscriptionStart: subscriptionStart,
      subscriptionEnd: subscriptionEnd,
      demandForecastStart:
          normalizeDate(demandForecastStart) ?? demandForecastStart,
      demandForecastEnd: normalizeDate(demandForecastEnd) ?? demandForecastEnd,
      refundDate: normalizeDate(refundDate) ?? refundDate,
      listingDate: normalizeDate(listingDate) ?? listingDate,
      lockupReleaseDate: normalizeDate(lockupReleaseDate) ?? lockupReleaseDate,
      generalSharesDate: normalizeDate(generalSharesDate) ?? generalSharesDate,
      cbBwDate: normalizeDate(cbBwDate) ?? cbBwDate,
      securityType: securityType?.trim(),
      leadManagers: normalizedLeadManagers,
      sourceIdentifiers: identifiers,
      fundamentals: fundamentals.normalized(),
      outcome: outcome?.normalized(),
      snapshots: dedupedSnapshots,
    );
  }

  IpoCompetitionSnapshot? get latestSnapshot {
    if (snapshots.isEmpty) {
      return null;
    }
    return snapshots.reduce((a, b) {
      final aPriority = snapshotSourcePriority(a.source);
      final bPriority = snapshotSourcePriority(b.source);
      if (aPriority != bPriority) {
        return aPriority > bPriority ? a : b;
      }
      return a.capturedAt.compareTo(b.capturedAt) >= 0 ? a : b;
    });
  }

  Map<String, Object?> toJson() {
    final analysis = analyzeStock(this);
    return {
      'schemaVersion': schemaVersion,
      'id': safeId(id),
      'identifiers': identifiers.toJson(),
      'company': company,
      'market': market,
      'industry': industry,
      'demandForecastDate': normalizedDemandForecastStart,
      'demandForecastStart': normalizedDemandForecastStart,
      'demandForecastEnd': normalizedDemandForecastEnd,
      'subscriptionStart': subscriptionStart,
      'subscriptionEnd': subscriptionEnd,
      'refundDate': normalizedRefundDate,
      'listingDate': resolvedListingDate,
      'lockupReleaseDate': normalizedLockupReleaseDate,
      'generalSharesDate': normalizedGeneralSharesDate,
      'cbBwDate': normalizedCbBwDate,
      'securityType': normalizedSecurityType,
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
      'industry': industry,
      'demandForecastDate': normalizedDemandForecastStart,
      'demandForecastStart': normalizedDemandForecastStart,
      'demandForecastEnd': normalizedDemandForecastEnd,
      'subscriptionStart': subscriptionStart,
      'subscriptionEnd': subscriptionEnd,
      'leadManagers': leadManagers,
      'offerPrice': fundamentals.offerPrice,
      'priceBandMin': fundamentals.priceBandMin,
      'priceBandMax': fundamentals.priceBandMax,
      'refundDate': normalizedRefundDate,
      'listingDate': resolvedListingDate,
      'lockupReleaseDate': normalizedLockupReleaseDate,
      'generalSharesDate': normalizedGeneralSharesDate,
      'cbBwDate': normalizedCbBwDate,
      'securityType': normalizedSecurityType,
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

  String? get resolvedListingDate {
    return normalizeDate(listingDate) ??
        normalizeDate(outcome?.listingDate) ??
        listingDate ??
        outcome?.listingDate;
  }

  String? get normalizedGeneralSharesDate {
    return normalizeDate(generalSharesDate) ?? generalSharesDate;
  }

  String? get normalizedDemandForecastStart {
    return normalizeDate(demandForecastStart) ?? demandForecastStart;
  }

  String? get normalizedDemandForecastEnd {
    return normalizeDate(demandForecastEnd) ??
        normalizeDate(demandForecastStart) ??
        demandForecastEnd ??
        demandForecastStart;
  }

  String? get normalizedRefundDate {
    return normalizeDate(refundDate) ?? refundDate;
  }

  String? get normalizedLockupReleaseDate {
    return normalizeDate(lockupReleaseDate) ?? lockupReleaseDate;
  }

  String? get normalizedCbBwDate {
    return normalizeDate(cbBwDate) ?? cbBwDate;
  }

  String? get normalizedSecurityType {
    final normalized = securityType?.trim();
    if (normalized == null || normalized.isEmpty) {
      return null;
    }
    return normalized;
  }
}

class IpoManualFundamentalsOverride {
  const IpoManualFundamentalsOverride({
    required this.id,
    required this.company,
    required this.industry,
    required this.fundamentals,
  });

  final String id;
  final String company;
  final String industry;
  final IpoFundamentals fundamentals;

  factory IpoManualFundamentalsOverride.fromJson(Map<String, Object?> json) {
    final id = readString(json, 'id') ?? '';
    final company = readString(json, 'company') ?? '';
    if (id.trim().isEmpty && company.trim().isEmpty) {
      throw const FormatException(
        'Manual override entry requires "id" or "company".',
      );
    }

    final nested = json['fundamentals'];
    final fundamentalsSource = nested == null
        ? json
        : nested is Map<String, Object?>
        ? nested
        : throw const FormatException(
            'Invalid manual override fundamentals: "fundamentals" must be an object.',
          );

    return IpoManualFundamentalsOverride(
      id: id.trim(),
      company: company.trim(),
      industry: (readString(json, 'industry') ?? '').trim(),
      fundamentals: IpoFundamentals.fromJson(fundamentalsSource),
    );
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
        parseDate(stock.resolvedListingDate) ??
        parseDate(stock.normalizedGeneralSharesDate) ??
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

Future<void> writeDashboardFeed({
  required String outDir,
  required DateTime generatedAt,
  required List<IpoCompetitionStock> stocks,
}) async {
  final today = DateTime(generatedAt.year, generatedAt.month, generatedAt.day);
  final normalized = stocks.map((stock) => stock.normalized()).toList();

  bool isActiveOrUpcomingOrRecent(IpoCompetitionStock stock) {
    final start = parseDate(stock.subscriptionStart);
    final end =
        parseDate(stock.subscriptionEnd) ??
        parseDate(stock.outcome?.listingDate) ??
        start;
    if (start == null && end == null) {
      return false;
    }
    if (start != null && start.isAfter(today)) {
      return true;
    }
    if (start != null &&
        end != null &&
        !today.isBefore(start) &&
        !today.isAfter(end)) {
      return true;
    }
    if (end != null && !end.isAfter(today)) {
      return true;
    }
    return false;
  }

  final selected = normalized.where(isActiveOrUpcomingOrRecent).toList()
    ..sort((a, b) {
      final aDate =
          a.subscriptionStart ??
          a.subscriptionEnd ??
          a.outcome?.listingDate ??
          '';
      final bDate =
          b.subscriptionStart ??
          b.subscriptionEnd ??
          b.outcome?.listingDate ??
          '';
      return bDate.compareTo(aDate);
    });

  await File('$outDir/dashboard.json').writeAsString(
    prettyJson({
      'schemaVersion': schemaVersion,
      'generatedAt': generatedAt.toIso8601String(),
      'stocks': selected
          .map(
            (stock) => buildDashboardFeedItem(stock, 'stocks/${stock.id}.json'),
          )
          .toList(),
    }),
  );
}

Map<String, Object?> buildDashboardFeedItem(
  IpoCompetitionStock stock,
  String path,
) {
  final latest = stock.latestSnapshot;
  final analysis = analyzeStock(stock);
  final bestBroker = bestDashboardBrokerMetric(latest);
  final putbackSummary =
      stock.fundamentals.putbackSummary?.trim().isNotEmpty == true
      ? stock.fundamentals.putbackSummary!.trim()
      : null;
  final hasPutbackRight = stock.fundamentals.hasPutbackRight;
  return {
    'id': safeId(stock.id),
    'path': path,
    'identifiers': stock.identifiers.toJson(),
    'company': stock.company,
    'market': stock.market,
    'industry': stock.industry,
    'demandForecastDate': stock.normalizedDemandForecastStart,
    'demandForecastStart': stock.normalizedDemandForecastStart,
    'demandForecastEnd': stock.normalizedDemandForecastEnd,
    'subscriptionStart': stock.subscriptionStart,
    'subscriptionEnd': stock.subscriptionEnd,
    'refundDate': stock.normalizedRefundDate,
    'listingDate': stock.resolvedListingDate,
    'lockupReleaseDate': stock.normalizedLockupReleaseDate,
    'generalSharesDate': stock.normalizedGeneralSharesDate,
    'cbBwDate': stock.normalizedCbBwDate,
    'securityType': stock.normalizedSecurityType,
    'latestSnapshotAt': latest?.capturedAt,
    'latestCompetitionRate': latest?.aggregate.competitionRate,
    'score': analysis.score.overall,
    'grade': analysis.score.grade,
    'decisionLevel': analysis.decision.level,
    'hasPutbackRight': hasPutbackRight,
    'putbackSummary': putbackSummary,
    'bestBrokerName': bestBroker?.name,
    'bestBrokerCompetitionRate':
        bestBroker?.proportionalCompetitionRate ?? bestBroker?.competitionRate,
  };
}

IpoBrokerCompetition? bestDashboardBrokerMetric(
  IpoCompetitionSnapshot? snapshot,
) {
  if (snapshot == null || snapshot.brokers.isEmpty) {
    return null;
  }
  final candidates = snapshot.brokers.where((broker) {
    final name = canonicalBrokerName(broker.name);
    if (name.trim().isEmpty || name.trim() == '통합') {
      return false;
    }
    final rate = broker.proportionalCompetitionRate ?? broker.competitionRate;
    return rate != null && rate > 0;
  }).toList();
  if (candidates.isEmpty) {
    return null;
  }
  candidates.sort((a, b) {
    final aRate = a.proportionalCompetitionRate ?? a.competitionRate ?? 0;
    final bRate = b.proportionalCompetitionRate ?? b.competitionRate ?? 0;
    return aRate.compareTo(bRate);
  });
  return candidates.first;
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
      .map(
        (stock) => canonicalSubscriptionKey(stock.identifiers.subscriptionKey),
      )
      .where((key) => key.isNotEmpty)
      .where((key) => key.isNotEmpty)
      .toSet();
  final selectedIds = normalizedSelected.map((stock) => stock.id).toSet();

  bool isWithinBackfill(IpoCompetitionStock stock) {
    final end = parseDate(stock.subscriptionEnd);
    return end == null || !end.isBefore(cutoff);
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
        final key = canonicalSubscriptionKey(stock.identifiers.subscriptionKey);
        return !selectedIds.contains(stock.id) &&
            (key.isEmpty || !selectedKeys.contains(key));
      })
      .map((stock) => stockIssueJson(stock, ['discovered_not_generated']))
      .toList();

  final qualityRows = <Map<String, Object?>>[];
  final issueCounts = <String, int>{};
  for (final stock in normalizedSelected) {
    final issues = coverageIssuesFor(stock, today);
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

bool isCompletedOnOrBefore(IpoCompetitionStock stock, DateTime today) {
  final end =
      parseDate(stock.subscriptionEnd) ?? parseDate(stock.subscriptionStart);
  return end != null && !end.isAfter(today);
}

bool isActiveOnDate(IpoCompetitionStock stock, DateTime today) {
  final start = parseDate(stock.subscriptionStart);
  final end = parseDate(stock.subscriptionEnd) ?? start;
  if (start == null || end == null) {
    return false;
  }
  return !today.isBefore(start) && !today.isAfter(end);
}

bool hasBrokerLevelSnapshot(IpoCompetitionStock stock) {
  return stock.snapshots.any(
    (snapshot) => snapshot.brokers.any((broker) {
      final isAggregateName = broker.name == '통합' || broker.name == 'aggregate';
      return !isAggregateName &&
          (broker.offeredShares > 0 ||
              broker.competitionRate != null ||
              broker.equalCompetitionRate != null ||
              broker.proportionalCompetitionRate != null);
    }),
  );
}

List<String> coverageIssuesFor(IpoCompetitionStock stock, DateTime today) {
  final issues = <String>[];
  final fundamentals = stock.fundamentals;
  final latest = stock.latestSnapshot;
  final identifiers = stock.identifiers;
  final isCompleted = isCompletedOnOrBefore(stock, today);
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
  if (latest?.aggregate.competitionRate == null && isCompleted) {
    issues.add('missing_retail_competition_rate');
  }
  if (latest == null && isCompleted) {
    issues.add('missing_competition_snapshot');
  }
  if (!hasBrokerLevelSnapshot(stock) && isCompleted) {
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

Map<String, Object?> buildServiceHealthReport({
  required DateTime generatedAt,
  required List<IpoCompetitionStock> stocks,
}) {
  final today = DateTime(generatedAt.year, generatedAt.month, generatedAt.day);
  final normalized = stocks.map((stock) => stock.normalized()).toList();
  final active =
      normalized.where((stock) => isActiveOnDate(stock, today)).toList()..sort(
        (a, b) => (a.subscriptionEnd ?? '').compareTo(b.subscriptionEnd ?? ''),
      );
  final upcomingNext7Days =
      normalized.where((stock) {
        final start = parseDate(stock.subscriptionStart);
        if (start == null || !start.isAfter(today)) {
          return false;
        }
        return !start.isAfter(today.add(const Duration(days: 7)));
      }).toList()..sort(
        (a, b) =>
            (a.subscriptionStart ?? '').compareTo(b.subscriptionStart ?? ''),
      );

  final latestSourceCounts = <String, int>{};
  final freshnessBuckets = <String, int>{
    'within_1h': 0,
    'within_6h': 0,
    'within_24h': 0,
    'older': 0,
    'missing': 0,
  };
  var missingInstitutionCompetition = 0;
  var missingInstitutionParticipants = 0;
  var missingLockupCommitmentRate = 0;
  var missingLatestCompetitionRate = 0;
  var missingLatestSnapshotAt = 0;
  var missingBrokerMetrics = 0;
  var liveLikeSourceCount = 0;
  var finutsSourceCount = 0;
  var ocrSourceCount = 0;

  for (final stock in active) {
    final latest = stock.latestSnapshot;
    final source = latest?.source.trim().isEmpty ?? true
        ? 'missing'
        : latest!.source.trim();
    latestSourceCounts[source] = (latestSourceCounts[source] ?? 0) + 1;

    final lowerSource = source.toLowerCase();
    if (lowerSource.contains('live') || lowerSource.contains('ipostock')) {
      liveLikeSourceCount += 1;
    }
    if (lowerSource.contains('finuts')) {
      finutsSourceCount += 1;
    }
    if (lowerSource.contains('ocr') || lowerSource.contains('youtube')) {
      ocrSourceCount += 1;
    }

    final capturedAt = parseDate(latest?.capturedAt);
    if (capturedAt == null) {
      freshnessBuckets['missing'] = (freshnessBuckets['missing'] ?? 0) + 1;
      missingLatestSnapshotAt += 1;
    } else {
      final age = generatedAt.difference(capturedAt);
      if (age <= const Duration(hours: 1)) {
        freshnessBuckets['within_1h'] =
            (freshnessBuckets['within_1h'] ?? 0) + 1;
      } else if (age <= const Duration(hours: 6)) {
        freshnessBuckets['within_6h'] =
            (freshnessBuckets['within_6h'] ?? 0) + 1;
      } else if (age <= const Duration(hours: 24)) {
        freshnessBuckets['within_24h'] =
            (freshnessBuckets['within_24h'] ?? 0) + 1;
      } else {
        freshnessBuckets['older'] = (freshnessBuckets['older'] ?? 0) + 1;
      }
    }

    if (stock.fundamentals.institutionCompetitionRate == null) {
      missingInstitutionCompetition += 1;
    }
    if (stock.fundamentals.institutionParticipants == null) {
      missingInstitutionParticipants += 1;
    }
    if (stock.fundamentals.lockupCommitmentRate == null) {
      missingLockupCommitmentRate += 1;
    }
    if (latest?.aggregate.competitionRate == null) {
      missingLatestCompetitionRate += 1;
    }
    if (!hasBrokerLevelSnapshot(stock)) {
      missingBrokerMetrics += 1;
    }
  }

  final activeSamples = active.take(5).map((stock) {
    final latest = stock.latestSnapshot;
    return {
      'id': stock.id,
      'company': stock.company,
      'path': 'stocks/${stock.id}.json',
      'subscriptionStart': stock.subscriptionStart,
      'subscriptionEnd': stock.subscriptionEnd,
      'latestSnapshotAt': latest?.capturedAt,
      'latestSnapshotSource': latest?.source,
      'latestCompetitionRate': latest?.aggregate.competitionRate,
      'institutionCompetitionRate':
          stock.fundamentals.institutionCompetitionRate,
      'institutionParticipants': stock.fundamentals.institutionParticipants,
      'lockupCommitmentRate': stock.fundamentals.lockupCommitmentRate,
      'issues': coverageIssuesFor(stock, today),
    };
  }).toList();

  final sortedSourceCounts = latestSourceCounts.entries.toList()
    ..sort((a, b) => b.value.compareTo(a.value));

  return {
    'schemaVersion': schemaVersion,
    'generatedAt': generatedAt.toIso8601String(),
    'totals': {
      'generatedStocks': normalized.length,
      'activeStocks': active.length,
      'upcomingNext7Days': upcomingNext7Days.length,
    },
    'activeReadiness': {
      'missingLatestSnapshotAt': missingLatestSnapshotAt,
      'missingLatestCompetitionRate': missingLatestCompetitionRate,
      'missingBrokerLevelCompetition': missingBrokerMetrics,
      'missingInstitutionCompetitionRate': missingInstitutionCompetition,
      'missingInstitutionParticipants': missingInstitutionParticipants,
      'missingLockupCommitmentRate': missingLockupCommitmentRate,
      'liveLikeSourceCount': liveLikeSourceCount,
      'finutsSourceCount': finutsSourceCount,
      'ocrSourceCount': ocrSourceCount,
    },
    'activeLatestSourceCounts': {
      for (final entry in sortedSourceCounts) entry.key: entry.value,
    },
    'activeSnapshotFreshness': freshnessBuckets,
    'activeSamples': activeSamples,
    'upcomingSamples': upcomingNext7Days.take(5).map((stock) {
      return {
        'id': stock.id,
        'company': stock.company,
        'path': 'stocks/${stock.id}.json',
        'subscriptionStart': stock.subscriptionStart,
        'subscriptionEnd': stock.subscriptionEnd,
        'leadManagers': stock.leadManagers,
      };
    }).toList(),
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
      industry: stock.industry.trim().isEmpty
          ? existing.industry
          : stock.industry,
      subscriptionStart: stock.subscriptionStart ?? existing.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd ?? existing.subscriptionEnd,
      demandForecastStart:
          stock.demandForecastStart ?? existing.demandForecastStart,
      demandForecastEnd: stock.demandForecastEnd ?? existing.demandForecastEnd,
      refundDate: stock.refundDate ?? existing.refundDate,
      listingDate: stock.listingDate ?? existing.listingDate,
      lockupReleaseDate: stock.lockupReleaseDate ?? existing.lockupReleaseDate,
      generalSharesDate: stock.generalSharesDate ?? existing.generalSharesDate,
      cbBwDate: stock.cbBwDate ?? existing.cbBwDate,
      securityType: stock.securityType ?? existing.securityType,
      leadManagers: {...existing.leadManagers, ...stock.leadManagers}.toList(),
      sourceIdentifiers: existing.identifiers.merge(stock.identifiers),
      fundamentals: existing.fundamentals.merge(stock.fundamentals),
      outcome: stock.outcome ?? existing.outcome,
      snapshots: [...existing.snapshots, ...stock.snapshots],
    );
  }
  return byId.values.toList();
}

List<IpoCompetitionStock> mergeStocksByIdentity(
  List<IpoCompetitionStock> stocks,
) {
  final byKey = <String, IpoCompetitionStock>{};
  for (final stock in stocks) {
    final key = stockIdentityKey(stock);
    final existing = byKey[key];
    if (existing == null) {
      byKey[key] = stock;
      continue;
    }
    final preferred = preferStock(existing, stock);
    final secondary = identical(preferred, existing) ? stock : existing;
    byKey[key] = mergePreferredStock(preferred, secondary);
  }
  return byKey.values.toList();
}

List<IpoCompetitionStock> applyGeneralSharesBackfill(
  List<IpoCompetitionStock> stocks,
) {
  return stocks.map(backfillGeneralSharesStock).toList();
}

IpoCompetitionStock backfillGeneralSharesStock(IpoCompetitionStock stock) {
  final inferredDate = inferGeneralSharesDate(stock);
  final inferredType = inferGeneralSharesSecurityType(stock, inferredDate);
  if (inferredDate == null && inferredType == null) {
    return stock;
  }
  return IpoCompetitionStock(
    id: stock.id,
    company: stock.company,
    market: stock.market,
    industry: stock.industry,
    subscriptionStart: stock.subscriptionStart,
    subscriptionEnd: stock.subscriptionEnd,
    demandForecastStart: stock.demandForecastStart,
    demandForecastEnd: stock.demandForecastEnd,
    refundDate: stock.refundDate,
    listingDate: stock.listingDate,
    lockupReleaseDate: stock.lockupReleaseDate,
    generalSharesDate: stock.generalSharesDate ?? inferredDate,
    cbBwDate: stock.cbBwDate,
    securityType: stock.securityType ?? inferredType,
    leadManagers: stock.leadManagers,
    sourceIdentifiers: stock.sourceIdentifiers,
    fundamentals: stock.fundamentals,
    outcome: stock.outcome,
    snapshots: stock.snapshots,
  );
}

String? inferGeneralSharesSecurityType(
  IpoCompetitionStock stock,
  String? inferredDate,
) {
  if (stock.normalizedSecurityType != null) {
    return null;
  }
  if (stock.normalizedGeneralSharesDate != null || inferredDate != null) {
    return 'GENERAL_SHARES';
  }
  return null;
}

String? inferGeneralSharesDate(IpoCompetitionStock stock) {
  if (stock.normalizedGeneralSharesDate != null) {
    return null;
  }
  if (!isLikelyGeneralSharesStock(stock)) {
    return null;
  }
  final start =
      normalizeDate(stock.subscriptionStart) ?? stock.subscriptionStart;
  final end = normalizeDate(stock.subscriptionEnd) ?? stock.subscriptionEnd;
  if (start != null && end != null) {
    return start.compareTo(end) <= 0 ? end : start;
  }
  return end ?? start;
}

bool isLikelyGeneralSharesStock(IpoCompetitionStock stock) {
  if (isSpacStock(stock)) {
    return false;
  }
  if (stock.resolvedListingDate != null) {
    return false;
  }
  final identifiers = stock.identifiers;
  if (identifiers.corpCode != null || identifiers.stockCode != null) {
    return false;
  }
  if (stock.industry.trim().isNotEmpty) {
    return false;
  }

  final offerPrice = stock.fundamentals.offerPrice;
  if (offerPrice == null || offerPrice <= 0) {
    return false;
  }
  final priceBandMin = stock.fundamentals.priceBandMin ?? 0;
  final priceBandMax = stock.fundamentals.priceBandMax ?? 0;
  if (priceBandMin != 0 || priceBandMax != 0) {
    return false;
  }
  return true;
}

String stockIdentityKey(IpoCompetitionStock stock) {
  final subscriptionKey = canonicalSubscriptionKey(
    stock.identifiers.subscriptionKey,
  );
  if (subscriptionKey.isNotEmpty) {
    return 'sub:$subscriptionKey';
  }
  final identifierKey = preferredIdentifierKey(stock.identifiers);
  if (identifierKey != null) {
    return identifierKey;
  }
  final company = normalizeCompanyIdentity(stock.company);
  final start =
      normalizeDate(stock.subscriptionStart) ?? stock.subscriptionStart;
  final end = normalizeDate(stock.subscriptionEnd) ?? stock.subscriptionEnd;
  if (company.isNotEmpty &&
      ((start?.isNotEmpty ?? false) || (end?.isNotEmpty ?? false))) {
    return 'company:$company:${start ?? ''}:${end ?? ''}';
  }
  return 'id:${safeId(stock.id)}';
}

String? preferredIdentifierKey(IpoStockIdentifiers identifiers) {
  String? clean(String? value) {
    final trimmed = value?.trim();
    return trimmed == null || trimmed.isEmpty ? null : trimmed;
  }

  final kindCode = clean(identifiers.kindCode);
  if (kindCode != null) {
    return 'kind:$kindCode';
  }
  final corpCode = clean(identifiers.corpCode);
  if (corpCode != null) {
    return 'corp:$corpCode';
  }
  final stockCode = clean(identifiers.stockCode);
  if (stockCode != null) {
    return 'stock:$stockCode';
  }
  final isin = clean(identifiers.isin);
  if (isin != null) {
    return 'isin:$isin';
  }
  return null;
}

String canonicalSubscriptionKey(String? raw) {
  final value = (raw ?? '').trim();
  if (value.isEmpty) {
    return '';
  }
  final match = RegExp(r'^(.*)_(\d{8})_(\d{8})$').firstMatch(value);
  if (match == null) {
    return value;
  }
  final company = normalizeCompanyIdentity(match.group(1) ?? '');
  final start = match.group(2) ?? '';
  final end = match.group(3) ?? '';
  if (company.isEmpty) {
    return value;
  }
  return '${company}_${start}_$end';
}

String normalizeCompanyIdentity(String value) {
  var normalized = normalizeLookup(value);
  final spacMatch = RegExp(r'^(.*?)(?:제)?(\d+)호스팩$').firstMatch(normalized);
  if (spacMatch != null) {
    final prefix = spacMatch.group(1) ?? '';
    final number = spacMatch.group(2) ?? '';
    return '$prefix스팩$number호';
  }
  return normalized;
}

IpoCompetitionStock preferStock(
  IpoCompetitionStock left,
  IpoCompetitionStock right,
) {
  final leftScore = stockCompletenessScore(left);
  final rightScore = stockCompletenessScore(right);
  if (leftScore != rightScore) {
    return leftScore > rightScore ? left : right;
  }

  final leftId = safeId(left.id);
  final rightId = safeId(right.id);
  final leftHasAsciiOnly = RegExp(r'^[a-z0-9_]+$').hasMatch(leftId);
  final rightHasAsciiOnly = RegExp(r'^[a-z0-9_]+$').hasMatch(rightId);
  if (leftHasAsciiOnly != rightHasAsciiOnly) {
    return leftHasAsciiOnly ? left : right;
  }
  return leftId.compareTo(rightId) <= 0 ? left : right;
}

int stockCompletenessScore(IpoCompetitionStock stock) {
  var score = 0;
  if (stock.company.trim().isNotEmpty) {
    score += 1;
  }
  if (stock.market.trim().isNotEmpty) {
    score += 1;
  }
  if (stock.industry.trim().isNotEmpty) {
    score += 4;
  }
  if (stock.subscriptionStart != null) {
    score += 2;
  }
  if (stock.subscriptionEnd != null) {
    score += 2;
  }
  if (stock.listingDate != null || stock.outcome?.listingDate != null) {
    score += 2;
  }
  if (stock.generalSharesDate != null) {
    score += 3;
  }
  if (stock.securityType != null) {
    score += 3;
  }
  if (stock.leadManagers.isNotEmpty) {
    score += min(stock.leadManagers.length, 3);
  }

  final identifiers = stock.identifiers;
  if (identifiers.corpCode != null) {
    score += 2;
  }
  if (identifiers.stockCode != null) {
    score += 2;
  }
  if (identifiers.kindCode != null) {
    score += 1;
  }
  if (identifiers.isin != null) {
    score += 1;
  }

  final fundamentals = stock.fundamentals;
  if (fundamentals.offerPrice != null) {
    score += 2;
  }
  if (fundamentals.priceBandMin != null || fundamentals.priceBandMax != null) {
    score += 2;
  }
  if (fundamentals.topBandConfirmation != null) {
    score += 1;
  }
  if (fundamentals.publicAllocationShares != null) {
    score += 2;
  }
  if (fundamentals.institutionCompetitionRate != null) {
    score += 6;
  }
  if (fundamentals.institutionParticipants != null) {
    score += 4;
  }
  if (fundamentals.lockupCommitmentRate != null) {
    score += 4;
  }
  if (fundamentals.floatRate != null) {
    score += 2;
  }
  if (stock.outcome != null) {
    score += 2;
  }
  if (stock.snapshots.isNotEmpty) {
    score += 3;
  }
  if (hasBrokerLevelSnapshot(stock)) {
    score += 2;
  }
  return score;
}

IpoCompetitionStock mergePreferredStock(
  IpoCompetitionStock preferred,
  IpoCompetitionStock secondary,
) {
  final mergedSnapshots = <IpoCompetitionSnapshot>[
    ...preferred.snapshots,
    ...secondary.snapshots,
  ];
  return IpoCompetitionStock(
    id: preferred.id,
    company: preferred.company.trim().isEmpty
        ? secondary.company
        : preferred.company,
    market: preferred.market.trim().isEmpty
        ? secondary.market
        : preferred.market,
    industry: preferred.industry.trim().isEmpty
        ? secondary.industry
        : preferred.industry,
    subscriptionStart:
        preferred.subscriptionStart ?? secondary.subscriptionStart,
    subscriptionEnd: preferred.subscriptionEnd ?? secondary.subscriptionEnd,
    demandForecastStart:
        preferred.demandForecastStart ?? secondary.demandForecastStart,
    demandForecastEnd:
        preferred.demandForecastEnd ?? secondary.demandForecastEnd,
    refundDate: preferred.refundDate ?? secondary.refundDate,
    listingDate: preferred.listingDate ?? secondary.listingDate,
    lockupReleaseDate:
        preferred.lockupReleaseDate ?? secondary.lockupReleaseDate,
    generalSharesDate:
        preferred.generalSharesDate ?? secondary.generalSharesDate,
    cbBwDate: preferred.cbBwDate ?? secondary.cbBwDate,
    securityType: preferred.securityType ?? secondary.securityType,
    leadManagers: mergeOrderedStrings(
      preferred.leadManagers,
      secondary.leadManagers,
    ),
    sourceIdentifiers: secondary.identifiers.merge(preferred.identifiers),
    fundamentals: secondary.fundamentals.merge(preferred.fundamentals),
    outcome: mergePreferredOutcome(preferred.outcome, secondary.outcome),
    snapshots: mergedSnapshots,
  );
}

IpoOutcome? mergePreferredOutcome(
  IpoOutcome? preferred,
  IpoOutcome? secondary,
) {
  if (preferred == null) {
    return secondary;
  }
  if (secondary == null) {
    return preferred;
  }
  return IpoOutcome(
    listingDate: preferred.listingDate ?? secondary.listingDate,
    openReturnRate: preferred.openReturnRate ?? secondary.openReturnRate,
    highReturnRate: preferred.highReturnRate ?? secondary.highReturnRate,
    closeReturnRate: preferred.closeReturnRate ?? secondary.closeReturnRate,
    sourceUrl: preferred.sourceUrl ?? secondary.sourceUrl,
  );
}

List<String> mergeOrderedStrings(
  List<String> preferred,
  List<String> secondary,
) {
  final seen = <String>{};
  final merged = <String>[];
  for (final value in [...preferred, ...secondary]) {
    final normalized = canonicalBrokerName(value).trim();
    if (normalized.isEmpty) {
      continue;
    }
    final key = normalizeLookup(normalized);
    if (seen.add(key)) {
      merged.add(normalized);
    }
  }
  return merged;
}

Future<void> deleteOrphanedStockFiles(
  Directory stockDir,
  Set<String> selectedIds,
) async {
  if (!await stockDir.exists()) {
    return;
  }
  await for (final entity in stockDir.list()) {
    if (entity is! File || !entity.path.endsWith('.json')) {
      continue;
    }
    final name = entity.uri.pathSegments.isEmpty
        ? entity.path
        : entity.uri.pathSegments.last;
    final id = name.replaceFirst(RegExp(r'\.json$'), '');
    if (!selectedIds.contains(id)) {
      await entity.delete();
    }
  }
}

List<IpoCompetitionStock> mergeManualFundamentalsOverrides(
  List<IpoCompetitionStock> stocks,
  List<IpoManualFundamentalsOverride> overrides,
) {
  final byId = <String, IpoManualFundamentalsOverride>{};
  final byCompany = <String, IpoManualFundamentalsOverride>{};
  for (final override in overrides) {
    if (override.id.isNotEmpty) {
      byId[safeId(override.id)] = override;
    }
    if (override.company.isNotEmpty) {
      byCompany[normalizeLookup(override.company)] = override;
    }
  }

  return stocks.map((stock) {
    final override =
        byId[safeId(stock.id)] ?? byCompany[normalizeLookup(stock.company)];
    if (override == null) {
      return stock;
    }
    return IpoCompetitionStock(
      id: stock.id,
      company: stock.company,
      market: stock.market,
      industry: stock.industry.trim().isEmpty
          ? override.industry
          : stock.industry,
      subscriptionStart: stock.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd,
      demandForecastStart: stock.demandForecastStart,
      demandForecastEnd: stock.demandForecastEnd,
      refundDate: stock.refundDate,
      listingDate: stock.listingDate,
      lockupReleaseDate: stock.lockupReleaseDate,
      generalSharesDate: stock.generalSharesDate,
      cbBwDate: stock.cbBwDate,
      securityType: stock.securityType,
      leadManagers: stock.leadManagers,
      sourceIdentifiers: stock.sourceIdentifiers,
      fundamentals: stock.fundamentals.merge(override.fundamentals),
      outcome: stock.outcome,
      snapshots: stock.snapshots,
    );
  }).toList();
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
          industry: stock.industry,
          subscriptionStart: stock.subscriptionStart,
          subscriptionEnd: stock.subscriptionEnd,
          demandForecastStart: stock.demandForecastStart,
          demandForecastEnd: stock.demandForecastEnd,
          refundDate: stock.refundDate,
          listingDate: stock.listingDate,
          lockupReleaseDate: stock.lockupReleaseDate,
          generalSharesDate: stock.generalSharesDate,
          cbBwDate: stock.cbBwDate,
          securityType: stock.securityType,
          leadManagers: overrides[safeId(stock.id)]!,
          sourceIdentifiers: stock.identifiers,
          fundamentals: const IpoFundamentals(
            offerPrice: null,
            priceBandMin: null,
            priceBandMax: null,
            topBandConfirmation: null,
            institutionCompetitionRate: null,
            institutionParticipants: null,
            lockupCommitmentRate: null,
            floatRate: null,
            marketCapKrw: null,
            publicAllocationShares: null,
            hasPutbackRight: false,
            putbackSummary: null,
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
      industry: stock.industry,
      subscriptionStart: stock.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd,
      demandForecastStart: stock.demandForecastStart,
      demandForecastEnd: stock.demandForecastEnd,
      refundDate: stock.refundDate,
      listingDate: stock.listingDate,
      lockupReleaseDate: stock.lockupReleaseDate,
      generalSharesDate: stock.generalSharesDate,
      cbBwDate: stock.cbBwDate,
      securityType: stock.securityType,
      leadManagers: stock.leadManagers,
      sourceIdentifiers: stock.identifiers,
      fundamentals: stock.fundamentals.merge(
        IpoFundamentals(
          offerPrice: outcomeRow.offerPrice,
          priceBandMin: null,
          priceBandMax: null,
          topBandConfirmation: null,
          institutionCompetitionRate: null,
          institutionParticipants: null,
          lockupCommitmentRate: null,
          floatRate: null,
          marketCapKrw: null,
          publicAllocationShares: null,
          hasPutbackRight: false,
          putbackSummary: null,
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
    final seen = <String>{};
    final matches = <IpoBrokerSnapshotRow>[];
    for (final row in [
      ...?byId[safeId(stock.id)],
      ...?byCompany[normalizeLookup(stock.company)],
    ]) {
      final key = _brokerSnapshotRowMergeKey(row);
      if (seen.add(key)) {
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
      industry: stock.industry,
      subscriptionStart: stock.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd,
      demandForecastStart: stock.demandForecastStart,
      demandForecastEnd: stock.demandForecastEnd,
      refundDate: stock.refundDate,
      listingDate: stock.listingDate,
      lockupReleaseDate: stock.lockupReleaseDate,
      generalSharesDate: stock.generalSharesDate,
      cbBwDate: stock.cbBwDate,
      securityType: stock.securityType,
      leadManagers: stock.leadManagers,
      sourceIdentifiers: stock.identifiers,
      fundamentals: stock.fundamentals,
      outcome: stock.outcome,
      snapshots: [...stock.snapshots, ...extraSnapshots],
    );
  }).toList();
}

String _brokerSnapshotRowMergeKey(IpoBrokerSnapshotRow row) {
  final brokerKey = row.brokers
      .map(
        (broker) => [
          normalizeLookup(broker.name),
          broker.offeredShares,
          broker.subscribedShares,
          broker.offerPrice ?? '',
          broker.depositRate ?? '',
          broker.competitionRate ?? '',
          broker.proportionalCompetitionRate ?? '',
          broker.equalAllocationShares ?? '',
          broker.proportionalAllocationShares ?? '',
          broker.expectedEqualShares ?? '',
          broker.applicationCount ?? '',
        ].join(':'),
      )
      .join('|');
  return [
    safeId(row.id ?? ''),
    normalizeLookup(row.company ?? ''),
    row.capturedAt,
    row.source,
    row.sourceUrl ?? '',
    brokerKey,
  ].join('||');
}

String _competitionSnapshotKey(IpoCompetitionSnapshot snapshot) {
  final brokerKey = snapshot.brokers
      .map(
        (broker) => [
          normalizeLookup(broker.name),
          broker.offeredShares,
          broker.subscribedShares,
          broker.offerPrice ?? '',
          broker.depositRate ?? '',
          broker.feeKrw ?? '',
          broker.competitionRate ?? '',
          broker.equalCompetitionRate ?? '',
          broker.proportionalCompetitionRate ?? '',
          broker.equalAllocationShares ?? '',
          broker.proportionalAllocationShares ?? '',
          broker.expectedEqualShares ?? '',
          broker.applicationCount ?? '',
        ].join(':'),
      )
      .join('|');
  return [
    snapshot.capturedAt,
    snapshot.source,
    snapshot.sourceUrl ?? '',
    snapshot.aggregateCompetitionRate ?? '',
    brokerKey,
  ].join('||');
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

Future<List<IpoManualFundamentalsOverride>> _loadManualFundamentalsRows(
  String path,
) async {
  if (path.trim().isEmpty) {
    return const [];
  }
  final file = File(path);
  if (!await file.exists()) {
    final normalizedPath = path.replaceAll('\\', '/');
    if (normalizedPath.endsWith('/manual_fundamentals.json') ||
        normalizedPath == 'data/manual_fundamentals.json' ||
        normalizedPath == 'manual_fundamentals.json') {
      return const [];
    }
    stderr.writeln('Manual fundamentals file not found: $path.');
    return const [];
  }
  final decoded = jsonDecode(await file.readAsString());
  final rawRows = decoded is Map<String, Object?> && decoded['stocks'] is List
      ? decoded['stocks'] as List
      : decoded is List
      ? decoded
      : throw const FormatException(
          'Manual fundamentals file must be a JSON array or an object with "stocks".',
        );
  return rawRows
      .whereType<Map<String, Object?>>()
      .map(IpoManualFundamentalsOverride.fromJson)
      .toList();
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

List<IpoIdentifierRow> alignIdentifierRowsToStocks(
  List<IpoCompetitionStock> stocks,
  List<IpoIdentifierRow> rows,
) {
  if (stocks.isEmpty || rows.isEmpty) {
    return rows;
  }
  final byId = <String, IpoCompetitionStock>{
    for (final stock in stocks) safeId(stock.id): stock,
  };
  return rows.map((row) {
    final rowId = row.id == null ? null : safeId(row.id!);
    final stock = rowId == null ? null : byId[rowId];
    if (stock == null) {
      return row;
    }
    return IpoIdentifierRow(
      id: row.id,
      company: row.company ?? stock.company,
      identifiers: scopeIdentifiersForMatchedStock(
        stock: stock,
        identifiers: row.identifiers,
        matchType: 'id',
      ),
    );
  }).toList();
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
    IpoIdentifierRow? row;
    String? matchType;
    row = byId[safeId(stock.id)];
    if (row != null) {
      matchType = 'id';
    } else {
      final subscriptionKey = stock.identifiers.subscriptionKey;
      row = bySubscriptionKey[subscriptionKey];
      if (row != null) {
        matchType = 'subscription';
      } else {
        row = byCompany[normalizeLookup(stock.company)];
        if (row != null) {
          matchType = 'company';
        }
      }
    }
    if (row == null) {
      return stock;
    }
    final scopedIdentifiers = scopeIdentifiersForMatchedStock(
      stock: stock,
      identifiers: row.identifiers,
      matchType: matchType ?? 'unknown',
    );
    return IpoCompetitionStock(
      id: stock.id,
      company: stock.company,
      market: stock.market,
      industry: stock.industry,
      subscriptionStart: stock.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd,
      demandForecastStart: stock.demandForecastStart,
      demandForecastEnd: stock.demandForecastEnd,
      refundDate: stock.refundDate,
      listingDate: stock.listingDate,
      lockupReleaseDate: stock.lockupReleaseDate,
      generalSharesDate: stock.generalSharesDate,
      cbBwDate: stock.cbBwDate,
      securityType: stock.securityType,
      leadManagers: stock.leadManagers,
      sourceIdentifiers: stock.identifiers.merge(scopedIdentifiers),
      fundamentals: stock.fundamentals,
      outcome: stock.outcome,
      snapshots: stock.snapshots,
    );
  }).toList();
}

IpoStockIdentifiers scopeIdentifiersForMatchedStock({
  required IpoCompetitionStock stock,
  required IpoStockIdentifiers identifiers,
  required String matchType,
}) {
  final hasEventDates =
      (stock.subscriptionStart?.trim().isNotEmpty ?? false) ||
      (stock.subscriptionEnd?.trim().isNotEmpty ?? false);
  var subscriptionKey = identifiers.subscriptionKey.trim();
  if (hasEventDates && (matchType == 'id' || matchType == 'company')) {
    subscriptionKey = subscriptionKeyFor(
      company: stock.company,
      subscriptionStart: stock.subscriptionStart,
      subscriptionEnd: stock.subscriptionEnd,
    );
  }

  final finutsKindCode = RegExp(r'^finuts_(\d+)_').firstMatch(safeId(stock.id));
  final eventKindCode =
      stock.sourceIdentifiers?.kindCode?.trim().isNotEmpty ?? false
      ? stock.sourceIdentifiers!.kindCode!.trim()
      : finutsKindCode?.group(1);
  var kindCode = identifiers.kindCode?.trim();
  if (eventKindCode != null &&
      eventKindCode.isNotEmpty &&
      (matchType == 'id' || matchType == 'company')) {
    kindCode = eventKindCode;
  }

  return IpoStockIdentifiers(
    subscriptionKey: subscriptionKey,
    normalizedCompany: identifiers.normalizedCompany,
    corpCode: identifiers.corpCode,
    stockCode: identifiers.stockCode,
    kindCode: kindCode,
    isin: identifiers.isin,
  );
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

String? _normalizeNaverIpoCode(String? raw) {
  final value = raw?.trim().toUpperCase() ?? '';
  if (value.isEmpty) {
    return null;
  }
  if (RegExp(r'^A[A-Z0-9]{6}$').hasMatch(value)) {
    return value;
  }
  if (RegExp(r'^\d{6}$').hasMatch(value)) {
    return 'A$value';
  }
  return null;
}

String canonicalBrokerName(String raw) {
  final key = normalizeLookup(raw);
  if (key == normalizeLookup('엔에이치투자증권') ||
      key == normalizeLookup('NH증권') ||
      key == normalizeLookup('NH')) {
    return 'NH투자증권';
  }
  if (key == normalizeLookup('케이비증권') || key == normalizeLookup('KB')) {
    return 'KB증권';
  }
  if (key == normalizeLookup('한국')) {
    return '한국투자증권';
  }
  if (key == normalizeLookup('미래')) {
    return '미래에셋증권';
  }
  if (key == normalizeLookup('현대차')) {
    return '현대차증권';
  }
  if (key == normalizeLookup('신한')) {
    return '신한투자증권';
  }
  if (key == normalizeLookup('대신')) {
    return '대신증권';
  }
  if (key == normalizeLookup('하나')) {
    return '하나증권';
  }
  if (key == normalizeLookup('삼성')) {
    return '삼성증권';
  }
  if (key == normalizeLookup('SK')) {
    return 'SK증권';
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
    industry: stock.industry,
    subscriptionStart: stock.subscriptionStart,
    subscriptionEnd: stock.subscriptionEnd,
    demandForecastStart: stock.demandForecastStart,
    demandForecastEnd: stock.demandForecastEnd,
    refundDate: stock.refundDate,
    listingDate: stock.listingDate,
    lockupReleaseDate: stock.lockupReleaseDate,
    generalSharesDate: stock.generalSharesDate,
    cbBwDate: stock.cbBwDate,
    securityType: stock.securityType,
    leadManagers: const [],
    sourceIdentifiers: stock.identifiers,
    fundamentals: IpoFundamentals(
      offerPrice: offerPrice,
      priceBandMin: null,
      priceBandMax: null,
      topBandConfirmation: null,
      institutionCompetitionRate: institutionCompetitionRate,
      institutionParticipants: institutionParticipants,
      lockupCommitmentRate: lockupCommitmentRate,
      floatRate: null,
      marketCapKrw: marketCapKrw,
      publicAllocationShares: publicAllocationShares,
      hasPutbackRight: false,
      putbackSummary: null,
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
    required this.topBandConfirmation,
    required this.institutionCompetitionRate,
    required this.institutionParticipants,
    required this.lockupCommitmentRate,
    required this.floatRate,
    required this.marketCapKrw,
    required this.publicAllocationShares,
    required this.hasPutbackRight,
    required this.putbackSummary,
  });

  final int? offerPrice;
  final int? priceBandMin;
  final int? priceBandMax;
  final bool? topBandConfirmation;
  final double? institutionCompetitionRate;
  final int? institutionParticipants;
  final double? lockupCommitmentRate;
  final double? floatRate;
  final int? marketCapKrw;
  final int? publicAllocationShares;
  final bool hasPutbackRight;
  final String? putbackSummary;

  factory IpoFundamentals.fromJson(Map<String, Object?> json) {
    final putbackSummary =
        readString(json, 'putbackSummary') ??
        readString(json, 'putbackRightSummary') ??
        readString(json, 'putbackNote');
    final explicitPutbackRight =
        json['hasPutbackRight'] as bool? ?? json['putbackRight'] as bool?;
    final inferredPutbackRight = inferPutbackRightFromSummary(putbackSummary);
    return IpoFundamentals(
      offerPrice: readOptionalInt(json['offerPrice']),
      priceBandMin: readOptionalInt(json['priceBandMin']),
      priceBandMax: readOptionalInt(json['priceBandMax']),
      topBandConfirmation: json['topBandConfirmation'] as bool?,
      institutionCompetitionRate: readDouble(
        json['institutionCompetitionRate'],
      ),
      institutionParticipants: readOptionalInt(json['institutionParticipants']),
      lockupCommitmentRate: readRatio(json['lockupCommitmentRate']),
      floatRate: readRatio(json['floatRate']),
      marketCapKrw: readOptionalInt(json['marketCapKrw']),
      publicAllocationShares: readOptionalInt(json['publicAllocationShares']),
      hasPutbackRight: explicitPutbackRight ?? inferredPutbackRight ?? false,
      putbackSummary: putbackSummary,
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
      topBandConfirmation: other.topBandConfirmation ?? topBandConfirmation,
      institutionCompetitionRate:
          other.institutionCompetitionRate ?? institutionCompetitionRate,
      institutionParticipants:
          other.institutionParticipants ?? institutionParticipants,
      lockupCommitmentRate: other.lockupCommitmentRate ?? lockupCommitmentRate,
      floatRate: other.floatRate ?? floatRate,
      marketCapKrw: other.marketCapKrw ?? marketCapKrw,
      publicAllocationShares:
          other.publicAllocationShares ?? publicAllocationShares,
      hasPutbackRight: other.hasPutbackRight || hasPutbackRight,
      putbackSummary: other.putbackSummary ?? putbackSummary,
    );
  }

  Map<String, Object?> toJson() {
    return {
      'offerPrice': offerPrice,
      'priceBandMin': priceBandMin,
      'priceBandMax': priceBandMax,
      'topBandConfirmation': topBandConfirmation,
      'institutionCompetitionRate': institutionCompetitionRate,
      'institutionParticipants': institutionParticipants,
      'lockupCommitmentRate': lockupCommitmentRate,
      'floatRate': floatRate,
      'marketCapKrw': marketCapKrw,
      'publicAllocationShares': publicAllocationShares,
      'hasPutbackRight': hasPutbackRight,
      'putbackSummary': putbackSummary,
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
    this.expectedEqualShares,
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
  final double? expectedEqualShares;
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
      expectedEqualShares: readDouble(json['expectedEqualShares']),
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
      expectedEqualShares: expectedEqualShares,
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
      'expectedEqualShares': expectedEqualShares == null
          ? null
          : roundDouble(expectedEqualShares!, 4),
      'applicationCount': applicationCount,
    };
  }

  double? get equalExpectedSharesPerAccount {
    if (expectedEqualShares != null && expectedEqualShares! > 0) {
      return expectedEqualShares;
    }
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

int snapshotSourcePriority(String source) {
  final normalized = source.trim().toLowerCase();
  if (normalized.contains('finuts')) {
    return 100;
  }
  if (normalized.contains('naver_calculator')) {
    return 90;
  }
  if (normalized.contains('ipostock')) {
    return 80;
  }
  if (normalized.contains('38_news')) {
    return 60;
  }
  if (normalized.contains('youtube')) {
    return 40;
  }
  if (normalized.contains('community') || normalized.contains('estimate')) {
    return 20;
  }
  return 0;
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
  bool ignoreLiveCompetitionForScoring = false,
  bool ignoreLiveCompetitionForReturn = false,
}) {
  final effectiveCalibration = calibration ?? _analysisCalibration;
  final isSpac = isSpacStock(stock);
  final latestRate = ignoreLiveCompetitionForReturn
      ? null
      : stock.latestSnapshot?.aggregate.competitionRate;
  final institutionScore = scoreInstitutionDemand(stock.fundamentals);
  final demandStrengthScore = isSpac ? 0 : scoreDemandStrengthForStock(stock);
  final spacMomentumScore = isSpac ? scoreSpacMomentum(stock) : 0;
  final spacVolatilityScore = isSpac ? scoreSpacVolatility(stock) : 0;
  final lockupScore = isSpac
      ? 0
      : scoreLockup(stock.fundamentals.lockupCommitmentRate);
  final pricingScore = scorePricing(stock.fundamentals);
  final marketScore = isSpac ? scoreMarket(stock.market) : 0;
  final managerScore = isSpac ? scoreLeadManagers(stock.leadManagers) : 0;
  final recencyScore = isSpac ? scoreRecency(stock.subscriptionEnd) : 0;
  final dataScore = isSpac ? scoreDataCompleteness(stock) : 0;
  final competitionScore = scoreCompetitionForStock(
    stock,
    ignoreLiveCompetition: ignoreLiveCompetitionForScoring,
  );
  late final Map<String, int> factors;
  late final int total;
  late final int demandScore;
  late final int valueScore;
  late final String scoreMode;
  if (isSpac) {
    factors = <String, int>{
      'competition': competitionScore,
      'institutionDemand': institutionScore,
      'spacMomentum': spacMomentumScore,
      'spacVolatility': spacVolatilityScore,
      'market': marketScore,
      'leadManagers': managerScore,
      'recency': recencyScore,
      'dataCompleteness': dataScore,
    };
    final normalizedTotal = normalizedFactorScore(factors);
    total = clampInt(normalizedTotal, 0, spacScoreCeilingFor(stock));
    demandScore = total;
    valueScore = total;
    scoreMode = 'spac_live_balanced';
  } else {
    final demandFactors = <String, int>{
      'institutionDemand': institutionScore,
      'demandStrength': demandStrengthScore,
      'lockupCommitment': lockupScore,
      'pricing': pricingScore,
    };
    final valueFactors = <String, int>{};
    if (stock.fundamentals.marketCapKrw != null) {
      valueFactors['marketCap'] = scoreMarketCapForStock(stock);
    }
    if (stock.fundamentals.publicAllocationShares != null) {
      valueFactors['publicAllocation'] = scorePublicAllocationForStock(stock);
    }
    if (stock.leadManagers.isNotEmpty) {
      valueFactors['leadManagerCoverage'] = scoreLeadManagerCoverageForStock(
        stock,
      );
    }
    if (stock.fundamentals.hasPutbackRight) {
      valueFactors['putbackRight'] = scorePutbackRightForStock(stock);
    }
    demandScore = normalizedFactorScore(demandFactors);
    valueScore = valueFactors.isEmpty
        ? demandScore
        : normalizedFactorScore(valueFactors);
    factors = <String, int>{...demandFactors, ...valueFactors};
    total = clampInt((demandScore * 0.82 + valueScore * 0.18).round(), 0, 100);
    scoreMode = 'general_pre_subscription_demand_value';
  }
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
  final level = decisionLevelFor(total, confidence);
  final grade = level == 'insufficient_data' ? '-' : gradeFor(total);

  return IpoAnalysis(
    score: IpoScore(
      overall: total,
      grade: grade,
      confidence: confidence,
      factors: factors,
      demand: demandScore,
      value: valueScore,
      mode: scoreMode,
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
        'feeKrw': null,
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
      'scoreMode': scoreMode,
      'demandScore': demandScore,
      'valueScore': valueScore,
    },
    methodVersion: 'ipo-score-v5-demand-value',
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
  const IpoAnalysisCalibration({this.spac, this.general});

  final IpoSpacCalibration? spac;
  final IpoGeneralCalibration? general;

  bool get hasSpac => spac != null && spac!.sampleCount > 0;
  bool get hasGeneral => general != null && general!.sampleCount > 0;

  Map<String, Object?> toJson() {
    return {'spac': spac?.toJson(), 'general': general?.toJson()};
  }
}

class IpoGeneralCalibration {
  const IpoGeneralCalibration({
    required this.sampleCount,
    required this.averageReferenceError,
    required this.medianReferenceError,
    required this.recencyWeightedReferenceError,
    required this.dampedAdjustment,
    required this.maxAdjustment,
  });

  final int sampleCount;
  final double? averageReferenceError;
  final double? medianReferenceError;
  final double? recencyWeightedReferenceError;
  final double dampedAdjustment;
  final double maxAdjustment;

  Map<String, Object?> toJson() {
    return {
      'sampleCount': sampleCount,
      'averageReferenceError': averageReferenceError,
      'medianReferenceError': medianReferenceError,
      'recencyWeightedReferenceError': recencyWeightedReferenceError,
      'dampedAdjustment': dampedAdjustment,
      'maxAdjustment': maxAdjustment,
    };
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
    required this.demand,
    required this.value,
    required this.mode,
  });

  final int overall;
  final String grade;
  final double confidence;
  final Map<String, int> factors;
  final int demand;
  final int value;
  final String mode;

  Map<String, Object?> toJson() {
    return {
      'overall': overall,
      'grade': grade,
      'confidence': roundDouble(confidence, 2),
      'factors': factors,
      'demand': demand,
      'value': value,
      'mode': mode,
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
    return 22;
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

bool isBeforeOrDuringSubscription(IpoCompetitionStock stock) {
  final now = DateTime.now();
  final today = DateTime(now.year, now.month, now.day);
  final start = parseDate(stock.subscriptionStart);
  final end = parseDate(stock.subscriptionEnd) ?? start;
  if (start == null && end == null) {
    return false;
  }
  final effectiveEnd = end ?? start!;
  return !effectiveEnd.isBefore(today);
}

int scoreFloatForStock(IpoCompetitionStock stock) {
  final rate = stock.fundamentals.floatRate;
  final direct = scoreFloat(rate);
  if (rate != null) {
    return direct;
  }
  if (isBeforeOrDuringSubscription(stock)) {
    return 8;
  }
  return 0;
}

int scoreMarketCapForStock(IpoCompetitionStock stock) {
  final marketCap = stock.fundamentals.marketCapKrw;
  if (marketCap == null || marketCap <= 0) {
    return 0;
  }
  if (marketCap <= 300000000000) {
    return 8;
  }
  if (marketCap <= 600000000000) {
    return 5;
  }
  if (marketCap <= 1000000000000) {
    return 3;
  }
  return 1;
}

int scoreLeadManagerCoverageForStock(IpoCompetitionStock stock) {
  final count = stock.leadManagers.length;
  if (count >= 2) {
    return 2;
  }
  if (count == 1) {
    return 0;
  }
  return 0;
}

int scorePublicAllocationForStock(IpoCompetitionStock stock) {
  final shares = stock.fundamentals.publicAllocationShares;
  if (shares == null || shares <= 0) {
    return 0;
  }
  if (shares <= 700000) {
    return 8;
  }
  if (shares <= 1200000) {
    return 6;
  }
  if (shares <= 2000000) {
    return 4;
  }
  if (shares <= 3000000) {
    return 2;
  }
  return 1;
}

int scorePutbackRightForStock(IpoCompetitionStock stock) {
  return stock.fundamentals.hasPutbackRight ? 4 : 0;
}

bool? inferPutbackRightFromSummary(String? rawSummary) {
  final summary = rawSummary?.trim();
  if (summary == null || summary.isEmpty) {
    return null;
  }
  final normalized = summary.replaceAll(' ', '');
  if (normalized.contains('환매청구권없') ||
      normalized.contains('환매청구권미부여') ||
      normalized.contains('환매청구권해당없') ||
      normalized.contains('풋백없') ||
      normalized.contains('풋백미부여')) {
    return false;
  }
  if (normalized.contains('환매청구권부여') ||
      normalized.contains('환매청구권있') ||
      normalized.contains('풋백부여') ||
      normalized.contains('풋백옵션부여')) {
    return true;
  }
  return null;
}

int scoreDemandStrengthForStock(IpoCompetitionStock stock) {
  var score = 0;
  final participants = stock.fundamentals.institutionParticipants ?? 0;
  if (participants >= 2200) {
    score += 7;
  } else if (participants >= 2000) {
    score += 6;
  } else if (participants >= 1500) {
    score += 5;
  } else if (participants >= 1000) {
    score += 3;
  } else if (participants >= 700) {
    score += 1;
  }

  final offer = stock.fundamentals.offerPrice;
  final min = stock.fundamentals.priceBandMin;
  final max = stock.fundamentals.priceBandMax;
  if (offer != null && min != null && max != null && max > min) {
    final position = (offer - min) / (max - min);
    if (stock.fundamentals.topBandConfirmation == true) {
      score += 8;
    } else if (offer >= max) {
      score += 6;
    } else if (position >= 0.9) {
      score += 4;
    }
  }
  return clampInt(score, 0, 10);
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
  if (fundamentals.topBandConfirmation == true) {
    return 10;
  }
  if (position > 1.0) {
    return 4;
  }
  if (position >= 0.85) {
    return 8;
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

int scoreCompetitionForStock(
  IpoCompetitionStock stock, {
  bool ignoreLiveCompetition = false,
}) {
  final direct = ignoreLiveCompetition
      ? 0
      : scoreCompetition(stock.latestSnapshot?.aggregate.competitionRate);
  if (direct > 0) {
    return direct;
  }
  if (!isBeforeOrDuringSubscription(stock)) {
    return 0;
  }
  final institutionRate = stock.fundamentals.institutionCompetitionRate ?? 0;
  final lockupRate = stock.fundamentals.lockupCommitmentRate ?? 0;
  if (stock.fundamentals.topBandConfirmation == true &&
      institutionRate >= 1000 &&
      lockupRate >= 0.5) {
    return 16;
  }
  if (institutionRate >= 1000 && lockupRate >= 0.5) {
    return 12;
  }
  if (institutionRate >= 700 && lockupRate >= 0.3) {
    return 8;
  }
  if (institutionRate >= 300) {
    return 4;
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
  if (isSpacStock(stock)) {
    final ceiling = spacConfidenceCeilingFor(stock);
    if (confidence > ceiling) {
      confidence = ceiling;
    }
  }
  return clampDouble(confidence, 0.05, 0.95);
}

int spacScoreCeilingFor(IpoCompetitionStock stock) {
  final source = stock.latestSnapshot?.source.toLowerCase() ?? '';
  if (source.contains('community') ||
      source.contains('article_and_public_estimate') ||
      source.contains('youtube_video_ocr_secondary') ||
      source.contains('estimated')) {
    return 82;
  }
  if (source.contains('live') || source.contains('ipostock')) {
    return 88;
  }
  return 90;
}

double spacConfidenceCeilingFor(IpoCompetitionStock stock) {
  final source = stock.latestSnapshot?.source.toLowerCase() ?? '';
  if (source.contains('community') ||
      source.contains('article_and_public_estimate') ||
      source.contains('youtube_video_ocr_secondary') ||
      source.contains('estimated')) {
    return 0.78;
  }
  if (source.contains('live') || source.contains('ipostock')) {
    return 0.86;
  }
  return 0.9;
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
  return generalExpectedReturnProfileFor(
    stock: stock,
    score: score,
    competitionRate: competitionRate,
    confidence: confidence,
    calibration: calibration.general,
  );
}

IpoExpectedReturnProfile generalExpectedReturnProfileFor({
  required IpoCompetitionStock stock,
  required int score,
  required double? competitionRate,
  required double confidence,
  required IpoGeneralCalibration? calibration,
}) {
  final fundamentals = stock.fundamentals;
  final institutionRate = fundamentals.institutionCompetitionRate;
  final lockupRate = fundamentals.lockupCommitmentRate;
  final participants = fundamentals.institutionParticipants ?? 0;
  final topBandConfirmed = fundamentals.topBandConfirmation == true;

  final priorBase = generalExpectedGainRateFor(
    stock: stock,
    score: score,
    competitionRate: null,
    confidence: confidence,
    calibration: calibration,
  );
  final generalAdjustment = competitionRate == null
      ? clampDouble(
          calibration?.dampedAdjustment ?? 0,
          -(calibration?.maxAdjustment ?? 0.18),
          calibration?.maxAdjustment ?? 0.18,
        )
      : 0.0;
  final competitionBase = generalCompetitionBaselineFor(competitionRate);
  final institutionBoost = institutionRate == null
      ? 0.0
      : clampDouble((institutionRate - 500) / 8000, 0, 0.09);
  final lockupBoost = lockupRate == null
      ? 0.0
      : clampDouble((lockupRate - 0.15) / 4.0, 0, 0.1);
  final participantBoost = participants >= 2200
      ? 0.05
      : participants >= 1800
      ? 0.04
      : participants >= 1200
      ? 0.025
      : participants >= 800
      ? 0.01
      : 0.0;
  final topBandBoost = topBandConfirmed ? 0.06 : 0.0;
  final scoreBoost = clampDouble((score - 72) / 180, -0.04, 0.08);
  final demandOverlay =
      institutionBoost +
      lockupBoost +
      participantBoost +
      topBandBoost +
      scoreBoost;
  final competitionDrivenBase = competitionRate == null
      ? priorBase
      : clampDouble(
          max(competitionBase, priorBase * 0.82) + demandOverlay,
          -0.1,
          1.3,
        );
  final expected = clampDouble(
    competitionDrivenBase + generalAdjustment,
    -0.1,
    1.3,
  );
  final bear = clampDouble(
    expected - (competitionRate == null ? 0.2 : 0.18),
    -0.15,
    1.0,
  );
  final bull = clampDouble(
    expected +
        (competitionRate == null ? 0.28 : 0.22) +
        (topBandConfirmed ? 0.06 : 0.0) +
        (lockupRate != null && lockupRate >= 0.6 ? 0.08 : 0.0),
    0,
    1.8,
  );
  return IpoExpectedReturnProfile(
    expectedListingGainRate: expected,
    bearCaseListingGainRate: bear,
    baseCaseListingGainRate: expected,
    bullCaseListingGainRate: bull,
    assumptions: {
      'method': 'ipo_score_v6_general_competition_calibrated',
      'generalCompetitionBaseline': competitionBase,
      'generalPriorBase': priorBase,
      'institutionBoost': institutionBoost,
      'lockupBoost': lockupBoost,
      'participantBoost': participantBoost,
      'topBandBoost': topBandBoost,
      'scoreBoost': scoreBoost,
      'competitionPresent': competitionRate != null,
      'generalCalibrationApplied': generalAdjustment,
      'generalCalibrationSampleCount': calibration?.sampleCount ?? 0,
      'generalCompetitionPending': competitionRate == null,
    },
  );
}

double generalExpectedGainRateFor({
  required IpoCompetitionStock stock,
  required int score,
  required double? competitionRate,
  required double confidence,
  required IpoGeneralCalibration? calibration,
}) {
  final institutionRate = stock.fundamentals.institutionCompetitionRate;
  final lockupRate = stock.fundamentals.lockupCommitmentRate;
  final participants = stock.fundamentals.institutionParticipants ?? 0;
  final topBandConfirmed = stock.fundamentals.topBandConfirmation == true;
  final scoreComponent = (score - 58) / 120;
  final competitionComponent = competitionRate == null
      ? 0.0
      : clampDouble((competitionRate - 220) / 1800, -0.08, 0.24);
  final institutionComponent = institutionRate == null
      ? 0.0
      : clampDouble((institutionRate - 450) / 7000, 0, 0.08);
  final lockupComponent = lockupRate == null
      ? 0.0
      : clampDouble((lockupRate - 0.12) / 4.2, 0, 0.09);
  final participantComponent = participants >= 2200
      ? 0.045
      : participants >= 1800
      ? 0.03
      : participants >= 1200
      ? 0.018
      : 0.0;
  final topBandComponent = topBandConfirmed ? 0.06 : 0.0;
  final calibrationAdjustment = competitionRate == null
      ? clampDouble(
          calibration?.dampedAdjustment ?? 0,
          -(calibration?.maxAdjustment ?? 0.18),
          calibration?.maxAdjustment ?? 0.18,
        )
      : 0.0;
  final raw =
      0.14 +
      scoreComponent +
      competitionComponent +
      institutionComponent +
      lockupComponent +
      participantComponent +
      topBandComponent +
      calibrationAdjustment;
  return clampDouble(raw * (0.68 + confidence * 0.32), -0.2, 1.2);
}

double generalCompetitionBaselineFor(double? competitionRate) {
  if (competitionRate == null) {
    return 0.0;
  }
  if (competitionRate >= 1500) {
    return 0.95;
  }
  if (competitionRate >= 800) {
    return 0.72;
  }
  if (competitionRate >= 400) {
    return 0.52;
  }
  if (competitionRate >= 200) {
    return 0.38;
  }
  if (competitionRate >= 100) {
    return 0.24;
  }
  if (competitionRate >= 50) {
    return 0.1;
  }
  return 0.03;
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
  final retailTailBoost = competitionRate == null
      ? 0.0
      : clampDouble((competitionRate - 1500) / 8000, 0, 0.04);
  final proportionalTailBoost = proportionalRate == null
      ? 0.0
      : clampDouble((proportionalRate - 3000) / 12000, 0, 0.04);
  final institutionTailBoost = institutionRate == null
      ? 0.0
      : clampDouble((institutionRate - 1200) / 6000, 0, 0.03);
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
      retailTailBoost +
      proportionalTailBoost +
      institutionTailBoost +
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
      'retailTailBoost': retailTailBoost,
      'proportionalTailBoost': proportionalTailBoost,
      'institutionTailBoost': institutionTailBoost,
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
    final profit = (shares * price * expectedGainRate).round();
    return MapEntry(key, profit);
  });
}

String gradeFor(int score) {
  if (score >= 98) {
    return 'S';
  }
  if (score >= 93) {
    return 'A+';
  }
  if (score >= 86) {
    return 'A';
  }
  if (score >= 79) {
    return 'A-';
  }
  if (score >= 72) {
    return 'B+';
  }
  if (score >= 65) {
    return 'B';
  }
  if (score >= 58) {
    return 'B-';
  }
  if (score >= 51) {
    return 'C+';
  }
  if (score >= 44) {
    return 'C';
  }
  return 'C-';
}

int maxScoreForFactors(Map<String, int> factors) {
  const maxByFactor = <String, int>{
    'competition': 16,
    'institutionDemand': 24,
    'demandStrength': 10,
    'spacMomentum': 16,
    'spacVolatility': 4,
    'lockupCommitment': 18,
    'floatRate': 12,
    'pricing': 10,
    'marketCap': 10,
    'publicAllocation': 8,
    'leadManagerCoverage': 2,
    'putbackRight': 4,
    'market': 6,
    'leadManagers': 6,
    'recency': 4,
    'dataCompleteness': 8,
  };
  return factors.keys.fold<int>(0, (sum, key) => sum + (maxByFactor[key] ?? 0));
}

int normalizedFactorScore(Map<String, int> factors) {
  final maxPossible = maxScoreForFactors(factors);
  if (maxPossible <= 0) {
    return 0;
  }
  final rawTotal = factors.values.fold<int>(0, (sum, value) => sum + value);
  return clampInt(((rawTotal / maxPossible) * 100).round(), 0, 100);
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
  final brokerPriorities = <String, int>{};
  for (final snapshot in stock.snapshots) {
    final sourcePriority = snapshotSourcePriority(snapshot.source);
    for (final broker in snapshot.brokers) {
      if (broker.name == '통합') {
        continue;
      }
      final previousPriority = brokerPriorities[broker.name] ?? -1;
      if (sourcePriority >= previousPriority) {
        brokerMetrics[broker.name] = broker;
        brokerPriorities[broker.name] = sourcePriority;
      }
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
        final hasDirectExpectedEqual =
            expectedEqual != null && expectedEqual > 0;
        final quality =
            (hasPositiveApplicationCount || hasDirectExpectedEqual) &&
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
  final generalRows = stocks
      .where((stock) => !isSpacStock(stock))
      .map((stock) {
        final referenceReturn = referenceReturnRateForBacktest(stock);
        if (referenceReturn == null) {
          return null;
        }
        final rawAnalysis = analyzeStock(
          stock,
          calibration: const IpoAnalysisCalibration(),
          ignoreLiveCompetitionForScoring: true,
          ignoreLiveCompetitionForReturn: true,
        );
        final error = roundDouble(
          referenceReturn - rawAnalysis.expectedReturn.expectedListingGainRate,
          4,
        );
        return {
          'id': safeId(stock.id),
          'referenceError': error,
          'recencyWeight': recencyWeightForCalibration(stock),
        };
      })
      .whereType<Map<String, Object?>>()
      .toList();
  if (spacRows.isEmpty && generalRows.isEmpty) {
    return const IpoAnalysisCalibration();
  }
  final spacErrors = spacRows
      .map((row) => row['referenceError'])
      .whereType<double>()
      .toList();
  final bucketed = <String, List<double>>{};
  for (final row in spacRows) {
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
  final generalErrors = generalRows
      .map((row) => row['referenceError'])
      .whereType<double>()
      .toList();
  final generalWeightedErrors = generalRows
      .map((row) {
        final error = row['referenceError'];
        final weight = row['recencyWeight'];
        if (error is! double || weight is! double) {
          return null;
        }
        return (error, weight);
      })
      .whereType<(double, double)>()
      .toList();
  return IpoAnalysisCalibration(
    spac: spacRows.isEmpty
        ? null
        : IpoSpacCalibration(
            sampleCount: spacRows.length,
            averageReferenceError: average(spacErrors),
            medianReferenceError: median(spacErrors),
            dampedAdjustment: dampedCalibrationAdjustment(spacErrors),
            maxAdjustment: 0.22,
            byCompetitionBucket: byCompetitionBucket,
          ),
    general: generalRows.isEmpty
        ? null
        : IpoGeneralCalibration(
            sampleCount: generalRows.length,
            averageReferenceError: average(generalErrors),
            medianReferenceError: median(generalErrors),
            recencyWeightedReferenceError: weightedAverage(
              generalWeightedErrors
                  .map<(double?, double)>((pair) => (pair.$1, pair.$2))
                  .toList(),
            ),
            dampedAdjustment: dampedCalibrationAdjustmentWeighted(
              generalWeightedErrors,
            ),
            maxAdjustment: 0.18,
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
  final generalRows = stocks
      .where((stock) => !isSpacStock(stock))
      .map((stock) {
        final referenceReturn = referenceReturnRateForBacktest(stock);
        if (referenceReturn == null) {
          return null;
        }
        final rawAnalysis = analyzeStock(
          stock,
          calibration: const IpoAnalysisCalibration(),
          ignoreLiveCompetitionForScoring: true,
          ignoreLiveCompetitionForReturn: true,
        );
        return {
          'id': safeId(stock.id),
          'company': stock.company,
          'institutionCompetitionRate':
              stock.fundamentals.institutionCompetitionRate,
          'lockupCommitmentRate': stock.fundamentals.lockupCommitmentRate,
          'recencyWeight': recencyWeightForCalibration(stock),
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
  generalRows.sort((a, b) => '${b['id']}'.compareTo('${a['id']}'));
  return {
    'schemaVersion': schemaVersion,
    'generatedAt': generatedAt.toIso8601String(),
    'methodVersion': 'ipo-score-v4',
    'calibration': calibration.toJson(),
    'spacHistoricalRows': spacRows,
    'generalHistoricalRows': generalRows,
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

double dampedCalibrationAdjustmentWeighted(List<(double, double)> errors) {
  final weighted = weightedAverage(
    errors.map<(double?, double)>((pair) => (pair.$1, pair.$2)).toList(),
  );
  if (weighted == null) {
    return 0;
  }
  final sampleWeight = clampDouble(errors.length / 20, 0.12, 0.55);
  return roundDouble(weighted * sampleWeight, 4);
}

double recencyWeightForCalibration(IpoCompetitionStock stock) {
  final anchor =
      parseDate(stock.outcome?.listingDate) ??
      parseDate(stock.subscriptionEnd) ??
      parseDate(stock.subscriptionStart);
  if (anchor == null) {
    return 0.25;
  }
  final now = DateTime.now();
  final ageDays = max(0, now.difference(anchor).inDays);
  final ageMonths = ageDays / 30.4;
  final weight = pow(0.5, ageMonths / 12).toDouble();
  return roundDouble(clampDouble(weight, 0.25, 1.0), 4);
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
    request.headers.set('X-Requested-With', 'XMLHttpRequest');
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
  final demandForecastStart = normalizeDate(
    firstNonEmptyString(row, [
      'dmdfcast_bgng_dt',
      'demandForecastStart',
      'demand_forecast_start',
      'demandForecastDate',
      'demand_forecast_date',
    ]),
  );
  final demandForecastEnd =
      normalizeDate(
        firstNonEmptyString(row, [
          'dmdfcast_end_dt',
          'demandForecastEnd',
          'demand_forecast_end',
        ]),
      ) ??
      demandForecastStart;
  final refundDate = normalizeDate(
    firstNonEmptyString(row, ['pay_dt', 'rfnd_dt', 'refundDate']),
  );
  final listingDate = normalizeDate(
    firstNonEmptyString(row, ['list_dt', 'lstg_dt', 'listingDate']),
  );
  final lockupReleaseDate = normalizeDate(
    firstNonEmptyString(row, [
      'lckup_rlse_dt',
      'lockupReleaseDate',
      'lockup_release_date',
      'protect_end_dt',
    ]),
  );
  final generalSharesDate = normalizeDate(
    firstNonEmptyString(row, [
      'gnrl_sb_dt',
      'rights_offer_dt',
      'general_shares_date',
      'generalSharesDate',
    ]),
  );
  final cbBwDate = normalizeDate(
    firstNonEmptyString(row, ['cb_bw_dt', 'cb_dt', 'bw_dt', 'cbBwDate']),
  );
  return IpoCompetitionStock(
    id: safeId('${company}_${subscriptionStart ?? ''}'),
    company: company,
    market: '',
    industry:
        firstNonEmptyString(row, [
          'induty',
          'industry',
          'sector',
          'induty_nm',
        ]) ??
        '',
    subscriptionStart: subscriptionStart,
    subscriptionEnd: subscriptionEnd,
    demandForecastStart: demandForecastStart,
    demandForecastEnd: demandForecastEnd,
    refundDate: refundDate,
    listingDate: listingDate,
    lockupReleaseDate: lockupReleaseDate,
    generalSharesDate: generalSharesDate,
    cbBwDate: cbBwDate,
    securityType: firstNonEmptyString(row, [
      'securityType',
      'security_type',
      'offerType',
      'kind',
    ]),
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
      topBandConfirmation: null,
      institutionCompetitionRate: null,
      institutionParticipants: null,
      lockupCommitmentRate: null,
      floatRate: null,
      marketCapKrw: null,
      publicAllocationShares: null,
      hasPutbackRight: false,
      putbackSummary: null,
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
  final demandForecastStart = normalizeDate(
    firstNonEmptyString(row, [
      'demandForecastStart',
      'demand_forecast_start',
      'demandForecastDate',
      'demand_forecast_date',
    ]),
  );
  final demandForecastEnd =
      normalizeDate(
        firstNonEmptyString(row, ['demandForecastEnd', 'demand_forecast_end']),
      ) ??
      demandForecastStart;
  final refundDate = normalizeDate(
    firstNonEmptyString(row, ['refundDate', 'refund_date', 'paymentDate']),
  );
  final listingDate = normalizeDate(
    firstNonEmptyString(row, ['ipoDate', 'listingDate', 'listing_date']),
  );
  final lockupReleaseDate = normalizeDate(
    firstNonEmptyString(row, ['lockupReleaseDate', 'lockup_release_date']),
  );
  final generalSharesDate = normalizeDate(
    firstNonEmptyString(row, [
      'generalSharesDate',
      'general_shares_date',
      'rightsOfferDate',
      'rights_offer_date',
    ]),
  );
  final cbBwDate = normalizeDate(
    firstNonEmptyString(row, ['cbBwDate', 'cb_bw_date', 'cb_bw_dt']),
  );
  return IpoCompetitionStock(
    id: safeId('${company}_${subscriptionStart ?? ''}'),
    company: company,
    market: firstNonEmptyString(row, ['market', 'exchange']) ?? '',
    industry: firstNonEmptyString(row, ['industry', 'sector']) ?? '',
    subscriptionStart: subscriptionStart,
    subscriptionEnd: subscriptionEnd,
    demandForecastStart: demandForecastStart,
    demandForecastEnd: demandForecastEnd,
    refundDate: refundDate,
    listingDate: listingDate,
    lockupReleaseDate: lockupReleaseDate,
    generalSharesDate: generalSharesDate,
    cbBwDate: cbBwDate,
    securityType: firstNonEmptyString(row, [
      'securityType',
      'security_type',
      'offerType',
      'kind',
      'type',
    ]),
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
      topBandConfirmation: null,
      institutionCompetitionRate: null,
      institutionParticipants: null,
      lockupCommitmentRate: null,
      floatRate: null,
      marketCapKrw: null,
      publicAllocationShares: null,
      hasPutbackRight: false,
      putbackSummary: null,
    ),
    outcome: null,
    snapshots: const [],
  );
}

IpoCompetitionStock? stockFromFinutsRows(List<Map<String, Object?>> rows) {
  if (rows.isEmpty) {
    return null;
  }

  Map<String, Object?>? rowForCode(String code) {
    for (final row in rows) {
      final scheduleCode = (row['SCHDL_SE_CD'] ?? '')
          .toString()
          .trim()
          .toUpperCase();
      if (scheduleCode == code) {
        return row;
      }
    }
    return null;
  }

  final primary =
      rowForCode('S') ?? rowForCode('L') ?? rowForCode('D') ?? rows.first;
  final company = (primary['ENT_NM'] ?? '').toString().trim();
  final ipoSn = (primary['IPO_SN'] ?? '').toString().trim();
  final finutsType = (primary['SE_CD'] ?? '').toString().trim().toUpperCase();
  if (company.isEmpty || ipoSn.isEmpty) {
    return null;
  }
  if (finutsType != 'IPO' && finutsType != 'SPAC' && finutsType != 'FORF') {
    return null;
  }

  final rowS = rowForCode('S');
  final rowL = rowForCode('L');
  final rowD = rowForCode('D');

  final subscriptionStart = normalizeDate((rowS?['BGNG_YMD'] ?? '').toString());
  final subscriptionEnd =
      normalizeDate((rowS?['END_YMD'] ?? '').toString()) ?? subscriptionStart;
  final listingDate =
      normalizeDate((rowL?['BGNG_YMD'] ?? '').toString()) ??
      normalizeDate((rowL?['IPO_DATE'] ?? '').toString()) ??
      normalizeDate((primary['IPO_DATE'] ?? '').toString());
  final generalSharesDate = finutsType == 'FORF'
      ? (listingDate ?? subscriptionEnd ?? subscriptionStart)
      : null;
  final demandForecastStart = normalizeDate(
    (rowD?['BGNG_YMD'] ?? '').toString(),
  );
  final demandForecastEnd =
      normalizeDate((rowD?['END_YMD'] ?? '').toString()) ?? demandForecastStart;

  if (subscriptionStart == null &&
      subscriptionEnd == null &&
      listingDate == null &&
      demandForecastStart == null) {
    return null;
  }

  final confirmedPrice = readDouble(primary['PSS_PRC']);
  final bandMin = readDouble(primary['BAND_BGNG_AMT']);
  final bandMax = readDouble(primary['BAND_END_AMT']);
  final institutionCompetitionRate = readDouble(primary['INST_CMPET_RT']);
  final lockupCommitmentRate = readRatio(primary['DUTY_HOLD_DFPR_RT']);
  final publicAllocationShares = inferFinutsPublicAllocationShares(
    finutsType: finutsType,
    row: primary,
  );
  final offerPrice = confirmedPrice != null && confirmedPrice > 0
      ? confirmedPrice.round()
      : null;
  final priceBandMin = bandMin != null && bandMin > 0
      ? bandMin.round()
      : offerPrice;
  final priceBandMax = bandMax != null && bandMax > 0
      ? bandMax.round()
      : offerPrice;
  final topBandConfirmation =
      offerPrice != null &&
          priceBandMin != null &&
          priceBandMax != null &&
          priceBandMax > priceBandMin
      ? offerPrice >= priceBandMax
      : null;

  return IpoCompetitionStock(
    id: safeId('finuts_${ipoSn}_${subscriptionStart ?? listingDate ?? ''}'),
    company: company,
    market: 'KOSDAQ',
    industry: '',
    subscriptionStart: subscriptionStart,
    subscriptionEnd: subscriptionEnd,
    demandForecastStart: demandForecastStart,
    demandForecastEnd: demandForecastEnd,
    listingDate: finutsType == 'FORF' ? null : listingDate,
    generalSharesDate: generalSharesDate,
    securityType: finutsType == 'FORF' ? 'GENERAL_SHARES' : finutsType,
    leadManagers: readLeadManagers(
      firstNonEmptyString(primary, ['INDCT_JUGANSA_NM']),
    ),
    sourceIdentifiers: IpoStockIdentifiers(
      subscriptionKey: '',
      normalizedCompany: '',
      corpCode: null,
      stockCode: null,
      kindCode: ipoSn,
      isin: null,
    ),
    fundamentals: IpoFundamentals(
      offerPrice: offerPrice,
      priceBandMin: priceBandMin,
      priceBandMax: priceBandMax,
      topBandConfirmation: topBandConfirmation,
      institutionCompetitionRate: institutionCompetitionRate,
      institutionParticipants: null,
      lockupCommitmentRate: lockupCommitmentRate,
      floatRate: null,
      marketCapKrw: null,
      publicAllocationShares: publicAllocationShares,
      hasPutbackRight: false,
      putbackSummary: null,
    ),
    outcome: null,
    snapshots: const [],
  );
}

int? inferFinutsPublicAllocationShares({
  required String finutsType,
  required Map<String, Object?> row,
}) {
  final raw = readDouble(row['PSS_GRAMT']);
  if (raw == null || raw <= 0) {
    return null;
  }
  if (finutsType == 'SPAC') {
    return (raw * 1000).round();
  }
  return null;
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
  if (digits.isEmpty || digits == '99999999') {
    return null;
  }
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
