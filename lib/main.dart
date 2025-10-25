import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:flutter/services.dart';
import 'package:flutter/material.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Pinger (WebSocket) Demo',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.deepPurple),
      ),
      home: const MyHomePage(title: 'Pinger — WebSocket'),
    );
  }
}

enum Mode { server, client }

class MyHomePage extends StatefulWidget {
  const MyHomePage({super.key, required this.title});
  final String title;

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  Mode _mode = Mode.client;

  // Server state
  HttpServer? _httpServer;
  final List<WebSocket> _clients = [];
  bool _serverRunning = false;
  int _serverPort = 8080;
  final List<String> _serverLogs = [];

  // Client state
  WebSocket? _webSocket;
  bool _clientConnected = false;
  String _clientHost = '127.0.0.1';
  int _clientPort = 8080;
  final List<String> _clientLogs = [];

  // pending timestamps keyed by the message payload.
  // each pending entry is a map { 'sentMs': int, 'server': String }
  final Map<String, List<Map<String, dynamic>>> _pendingPings = {};

  // Per-server collected RTTs (ms)
  final Map<String, List<int>> _rttsByServer = {};
  final Map<String, double> _avgByServer = {};
  final Map<String, int> _lastRttByServer = {};

  // Per-server sample records (one card per ping): { 'rtt': int, 'ts': int }
  final Map<String, List<Map<String, dynamic>>> _samplesByServer = {};

  // Discovered local addresses (each entry has 'ip' and 'label')
  final List<Map<String, String>> _localAddrs = [];

  // ping flow control
  bool _pingInProgress = false;
  int _pingCount = 10;
  int _pingIntervalMs = 1000;

  // UI helpers
  final ScrollController _logScroll = ScrollController();

  @override
  void initState() {
    super.initState();
    _refreshLocalIps(); // populate addresses on start
  }

  @override
  void dispose() {
    _stopServer();
    _disconnectClient();
    _logScroll.dispose();
    super.dispose();
  }

  // ---------------- Local IP discovery ----------------
  Future<void> _refreshLocalIps() async {
    _localAddrs.clear();
    try {
      final interfaces = await NetworkInterface.list(
        type: InternetAddressType.IPv4,
        includeLoopback: false,
      );
      for (var iface in interfaces) {
        for (var addr in iface.addresses) {
          _localAddrs.add({
            'ip': addr.address,
            'label': '${addr.address} (${iface.name})',
          });
        }
      }
    } catch (e) {
      // discovery failed, ignore
    }

    // Helpful alias for Android emulators to reach host machine:
    // 10.0.2.2 is the special host loopback for the Android AVD.
    if (!_localAddrs.any((a) => a['ip'] == '10.0.2.2')) {
      _localAddrs.add({'ip': '10.0.2.2', 'label': '10.0.2.2 (Android emulator host)'});
    }

    setState(() {});
  }

  Future<void> _copyToClipboard(String text) async {
    await Clipboard.setData(ClipboardData(text: text));
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text('Copied $text to clipboard')),
    );
  }

  // ---------------- Server ----------------
  Future<void> _startServer() async {
    if (_serverRunning) return;
    try {
      _httpServer = await HttpServer.bind(InternetAddress.anyIPv4, _serverPort);
      _appendServerLog('Server listening on ws://0.0.0.0:$_serverPort');
      _serverRunning = true;

      // refresh addresses again (server started)
      await _refreshLocalIps();

      _httpServer!.listen((HttpRequest req) async {
        if (WebSocketTransformer.isUpgradeRequest(req)) {
          try {
            WebSocket ws = await WebSocketTransformer.upgrade(req);
            _clients.add(ws);
            _appendServerLog('Client connected (${_clients.length})');
            ws.listen(
              (message) {
                // Echo message back immediately
                ws.add(message);
                _appendServerLog('Echoed: $message');
              },
              onDone: () {
                _clients.remove(ws);
                _appendServerLog('Client disconnected (${_clients.length})');
              },
              onError: (e) {
                _clients.remove(ws);
                _appendServerLog('Client error: $e');
              },
            );
          } catch (e) {
            _appendServerLog('Upgrade failed: $e');
            req.response
              ..statusCode = HttpStatus.internalServerError
              ..close();
          }
        } else {
          // Simple HTTP hint
          req.response
            ..statusCode = HttpStatus.ok
            ..headers.contentType = ContentType.html
            ..write('<html><body>WebSocket server running</body></html>');
          await req.response.close();
        }
      });
      setState(() {});
    } catch (e) {
      _appendServerLog('Failed to start server: $e');
      setState(() {});
    }
  }

  Future<void> _stopServer() async {
    if (!_serverRunning) return;
    try {
      for (var c in List<WebSocket>.from(_clients)) {
        await c.close();
      }
      _clients.clear();
      await _httpServer?.close(force: true);
      _httpServer = null;
      _serverRunning = false;
      _appendServerLog('Server stopped');
      setState(() {});
    } catch (e) {
      _appendServerLog('Error stopping server: $e');
    }
  }

  void _appendServerLog(String s) {
    _serverLogs.add('${DateTime.now().toIso8601String()} - $s');
    setState(() {});
    _scrollLogs();
  }

  // ---------------- Client ----------------
  Future<void> _connectClient() async {
    if (_clientConnected) return;

    final host = _clientHost.trim();
    if (host.isEmpty || host == '0.0.0.0') {
      _appendClientLog('Invalid host: $host');
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text(
            'Invalid host. Copy a server IP from the Server panel or use 10.0.2.2 (Android emulator) or a public IP for WAN.',
          ),
        ),
      );
      return;
    }

    final uri = 'ws://$host:$_clientPort';
    try {
      _webSocket = await WebSocket.connect(uri);
      _clientConnected = true;
      _appendClientLog('Connected to $uri');
      _webSocket!.listen(
        (message) {
          _handleClientMessage(message);
        },
        onDone: () {
          _appendClientLog('Connection closed by server');
          _clientConnected = false;
          _webSocket = null;
          setState(() {});
        },
        onError: (e) {
          _appendClientLog('Connection error: $e');
          _clientConnected = false;
          _webSocket = null;
          setState(() {});
        },
      );
      setState(() {});
    } catch (e) {
      _appendClientLog(
        'Failed to connect: $e — ensure server is running, correct IP/port, firewall/NAT/port-forwarding.',
      );
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Connect failed: $e')),
      );
    }
  }

  Future<void> _disconnectClient() async {
    if (!_clientConnected) return;
    try {
      await _webSocket?.close();
    } catch (_) {}
    _webSocket = null;
    _clientConnected = false;
    _appendClientLog('Disconnected');
    setState(() {});
  }

  void _appendClientLog(String s) {
    _clientLogs.add('${DateTime.now().toIso8601String()} - $s');
    setState(() {});
    _scrollLogs();
  }

  void _handleClientMessage(dynamic message) {
    // Expecting simple echoed messages (e.g. the client's IP string).
    try {
      final text = message.toString();
      _appendClientLog('Received: $text');

      // Use the echoed text as the key to match a pending send time.
      if (_pendingPings.containsKey(text) && _pendingPings[text]!.isNotEmpty) {
        final meta = _pendingPings[text]!.removeAt(0);
        if (_pendingPings[text]!.isEmpty) _pendingPings.remove(text);
        final sentMs = meta['sentMs'] as int;
        final server = meta['server'] as String;
        final now = DateTime.now().millisecondsSinceEpoch;
        final rtt = now - sentMs;

        // store per-server results and a sample record (one card per ping)
        _rttsByServer.putIfAbsent(server, () => []).add(rtt);
        _lastRttByServer[server] = rtt;
        _samplesByServer.putIfAbsent(server, () => []).add({'rtt': rtt, 'ts': now});

        _appendClientLog('Msg="$text" RTT=${rtt}ms');
        // update UI immediately so cards appear as pings return
        setState(() {});
      }
    } catch (e) {
      _appendClientLog('Error handling message: $e');
    }
  }

  // send a single simple message (payload is the device/client IP or configured host)
  void _sendPingOne() {
    if (!_clientConnected || _webSocket == null) {
      _appendClientLog('Not connected');
      return;
    }

    final clientIp = _localAddrs.isNotEmpty ? _localAddrs.first['ip']! : _clientHost;
    final payload = clientIp;
    final nowMs = DateTime.now().millisecondsSinceEpoch;
    // store server that we are targeting for this ping (use current target host)
    _pendingPings.putIfAbsent(payload, () => <Map<String, dynamic>>[]).add({
      'sentMs': nowMs,
      'server': _clientHost,
    });
    _webSocket!.add(payload);
    _appendClientLog('Sent msg="$payload" at ${nowMs}ms -> server=$_clientHost');
  }

  // run N pings spaced by intervalMs and compute average (results saved per server)
  Future<void> _runPingSequence({int count = 10, int intervalMs = 1000}) async {
    if (_pingInProgress) return;
    if (!_clientConnected || _webSocket == null) {
      _appendClientLog('Cannot start pings: client not connected');
      return;
    }

    _pingInProgress = true;
    setState(() {});

    // clear previous results for this server
    final server = _clientHost;
    _rttsByServer[server] = [];
    _avgByServer.remove(server);
    _lastRttByServer.remove(server);
    _samplesByServer[server] = [];

    for (int i = 0; i < count; i++) {
      _sendPingOne();
      await Future.delayed(Duration(milliseconds: intervalMs));
    }

    // allow a short grace period for last echoes to arrive
    await Future.delayed(const Duration(seconds: 2));

    // compute avg if responses arrived
    final list = _rttsByServer[server] ?? [];
    if (list.isNotEmpty) {
      final avg = list.reduce((a, b) => a + b) / list.length;
      _avgByServer[server] = avg;
      _appendClientLog('Completed $count pings to $server — avg RTT=${avg.toStringAsFixed(1)}ms');
    } else {
      _appendClientLog('No responses from $server');
    }

    _pingInProgress = false;
    setState(() {});
  }

  void _scrollLogs() {
    // Allow short delay for ListView to update
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_logScroll.hasClients) {
        _logScroll.jumpTo(_logScroll.position.maxScrollExtent);
      }
    });
  }

  // ---------------- UI ----------------
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text(widget.title),
        backgroundColor: Theme.of(context).colorScheme.inversePrimary,
      ),
      body: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          children: [
            Row(
              children: [
                const Text('Mode:'),
                const SizedBox(width: 12),
                ChoiceChip(
                  label: const Text('Client'),
                  selected: _mode == Mode.client,
                  onSelected: (v) {
                    setState(() {
                      _mode = Mode.client;
                    });
                  },
                ),
                const SizedBox(width: 8),
                ChoiceChip(
                  label: const Text('Server'),
                  selected: _mode == Mode.server,
                  onSelected: (v) {
                    setState(() {
                      _mode = Mode.server;
                    });
                  },
                ),
              ],
            ),
            const SizedBox(height: 12),
            if (_mode == Mode.server) _buildServerPanel(),
            if (_mode == Mode.client) _buildClientPanel(),
            const SizedBox(height: 12),
            Expanded(child: _buildLogs()),
          ],
        ),
      ),
    );
  }

  Widget _buildServerPanel() {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          children: [
            const Text('Port:'),
            const SizedBox(width: 8),
            SizedBox(
              width: 100,
              child: TextFormField(
                initialValue: _serverPort.toString(),
                keyboardType: TextInputType.number,
                onChanged: (v) {
                  final p = int.tryParse(v);
                  if (p != null) _serverPort = p;
                },
              ),
            ),
            const SizedBox(width: 12),
            ElevatedButton(
              onPressed: _serverRunning ? _stopServer : _startServer,
              child: Text(_serverRunning ? 'Stop Server' : 'Start Server'),
            ),
            const SizedBox(width: 12),
            Text('Clients: ${_clients.length}'),
          ],
        ),
        const SizedBox(height: 8),
        Row(
          children: [
            const Text('Local IPs:'),
            const SizedBox(width: 8),
            ElevatedButton(
              onPressed: _refreshLocalIps,
              child: const Text('Refresh IPs'),
            ),
          ],
        ),
        const SizedBox(height: 8),
        Wrap(
          spacing: 8,
          runSpacing: 6,
          children: _localAddrs.map((a) {
            final ip = a['ip']!;
            final label = a['label']!;
            return Chip(
              label: Text(label),
              onDeleted: null,
              avatar: IconButton(
                icon: const Icon(Icons.copy, size: 16),
                onPressed: () => _copyToClipboard(ip),
                tooltip: 'Copy IP',
              ),
            );
          }).toList(),
        ),
      ],
    );
  }

  Widget _buildClientPanel() {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Column(
          children: [
            Row(
              children: [
                const Text('Host:'),
                const SizedBox(width: 8),
                SizedBox(
                  width: 240,
                  child: TextFormField(
                    initialValue: _clientHost,
                    decoration: const InputDecoration(
                      border: OutlineInputBorder(),
                      isDense: true,
                      hintText: 'IP or hostname (e.g. 203.0.113.5)',
                    ),
                    onChanged: (v) {
                      _clientHost = v.trim();
                    },
                  ),
                ),
                const SizedBox(width: 8),
                const Text('Port:'),
                const SizedBox(width: 8),
                SizedBox(
                  width: 80,
                  child: TextFormField(
                    initialValue: _clientPort.toString(),
                    keyboardType: TextInputType.number,
                    onChanged: (v) {
                      final p = int.tryParse(v);
                      if (p != null) _clientPort = p;
                    },
                  ),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Row(
              children: [
                ElevatedButton(
                  onPressed:
                      _clientConnected ? _disconnectClient : _connectClient,
                  child:
                      Text(_clientConnected ? 'Disconnect' : 'Connect'),
                ),
                const SizedBox(width: 12),
                ElevatedButton(
                  onPressed: _pingInProgress || !_clientConnected
                      ? null
                      : () => _runPingSequence(count: _pingCount, intervalMs: _pingIntervalMs),
                  child: Text(_pingInProgress ? 'Pinging...' : 'Start 10 pings'),
                ),
                const SizedBox(width: 12),
                ElevatedButton(
                  onPressed: _refreshLocalIps,
                  child: const Text('Refresh IPs'),
                ),
              ],
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildLogs() {
    if (_mode == Mode.server) {
      final logs = _serverLogs;
      return Card(
        child: Column(
          children: [
            ListTile(
              title: const Text('Server Logs'),
              subtitle: Text(_serverRunning ? 'Running on port $_serverPort' : 'Stopped'),
            ),
            const Divider(height: 1),
            Expanded(
              child: ListView.builder(
                controller: _logScroll,
                itemCount: logs.length,
                itemBuilder: (context, i) {
                  final text = logs[i];
                  return Padding(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 12,
                      vertical: 6,
                    ),
                    child: InkWell(
                      onLongPress: () => _copyToClipboard(text),
                      child: Text(text, style: const TextStyle(fontSize: 12)),
                    ),
                  );
                },
              ),
            ),
          ],
        ),
      );
    }

    // Client: show per-server cards with last RTT, avg and number of samples
    final serverKeys = _rttsByServer.keys.toList();
    // ensure we always show current target server even if no samples yet
    if (!serverKeys.contains(_clientHost)) {
      serverKeys.insert(0, _clientHost);
      _rttsByServer.putIfAbsent(_clientHost, () => []);
      _samplesByServer.putIfAbsent(_clientHost, () => []);
    }

    // Build a vertical list with a header card for each server and a card per sample.
    final List<Widget> widgets = [];
    for (final server in serverKeys) {
      final samples = _samplesByServer[server] ?? [];
      widgets.add(Card(
        margin: const EdgeInsets.symmetric(vertical: 6, horizontal: 8),
        child: ListTile(
          title: Text('Server: $server'),
          subtitle: Text(
            _pingInProgress ? 'Pinging... Samples: ${samples.length}' : (samples.isNotEmpty ? 'Samples: ${samples.length}' : 'No samples yet. Press "Start 10 pings".'),
          ),
          trailing: IconButton(
            icon: const Icon(Icons.copy),
            onPressed: () => _copyToClipboard(server),
            tooltip: 'Copy server IP',
          ),
        ),
      ));

      // add each sample as a card (appear as they come in)
      for (final s in samples) {
        final rtt = s['rtt'] as int;
        final ts = s['ts'] as int;
        widgets.add(Card(
          margin: const EdgeInsets.symmetric(vertical: 4, horizontal: 16),
          child: ListTile(
            dense: true,
            title: Text('RTT: ${rtt} ms    Latency ≈ ${(rtt / 2).toStringAsFixed(1)} ms'),
            subtitle: Text('${DateTime.fromMillisecondsSinceEpoch(ts)}'),
            onLongPress: () => _copyToClipboard('Server:$server RTT:${rtt}ms at ${DateTime.fromMillisecondsSinceEpoch(ts)}'),
          ),
        ));
      }

      // show average only after sequence finishes (avg is set in _runPingSequence)
      if (_avgByServer.containsKey(server)) {
        final avg = _avgByServer[server]!;
        widgets.add(Card(
          color: Colors.grey.shade100,
          margin: const EdgeInsets.symmetric(vertical: 6, horizontal: 12),
          child: ListTile(
            leading: const Icon(Icons.functions),
            title: Text('Average RTT for $server: ${avg.toStringAsFixed(1)} ms'),
            subtitle: Text('Avg latency ≈ ${(avg / 2).toStringAsFixed(1)} ms'),
          ),
        ));
      }
    }

    return ListView(children: widgets);
  }
}
