import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:flutter/services.dart'; // <-- new
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
  final Map<String, int> _pendingPings = {}; // id -> send timestamp ms

  // Discovered local addresses (each entry has 'ip' and 'label')
  final List<Map<String, String>> _localAddrs = [];

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

    // NOTE: do not overwrite user-entered client host; server IPs are only given (display/copy).
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
    // Expecting messages of form: PING:<id>:<sentMs>
    // We echo same payload from server, so client receives what it sent.
    try {
      final text = message.toString();
      _appendClientLog('Received: $text');
      if (text.startsWith('PING:')) {
        final parts = text.split(':');
        if (parts.length >= 3) {
          final id = parts[1];
          final sentMs = int.tryParse(parts[2]);
          if (sentMs != null && _pendingPings.containsKey(id)) {
            final now = DateTime.now().millisecondsSinceEpoch;
            final rtt = now - _pendingPings.remove(id)!;
            final latency = rtt / 2;
            _appendClientLog(
              'Ping $id RTT=${rtt}ms latency=${latency.toStringAsFixed(1)}ms',
            );
          }
        }
      }
    } catch (e) {
      _appendClientLog('Error handling message: $e');
    }
  }

  void _sendPing() {
    if (!_clientConnected || _webSocket == null) {
      _appendClientLog('Not connected');
      return;
    }
    final id = DateTime.now().microsecondsSinceEpoch.toString();
    final nowMs = DateTime.now().millisecondsSinceEpoch;
    final payload = 'PING:$id:$nowMs';
    _pendingPings[id] = nowMs;
    _webSocket!.add(payload);
    _appendClientLog('Sent ping $id');
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
                  onPressed: _sendPing,
                  child: const Text('Send Ping'),
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
    final logs = _mode == Mode.server ? _serverLogs : _clientLogs;
    final title = _mode == Mode.server ? 'Server Logs' : 'Client Logs';
    return Card(
      child: Column(
        children: [
          ListTile(
            title: Text(title),
            subtitle: Text(
              _mode == Mode.server
                  ? (_serverRunning
                        ? 'Running on port $_serverPort'
                        : 'Stopped')
                  : (_clientConnected
                        ? 'Connected to $_clientHost:$_clientPort'
                        : 'Disconnected'),
            ),
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
}
