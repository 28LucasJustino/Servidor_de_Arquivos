using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using UdpFileTransfer.Core;
using UdpFileTransfer.Sessions;

namespace UdpFileTransfer;

// Servidor UDP: abre um socket, recebe datagramas e gerencia sessões de transferência.
// Cada cliente recebe uma FileTransferSession própria, identificada pelo seu IP:Porta.
public sealed class UdpServer : IAsyncDisposable
{
    private readonly int    _port;
    private readonly string _filesDirectory;
    private readonly bool   _simulateLoss;   // Se true, descarta ~10% dos pacotes para testar retransmissão

    public string FilesDirectory => _filesDirectory;

    private UdpClient? _socket;

    // Sessões ativas: chave = "IP:Porta" do cliente
    private readonly ConcurrentDictionary<string, FileTransferSession> _sessions = new();
    private readonly Lock _sessionLock = new();

    // Disparado a cada evento de progresso; Program.cs assina para encaminhar ao ProgressHub
    public event Action<TransferProgress>? OnProgress;

    private readonly CancellationTokenSource _cts = new();

    public UdpServer(int port = 5000, string? filesDirectory = null, bool simulateLoss = false)
    {
        _port           = port;
        _filesDirectory = filesDirectory ?? Path.Combine(AppContext.BaseDirectory, "ServerFiles");
        _simulateLoss   = simulateLoss;

        Directory.CreateDirectory(_filesDirectory);
        Directory.CreateDirectory(Path.Combine(_filesDirectory, "uploads"));
    }

    // Abre o socket UDP e entra no loop de recebimento (bloqueante até cancelamento)
    public async Task StartAsync(CancellationToken externalCt = default)
    {
        _socket = new UdpClient(_port);
        Log($"[SERVER] UDP socket bound on :{_port}");
        Log($"[SERVER] Serving files from: {_filesDirectory}");
        Log($"[SERVER] Loss simulation: {(_simulateLoss ? "ON (10%)" : "OFF")}");

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, externalCt);
        await ReceiveLoopAsync(linked.Token);
    }

    // Cancela o token interno, encerra todas as sessões e fecha o socket
    public async Task StopAsync()
    {
        Log("[SERVER] Shutting down...");
        await _cts.CancelAsync();
        foreach (var (_, session) in _sessions)
            await session.DisposeAsync();
        _sessions.Clear();
        _socket?.Dispose();
        Log("[SERVER] Stopped.");
    }

    // Aguarda datagramas em loop. Cada pacote recebido é processado em uma Task separada
    // para não bloquear o recebimento enquanto uma sessão longa está em andamento.
    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        Log("[SERVER] Receive loop started.");
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await _socket!.ReceiveAsync(ct);
                var ep     = result.RemoteEndPoint;
                _ = Task.Run(() => DispatchPacketAsync(result.Buffer, ep, ct), ct);
            }
            catch (OperationCanceledException) { break; }
            catch (SocketException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { Log($"[SERVER] Receive error: {ex.Message}"); }
        }
        Log("[SERVER] Receive loop ended.");
    }

    // Desserializa o datagrama e roteia para o handler correto pelo campo Type
    private async Task DispatchPacketAsync(byte[] raw, IPEndPoint ep, CancellationToken ct)
    {
        DatagramPacket packet;
        try
        {
            packet = DatagramPacket.Deserialize(raw);
            Log($"[RX] {ep} -> {packet}");
        }
        catch (InvalidDataException ex)
        {
            Log($"[SERVER] Malformed packet from {ep}: {ex.Message}");
            return;
        }

        switch (packet.Type)
        {
            case MessageType.RequestList:
                await HandleListRequestAsync(ep);
                break;
            case MessageType.DownloadInit:
                await HandleDownloadInitAsync(ep, packet, ct);
                break;
            case MessageType.UploadInit:
                HandleUploadInit(ep, packet);
                break;
            case MessageType.FileChunk:
                await HandleFileChunkAsync(ep, packet);
                break;
            case MessageType.Ack:
                HandleAck(ep, packet);
                break;
            case MessageType.TransferDone:
                HandleTransferDone(ep);
                break;
            default:
                Log($"[SERVER] Unknown message type {packet.Type} from {ep}");
                break;
        }
    }

    // Responde com a lista de arquivos disponíveis serializada como JSON
    private async Task HandleListRequestAsync(IPEndPoint ep)
    {
        var files = Directory.GetFiles(_filesDirectory, "*", SearchOption.TopDirectoryOnly);
        var list  = Array.ConvertAll(files, f => new
        {
            name = Path.GetFileName(f),
            size = new FileInfo(f).Length,
        });

        var payload  = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(list));
        var response = new DatagramPacket { Type = MessageType.FileList, Payload = payload };

        await SendToAsync(response.Serialize(), ep);
        Log($"[SERVER] Sent file list to {ep} ({list.Length} files)");
    }

    // Valida o arquivo solicitado, cria a sessão e inicia o envio de chunks em background
    private async Task HandleDownloadInitAsync(IPEndPoint ep, DatagramPacket packet, CancellationToken ct)
    {
        var fileName = packet.GetPayloadAsString().Trim();
        var filePath = Path.Combine(_filesDirectory, Path.GetFileName(fileName));

        if (!File.Exists(filePath))
        {
            await SendToAsync(DatagramPacket.CreateError($"File not found: {fileName}").Serialize(), ep);
            return;
        }

        var key = ep.ToString();

        lock (_sessionLock)
        {
            if (_sessions.ContainsKey(key))
            {
                Log($"[SERVER] Session already active for {ep}, ignoring DownloadInit.");
                return;
            }

            var session = new FileTransferSession(
                socket      : _socket!,
                clientEp    : ep,
                direction   : TransferDirection.Download,
                fileName    : fileName,
                progress    : new Progress<TransferProgress>(p => OnProgress?.Invoke(p)),
                simulateLoss: _simulateLoss);

            _sessions[key] = session;
        }

        var activeSession = _sessions[key];

        // Inicia o download em background e remove a sessão ao terminar
        _ = activeSession.StartDownloadAsync(filePath, ct).ContinueWith(_ =>
        {
            lock (_sessionLock)
            {
                _sessions.TryRemove(key, out FileTransferSession? removed);
            }
            Log($"[SERVER] Session removed for {ep}");
        });
    }

    // Cria a sessão de upload; os chunks chegarão em seguida via HandleFileChunkAsync
    private void HandleUploadInit(IPEndPoint ep, DatagramPacket packet)
    {
        var fileName = packet.GetPayloadAsString().Trim();
        var key      = ep.ToString();

        lock (_sessionLock)
        {
            if (_sessions.ContainsKey(key))
            {
                Log($"[SERVER] Session already active for {ep}, ignoring UploadInit.");
                return;
            }

            var session = new FileTransferSession(
                socket      : _socket!,
                clientEp    : ep,
                direction   : TransferDirection.Upload,
                fileName    : fileName,
                progress    : new Progress<TransferProgress>(p => OnProgress?.Invoke(p)),
                simulateLoss: _simulateLoss);

            _sessions[key] = session;
            Log($"[SERVER] Upload session created for {ep} -- file: {fileName}");
        }
    }

    // Repassa o chunk para a sessão ativa do cliente que o enviou
    private async Task HandleFileChunkAsync(IPEndPoint ep, DatagramPacket packet)
    {
        var key = ep.ToString();
        if (!_sessions.TryGetValue(key, out var session))
        {
            Log($"[SERVER] No upload session for {ep}, ignoring FileChunk.");
            return;
        }

        var uploadsDir = Path.Combine(_filesDirectory, "uploads");
        await session.HandleIncomingChunkAsync(packet, uploadsDir);
    }

    // Entrega o ACK à sessão de download para desbloquear o próximo envio Stop-and-Wait
    private void HandleAck(IPEndPoint ep, DatagramPacket packet)
    {
        var key = ep.ToString();
        if (!_sessions.TryGetValue(key, out var session))
        {
            Log($"[SERVER] Stale ACK Seq={packet.SeqNum} from {ep}");
            return;
        }

        Log($"[ACK] Seq={packet.SeqNum} from {ep}");
        session.DeliverAck(packet.SeqNum);
    }

    // Finaliza o upload e remove a sessão após o cliente sinalizar que terminou
    private void HandleTransferDone(IPEndPoint ep)
    {
        var key = ep.ToString();

        if (!_sessions.TryGetValue(key, out FileTransferSession? activeSession))
            return;

        activeSession.FinalizeUpload();

        lock (_sessionLock)
        {
            _sessions.TryRemove(key, out FileTransferSession? _);
        }
    }

    // Semáforo que garante que apenas um envio UDP ocorra por vez (UdpClient não é thread-safe)
    private readonly SemaphoreSlim _serverSendLock = new(1, 1);

    private async Task SendToAsync(byte[] data, IPEndPoint ep)
    {
        await _serverSendLock.WaitAsync();
        try   { await _socket!.SendAsync(data, ep); }
        finally { _serverSendLock.Release(); }
    }

    private static void Log(string msg) =>
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] {msg}");

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _cts.Dispose();
        _serverSendLock.Dispose();
    }
}