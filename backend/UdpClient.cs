using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using UdpFileTransfer.Core;
using UdpFileTransfer.Sessions;

namespace UdpFileTransfer;

/// <summary>
/// Client-side UDP transfer engine.
///
/// Mirrors the server design: one receive loop, SemaphoreSlim-guarded sends,
/// ConcurrentDictionary for buffering out-of-order download chunks.
/// </summary>
public sealed class UdpTransferClient : IAsyncDisposable
{
    // ── Config ─────────────────────────────────────────────────────────────
    private const int ChunkSize     = DatagramPacket.MaxPayloadSize;
    private const int TimeoutMs     = 500;
    private const int MaxRetransmit = 10;
    private const double LossRate   = 0.10;

    private readonly IPEndPoint _serverEp;
    private readonly bool       _simulateLoss;
    private readonly Random     _rng = new();

    // ── Socket ─────────────────────────────────────────────────────────────
    private readonly UdpClient    _socket;
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    // ── Download buffer ─────────────────────────────────────────────────────
    private readonly ConcurrentDictionary<uint, byte[]> _downloadBuffer = new();
    private TaskCompletionSource<bool>? _downloadDoneTcs;

    // ── ACK queue for upload path ───────────────────────────────────────────
    private readonly ConcurrentDictionary<uint, TaskCompletionSource<bool>> _pendingAcks = new();

    // ── File-list result ────────────────────────────────────────────────────
    private TaskCompletionSource<string>? _listTcs;

    // ── Lifetime ────────────────────────────────────────────────────────────
    private readonly CancellationTokenSource _cts = new();
    private Task? _receiveTask;

    public event Action<TransferProgress>? OnProgress;

    public UdpTransferClient(string serverHost = "127.0.0.1",
                             int    serverPort  = 5000,
                             bool   simulateLoss = false)
    {
        _serverEp     = new IPEndPoint(IPAddress.Parse(serverHost), serverPort);
        _simulateLoss = simulateLoss;
        _socket       = new UdpClient();
        _socket.Connect(_serverEp);
    }

    public void Start()
    {
        _receiveTask = Task.Run(ReceiveLoopAsync);
        Log("[CLIENT] Started, receive loop running.");
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Receive loop
    // ═══════════════════════════════════════════════════════════════════════

    private async Task ReceiveLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                var result = await _socket.ReceiveAsync(_cts.Token);
                var packet = DatagramPacket.Deserialize(result.Buffer);
                Log($"[RX] Server → {packet}");

                switch (packet.Type)
                {
                    case MessageType.FileList:
                        _listTcs?.TrySetResult(packet.GetPayloadAsString());
                        break;

                    case MessageType.FileChunk:
                        await HandleDownloadChunkAsync(packet);
                        break;

                    case MessageType.TransferDone:
                        _downloadDoneTcs?.TrySetResult(true);
                        break;

                    case MessageType.Ack:
                        if (_pendingAcks.TryRemove(packet.SeqNum, out var tcs))
                            tcs.TrySetResult(true);
                        break;

                    case MessageType.Error:
                        Log($"[CLIENT] Server error: {packet.GetPayloadAsString()}");
                        break;
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { Log($"[CLIENT] Receive error: {ex.Message}"); }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Public API: List files
    // ═══════════════════════════════════════════════════════════════════════

    public async Task<string> RequestFileListAsync(CancellationToken ct = default)
    {
        _listTcs = new TaskCompletionSource<string>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var req = new DatagramPacket { Type = MessageType.RequestList };
        await SendRawAsync(req.Serialize());

        using var timeout = new CancellationTokenSource(3000);
        using var linked  = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
        linked.Token.Register(() => _listTcs.TrySetCanceled());

        return await _listTcs.Task;
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Public API: Download
    // ═══════════════════════════════════════════════════════════════════════

    public async Task DownloadFileAsync(
        string fileName, string savePath, CancellationToken ct = default)
    {
        _downloadBuffer.Clear();
        _downloadDoneTcs = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var init = new DatagramPacket
        {
            Type    = MessageType.DownloadInit,
            Payload = System.Text.Encoding.UTF8.GetBytes(fileName),
        };
        await SendRawAsync(init.Serialize());
        Log($"[CLIENT] Requested download of '{fileName}'");

        await _downloadDoneTcs.Task.WaitAsync(ct);

        // Reassemble in order
        var orderedKeys = _downloadBuffer.Keys.OrderBy(k => k).ToArray();
        await using var fs = new FileStream(savePath,
            FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 65536, useAsync: true);

        foreach (var key in orderedKeys)
        {
            if (_downloadBuffer.TryGetValue(key, out var chunk))
                await fs.WriteAsync(chunk, ct);
        }

        Log($"[CLIENT] ✓ Download complete → {savePath}");
    }

    // ── Download chunk handler (receiver-side with ACK + loss sim) ──────────

    private async Task HandleDownloadChunkAsync(DatagramPacket packet)
    {
        if (_simulateLoss && _rng.NextDouble() < LossRate)
        {
            Log($"[LOSS SIM] Dropped Seq={packet.SeqNum} (simulated {LossRate*100:0}% loss)");
            return; // No ACK → forces server retransmit
        }

        // Buffer chunk (idempotent: duplicate ACKs are fine)
        _downloadBuffer.TryAdd(packet.SeqNum, packet.Payload);

        // Send ACK
        var ack = DatagramPacket.CreateAck(packet.SeqNum);
        await SendRawAsync(ack.Serialize());
        Log($"[CLIENT] ACK Seq={packet.SeqNum}");

        OnProgress?.Invoke(new TransferProgress(
            Guid.Empty, string.Empty, TransferDirection.Download,
            (int)packet.SeqNum, -1, EventKind.ChunkAcked));
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Public API: Upload
    // ═══════════════════════════════════════════════════════════════════════

    public async Task UploadFileAsync(
        string filePath, CancellationToken ct = default)
    {
        var fileName = Path.GetFileName(filePath);
        var data     = await File.ReadAllBytesAsync(filePath, ct);
        var total    = (uint)Math.Ceiling((double)data.Length / ChunkSize);

        // Announce upload
        var init = new DatagramPacket
        {
            Type    = MessageType.UploadInit,
            Payload = System.Text.Encoding.UTF8.GetBytes(fileName),
        };
        await SendRawAsync(init.Serialize());
        Log($"[CLIENT] Upload init — {fileName} ({data.Length:N0} bytes, {total} chunks)");

        // Stop-and-Wait send loop
        for (uint seq = 0; seq < total && !ct.IsCancellationRequested; seq++)
        {
            int offset = (int)(seq * ChunkSize);
            int len    = (int)Math.Min(ChunkSize, data.Length - offset);
            var chunk  = DatagramPacket.CreateChunk(seq, data.AsSpan(offset, len));

            await StopAndWaitSendAsync(chunk, ct);

            OnProgress?.Invoke(new TransferProgress(
                Guid.Empty, fileName, TransferDirection.Upload,
                (int)seq + 1, (int)total, EventKind.ChunkAcked));
        }

        // Signal end
        await SendRawAsync(DatagramPacket.CreateDone(total).Serialize());
        Log($"[CLIENT] ✓ Upload complete — {fileName}");
    }

    // ── Stop-and-Wait (upload / client sends, server ACKs) ─────────────────

    private async Task StopAndWaitSendAsync(DatagramPacket packet, CancellationToken ct)
    {
        for (int attempt = 0; attempt <= MaxRetransmit; attempt++)
        {
            if (attempt > 0)
            {
                Log($"[RETRANSMIT] Seq={packet.SeqNum} attempt {attempt}/{MaxRetransmit}");
                OnProgress?.Invoke(new TransferProgress(
                    Guid.Empty, string.Empty, TransferDirection.Upload,
                    (int)packet.SeqNum, -1, EventKind.Retransmit));
            }

            var tcs = _pendingAcks.GetOrAdd(packet.SeqNum,
                _ => new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously));

            await SendRawAsync(packet.Serialize());

            using var timeoutCts = new CancellationTokenSource(TimeoutMs);
            using var linked     = CancellationTokenSource.CreateLinkedTokenSource(
                ct, timeoutCts.Token);
            linked.Token.Register(() => tcs.TrySetCanceled());

            try
            {
                await tcs.Task;
                _pendingAcks.TryRemove(packet.SeqNum, out _);
                return;
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                Log($"[TIMEOUT] Seq={packet.SeqNum} — no ACK in {TimeoutMs}ms");
                OnProgress?.Invoke(new TransferProgress(
                    Guid.Empty, string.Empty, TransferDirection.Upload,
                    (int)packet.SeqNum, -1, EventKind.Timeout));
                _pendingAcks.TryRemove(packet.SeqNum, out _);
            }
        }

        throw new IOException(
            $"Upload failed: max retransmissions reached for Seq={packet.SeqNum}.");
    }

    // ── Send helper ─────────────────────────────────────────────────────────

    private async Task SendRawAsync(byte[] data)
    {
        await _sendLock.WaitAsync();
        try   { await _socket.SendAsync(data, data.Length); }
        finally { _sendLock.Release(); }
    }

    private static void Log(string msg) =>
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] {msg}");

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _socket.Dispose();
        _sendLock.Dispose();
        _cts.Dispose();
    }
}
