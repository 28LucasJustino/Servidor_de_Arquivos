using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using UdpFileTransfer.Core;

namespace UdpFileTransfer.Sessions;

public enum TransferDirection { Upload, Download }
public enum TransferStatus    { Active, Completed, Failed }

// Gerencia uma única transferência de arquivo (upload ou download) com um cliente específico.
// Upload:   recebe chunks, envia ACK por chunk, grava em disco conforme a sequência avança.
// Download: lê o arquivo e envia chunk a chunk aguardando ACK antes de enviar o próximo (Stop-and-Wait).
public sealed class FileTransferSession : IAsyncDisposable
{
    private const int    ChunkSize     = DatagramPacket.MaxPayloadSize; // 4 KB por chunk
    private const int    TimeoutMs     = 500;   // Aguarda 500ms pelo ACK antes de retransmitir
    private const int    MaxRetransmit = 10;    // Desiste após 10 tentativas sem ACK
    private const double LossRate      = 0.10;  // 10% de perda simulada para demonstrar retransmissão

    public Guid              SessionId { get; } = Guid.NewGuid();
    public IPEndPoint        ClientEp  { get; }
    public TransferDirection Direction { get; }
    public string            FileName  { get; }
    public TransferStatus    Status    { get; private set; } = TransferStatus.Active;

    private readonly UdpClient               _socket;
    private readonly SemaphoreSlim           _sendLock = new(1, 1); // Serializa envios UDP
    private readonly CancellationTokenSource _cts      = new();
    private readonly IProgress<TransferProgress> _progress;
    private readonly bool                    _simulateLoss;
    private readonly Random                  _rng      = new();

    // Buffer de chunks fora de ordem (upload): chave = SeqNum, valor = payload
    private readonly ConcurrentDictionary<uint, byte[]> _uploadBuffer = new();
    private uint _expectedSeq   = 0; // Próximo SeqNum a ser gravado em disco
    private long _totalReceived = 0;

    private string? _sourceFilePath;

    // Task que completa quando a transferência termina (sucesso, falha ou cancelamento)
    private readonly TaskCompletionSource _completionTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Completion => _completionTcs.Task;

    public FileTransferSession(
        UdpClient               socket,
        IPEndPoint              clientEp,
        TransferDirection       direction,
        string                  fileName,
        IProgress<TransferProgress> progress,
        bool                    simulateLoss = false)
    {
        _socket       = socket;
        ClientEp      = clientEp;
        Direction     = direction;
        FileName      = fileName;
        _progress     = progress;
        _simulateLoss = simulateLoss;
    }

    // ── Download (Server → Client) ─────────────────────────────────────────────

    // Lê o arquivo inteiro e envia chunk a chunk via Stop-and-Wait.
    // Só avança para o próximo chunk após receber o ACK do atual.
    public async Task StartDownloadAsync(string filePath, CancellationToken serverToken)
    {
        _sourceFilePath = filePath;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, serverToken);
        var ct = linked.Token;

        try
        {
            var data        = await File.ReadAllBytesAsync(filePath, ct);
            var totalChunks = (uint)Math.Ceiling((double)data.Length / ChunkSize);

            Log($"[DOWNLOAD] Starting — {FileName} ({data.Length:N0} bytes, {totalChunks} chunks)");

            for (uint seq = 0; seq < totalChunks && !ct.IsCancellationRequested; seq++)
            {
                int offset = (int)(seq * ChunkSize);
                int len    = (int)Math.Min(ChunkSize, data.Length - offset);
                var chunk  = DatagramPacket.CreateChunk(seq, data.AsSpan(offset, len));

                await StopAndWaitSendAsync(chunk, ct);

                _progress.Report(new TransferProgress(
                    SessionId, FileName, Direction,
                    (int)seq + 1, (int)totalChunks, EventKind.ChunkAcked));
            }

            await SendRawAsync(DatagramPacket.CreateDone(totalChunks));
            Status = TransferStatus.Completed;
            Log($"[DOWNLOAD] ✓ Complete — {FileName}");
            _completionTcs.TrySetResult();
        }
        catch (OperationCanceledException)
        {
            Status = TransferStatus.Failed;
            Log($"[DOWNLOAD] Cancelled — {FileName}");
            _completionTcs.TrySetCanceled();
        }
        catch (Exception ex)
        {
            Status = TransferStatus.Failed;
            Log($"[DOWNLOAD] ERROR — {ex.Message}");
            await SendRawAsync(DatagramPacket.CreateError(ex.Message));
            _completionTcs.TrySetException(ex);
        }
    }

    // Envia um chunk e aguarda ACK. Retransmite se o ACK não chegar em 500ms.
    // Lança IOException se atingir o limite de retransmissões.
    private async Task StopAndWaitSendAsync(DatagramPacket packet, CancellationToken ct)
    {
        for (int attempt = 0; attempt <= MaxRetransmit; attempt++)
        {
            if (attempt > 0)
            {
                Log($"[RETRANSMIT] Seq={packet.SeqNum} attempt {attempt}/{MaxRetransmit}");
                _progress.Report(new TransferProgress(
                    SessionId, FileName, Direction,
                    (int)packet.SeqNum, -1, EventKind.Retransmit));
            }

            await SendRawAsync(packet);

            using var timeoutCts = new CancellationTokenSource(TimeoutMs);
            using var combined   = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

            try
            {
                await WaitForAckAsync(packet.SeqNum, combined.Token);
                return; // ACK recebido — avança para o próximo chunk
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                Log($"[TIMEOUT] Seq={packet.SeqNum} — no ACK in {TimeoutMs}ms");
                _progress.Report(new TransferProgress(
                    SessionId, FileName, Direction,
                    (int)packet.SeqNum, -1, EventKind.Timeout));
            }
        }

        throw new IOException(
            $"Transfer failed: max retransmissions reached for Seq={packet.SeqNum}.");
    }

    // Mapa de ACKs pendentes: SeqNum → TaskCompletionSource
    // DeliverAck() resolve a TCS correspondente, desbloqueando o WaitForAckAsync
    private readonly ConcurrentDictionary<uint, TaskCompletionSource<bool>> _pendingAcks = new();

    private Task WaitForAckAsync(uint seq, CancellationToken ct)
    {
        var tcs = _pendingAcks.GetOrAdd(seq,
            _ => new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously));

        ct.Register(() =>
        {
            if (_pendingAcks.TryRemove(seq, out var t))
                t.TrySetCanceled();
        });

        return tcs.Task;
    }

    // Chamado pelo UdpServer ao receber um ACK do cliente; desbloqueia o Stop-and-Wait
    public void DeliverAck(uint seq)
    {
        if (_pendingAcks.TryRemove(seq, out var tcs))
            tcs.TrySetResult(true);
    }

    // ── Upload (Client → Server) ───────────────────────────────────────────────

    // Processa um chunk recebido: (1) simula perda opcional, (2) envia ACK,
    // (3) bufferiza o chunk, (4) grava em disco os chunks em sequência contígua.
    public async Task HandleIncomingChunkAsync(DatagramPacket packet, string outputDirectory)
    {
        // Descarta o chunk sem enviar ACK para forçar retransmissão (simulação de perda)
        if (_simulateLoss && _rng.NextDouble() < LossRate)
        {
            Log($"[LOSS SIM] Dropped Seq={packet.SeqNum} (simulated {LossRate*100:0}% loss)");
            return;
        }

        await SendRawAsync(DatagramPacket.CreateAck(packet.SeqNum));
        Log($"[UPLOAD] Received Seq={packet.SeqNum}, sent ACK");

        // TryAdd é idempotente: chunks duplicados são ignorados silenciosamente
        _uploadBuffer.TryAdd(packet.SeqNum, packet.Payload);

        // Grava em disco apenas os chunks que chegaram na ordem correta
        // Chunks fora de ordem ficam no buffer até os anteriores chegarem
        var outputPath = Path.Combine(outputDirectory, FileName);
        await using var fs = new FileStream(
            outputPath, FileMode.Append, FileAccess.Write, FileShare.None,
            bufferSize: 65536, useAsync: true);

        while (_uploadBuffer.TryRemove(_expectedSeq, out var chunk))
        {
            await fs.WriteAsync(chunk);
            _totalReceived += chunk.Length;
            _expectedSeq++;
        }
    }

    // Marca o upload como concluído após receber TransferDone do cliente
    public void FinalizeUpload()
    {
        Status = TransferStatus.Completed;
        Log($"[UPLOAD] ✓ Complete — {FileName} ({_totalReceived:N0} bytes written)");
        _completionTcs.TrySetResult();
    }

    // Serializa e envia um pacote ao cliente desta sessão (thread-safe via semáforo)
    private async Task SendRawAsync(DatagramPacket packet)
    {
        var bytes = packet.Serialize();
        await _sendLock.WaitAsync();
        try
        {
            await _socket.SendAsync(bytes, ClientEp, CancellationToken.None);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private static void Log(string message) =>
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] {message}");

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _sendLock.Dispose();
        _cts.Dispose();
        _completionTcs.TrySetCanceled();
    }
}

public enum EventKind { ChunkSent, ChunkAcked, Timeout, Retransmit, Error }

public record TransferProgress(
    Guid              SessionId,
    string            FileName,
    TransferDirection Direction,
    int               CurrentChunk, // -1 para eventos sem contexto de chunk específico
    int               TotalChunks,  // -1 se ainda desconhecido
    EventKind         Event);