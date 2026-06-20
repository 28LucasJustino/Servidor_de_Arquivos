using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using UdpFileTransfer;
using UdpFileTransfer.Sessions;

var builder = WebApplication.CreateBuilder(args);

// Permite que o Angular acesse a API sem bloqueio do navegador
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));

// Hub que distribui eventos de progresso para os clientes SSE
var progressHub = new ProgressHub();
builder.Services.AddSingleton(progressHub);

// Cria o servidor UDP e conecta seus eventos ao ProgressHub
builder.Services.AddSingleton(_ =>
{
    var server = new UdpServer(
        port          : 5000,
        filesDirectory: builder.Configuration["FilesDirectory"],
        simulateLoss  : builder.Configuration.GetValue<bool>("SimulateLoss"));

    // Sempre que um chunk é enviado/recebido/retransmitido, encaminha ao hub
    // Push recebe TransferProgress e faz a conversão para DTO internamente
    server.OnProgress += p => progressHub.Push(p);

    return server;
});

// Integra o UdpServer ao ciclo de vida do ASP.NET (start/stop automático)
builder.Services.AddHostedService<UdpServerHostedService>();

var app = builder.Build();
app.UseCors();

// ── Endpoint 1: Lista os arquivos disponíveis no servidor ─────────────────────
app.MapGet("/api/files", (UdpServer server) =>
{
    var dir = server.FilesDirectory;

    var files = Directory.GetFiles(dir, "*", SearchOption.AllDirectories)
        .Select(f => new
        {
            name = Path.GetRelativePath(dir, f).Replace(Path.DirectorySeparatorChar, '/'),
            size = new FileInfo(f).Length
        });

    return Results.Ok(files);
});

// ── Endpoint 2: Retorna o histórico de sessões de transferência ───────────────
app.MapGet("/api/history", (ProgressHub hub) =>
{
    return Results.Ok(hub.GetHistory());
});

// ── Endpoint 3: Recebe um arquivo do Angular e simula o fluxo de chunks UDP ──
// O navegador não suporta UDP puro, então o upload chega via HTTP multipart.
// Aqui simulamos o comportamento de chunks: lemos o arquivo em blocos de 4 KB,
// emitimos um evento de progresso por bloco e gravamos em disco.
app.MapPost("/api/upload", async (HttpContext context, UdpServer server, ProgressHub hub) =>
{
    if (!context.Request.HasFormContentType)
        return Results.BadRequest("Formato de requisição inválido. Esperado Multipart FormData.");

    var form = await context.Request.ReadFormAsync();
    var file = form.Files.GetFile("file");

    if (file == null || file.Length == 0)
        return Results.BadRequest("Nenhum arquivo foi enviado.");

    var fileName   = file.FileName;
    var totalBytes = file.Length;
    var sessionId  = Guid.NewGuid();

    // Mesmo tamanho de chunk usado no protocolo UDP real
    int chunkSize   = 4096;
    int totalChunks = (int)Math.Ceiling((double)totalBytes / chunkSize);

    var dir        = server.FilesDirectory;
    var targetPath = Path.Combine(dir, "uploads", fileName);
    Directory.CreateDirectory(Path.GetDirectoryName(targetPath)!);

    using (var stream     = new FileStream(targetPath, FileMode.Create, FileAccess.Write, FileShare.None, chunkSize, useAsync: true))
    using (var fileStream = file.OpenReadStream())
    {
        byte[] buffer      = new byte[chunkSize];
        int    bytesRead;
        int    currentChunk = 0;

        while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            await stream.WriteAsync(buffer, 0, bytesRead);
            currentChunk++;

            // Notifica o Angular via SSE para atualizar a barra de progresso
            hub.Push(new TransferProgress(
                sessionId,
                fileName,
                TransferDirection.Upload,
                currentChunk,
                totalChunks,
                EventKind.ChunkSent
            ));

            // Delay para o efeito visual da barra de progresso funcionar no Angular
            await Task.Delay(15);
        }
    }

    // Evento final: sinaliza ao Angular que a transferência chegou a 100%
    hub.Push(new TransferProgress(
        sessionId,
        fileName,
        TransferDirection.Upload,
        totalChunks,
        totalChunks,
        EventKind.ChunkAcked
    ));

    Console.WriteLine($"[HTTP BRIDGE] Arquivo '{fileName}' recebido com sucesso no Storage.");
    return Results.Ok(new { message = "Upload completo processado com sucesso!" });
});

// ── Endpoint 4: Serve um arquivo para download direto via HTTP ────────────────
// O parâmetro {*fileName} aceita caminhos com subpastas (ex: uploads/foto.png)
app.MapGet("/api/download/{*fileName}", (string fileName, UdpServer server) =>
{
    var dir  = server.FilesDirectory;
    var path = Path.Combine(dir, fileName);

    if (!File.Exists(path))
        return Results.NotFound("Arquivo não localizado no Storage.");

    var bytes        = File.ReadAllBytes(path);
    var downloadName = Path.GetFileName(fileName);
    return Results.File(bytes, "application/octet-stream", downloadName);
});

// ── Endpoint 5: SSE — Canal de eventos em tempo real para o Angular ───────────
// O Angular abre uma conexão EventSource aqui e a mantém aberta.
// Cada evento de progresso (chunk, timeout, retransmissão) é enviado como JSON
// nesta conexão assim que ocorre, sem necessidade de polling.
app.MapGet("/progress-hub/subscribe", async (HttpContext ctx, ProgressHub hub, CancellationToken ct) =>
{
    // Headers obrigatórios do protocolo SSE
    ctx.Response.Headers.Append("Content-Type", "text/event-stream");
    ctx.Response.Headers.Append("Cache-Control", "no-cache");
    ctx.Response.Headers.Append("Connection", "keep-alive");

    var reader = hub.Subscribe(); // Cria um canal exclusivo para este cliente
    try
    {
        while (!ct.IsCancellationRequested)
        {
            var dto  = await reader.ReadAsync(ct);
            var json = JsonSerializer.Serialize(dto);
            await ctx.Response.WriteAsync($"data: {json}\n\n", ct); // Formato SSE
            await ctx.Response.Body.FlushAsync(ct);
        }
    }
    catch (OperationCanceledException) { } // Cliente desconectou — encerra normalmente
    finally
    {
        hub.Unsubscribe(reader); // Remove o canal para evitar vazamento de memória
    }
});

app.Run();

// ── ProgressHub ───────────────────────────────────────────────────────────────
// Barramento central: recebe eventos do UdpServer e os distribui para todos
// os clientes SSE conectados. Também mantém um histórico em memória por sessão.
public sealed class ProgressHub
{
    // Um Channel por cliente SSE conectado
    private readonly ConcurrentDictionary<Channel<TransferProgressDto>, bool> _subscribers = new();

    // Último estado conhecido de cada sessão, consultado pelo /api/history
    private readonly ConcurrentDictionary<string, TransferProgressDto> _historyLog = new();

    // Recebe um evento, atualiza o histórico e envia para todos os clientes SSE
    public void Push(TransferProgress p)
    {
        var dto = new TransferProgressDto(
            p.SessionId.ToString(),
            p.FileName,
            p.Direction.ToString(),
            p.CurrentChunk,
            p.TotalChunks,
            p.Event.ToString());

       _historyLog[dto.SessionId] = dto; // Sobrescreve com o estado mais recente

        foreach (var (ch, _) in _subscribers)
            ch.Writer.TryWrite(dto); // TryWrite não bloqueia; descarta se o canal estiver cheio
    }

    public IEnumerable<TransferProgressDto> GetHistory() =>
        _historyLog.Values.ToList();

    // Cria e registra um canal para um novo cliente SSE
    // Capacidade de 200 mensagens; descarta as mais antigas se o cliente ficar para trás
    public ChannelReader<TransferProgressDto> Subscribe()
    {
        var ch = Channel.CreateBounded<TransferProgressDto>(
            new BoundedChannelOptions(200) { FullMode = BoundedChannelFullMode.DropOldest });
        _subscribers.TryAdd(ch, true);
        return ch.Reader;
    }

    // Remove o canal quando o cliente desconecta
    public void Unsubscribe(ChannelReader<TransferProgressDto> reader)
    {
        var toRemove = _subscribers.Keys.FirstOrDefault(ch => ch.Reader == reader);
        if (toRemove is not null)
            _subscribers.TryRemove(toRemove, out _);
    }
}

// DTO enviado ao Angular via SSE e /api/history
public record TransferProgressDto(
    string SessionId,
    string FileName,
    string Direction,
    int    CurrentChunk,
    int    TotalChunks,
    string Event);

// Adapta o UdpServer ao ciclo de vida do ASP.NET como serviço em background
public sealed class UdpServerHostedService : BackgroundService
{
    private readonly UdpServer _server;
    public UdpServerHostedService(UdpServer server) => _server = server;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        _server.StartAsync(stoppingToken);

    public override async Task StopAsync(CancellationToken ct)
    {
        await _server.StopAsync();
        await base.StopAsync(ct);
    }
}