import { Injectable, OnDestroy } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, Subject, BehaviorSubject } from 'rxjs';
import { tap } from 'rxjs/operators';

// ── DTOs ──────────────────────────────────────────────────────────────────────

// Arquivo disponível no servidor (retornado por GET /api/files)
export interface ServerFile {
  name: string;
  size: number;
}

// Metadados retornados ao iniciar um download (porta UDP, nome, tamanho)
export interface DownloadMeta {
  fileName: string;
  size:     number;
  udpPort:  number;
}

// Tipos de evento que o backend pode emitir via SSE
export type EventKind =
  | 'ChunkSent'
  | 'ChunkAcked'
  | 'Timeout'
  | 'Retransmit'
  | 'Error';

// Evento de progresso recebido via SSE, com timestamp adicionado no frontend
export interface TransferEvent {
  sessionId:    string;
  fileName:     string;
  direction:    'Upload' | 'Download';
  currentChunk: number;
  totalChunks:  number;
  event:        EventKind;
  timestamp:    Date;
}

// Shape do DTO retornado pelo GET /api/history
export interface TransferHistoryDto {
  sessionId:    string;
  fileName:     string;
  direction:    string;
  currentChunk: number;
  totalChunks:  number;
  event:        string;
}

// Estatísticas acumuladas de uma sessão, calculadas a partir dos eventos SSE
export interface SessionStats {
  sessionId:   string;
  fileName:    string;
  direction:   'Upload' | 'Download';
  ackedChunks: number;
  totalChunks: number;
  timeouts:    number;
  retransmits: number;
  status:      'active' | 'done' | 'failed';
}

// ── Service ───────────────────────────────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class FileTransferService implements OnDestroy {
  private readonly API = 'http://localhost:5000/api';

  // Stream de todos os eventos SSE recebidos; componentes podem subscrever para reagir
  readonly events$ = new Subject<TransferEvent>();

  // Mapa de sessões ativas com estatísticas acumuladas; atualizado a cada evento SSE
  readonly sessions$ = new BehaviorSubject<Map<string, SessionStats>>(new Map());

  private sseSource: EventSource | null = null;

  constructor(private http: HttpClient) {
    this.connectSse(); // Inicia a conexão SSE imediatamente ao criar o serviço
  }

  // Retorna a lista de arquivos disponíveis no servidor
  listFiles(): Observable<ServerFile[]> {
    return this.http.get<ServerFile[]>(`${this.API}/files`);
  }

  // Retorna o histórico de sessões armazenado em memória no backend
  getHistory(): Observable<TransferHistoryDto[]> {
    return this.http.get<TransferHistoryDto[]>(`${this.API}/history`);
  }

  // Inicia um download UDP e retorna os metadados da sessão criada
  initiateDownload(fileName: string): Observable<DownloadMeta> {
    return this.http.post<DownloadMeta>(`${this.API}/download`, { fileName }).pipe(
      tap(meta => console.log('[Service] Download meta:', meta))
    );
  }

  // Abre a conexão SSE com o backend e processa eventos em tempo real.
  // Reconecta automaticamente após 3 segundos em caso de falha.
  private connectSse(): void {
    this.sseSource = new EventSource(`http://localhost:5000/progress-hub/subscribe`);

    this.sseSource.onmessage = (raw: MessageEvent) => {
      try {
        const dto = JSON.parse(raw.data);
        const evt: TransferEvent = { ...dto, timestamp: new Date() };
        this.events$.next(evt);         // Publica o evento para qualquer subscriber
        this.updateSessionStats(evt);   // Atualiza as estatísticas acumuladas da sessão
      } catch { /* descarta eventos malformados */ }
    };

    this.sseSource.onerror = () => {
      setTimeout(() => this.connectSse(), 3000); // Tenta reconectar após 3s
    };
  }

  // Atualiza (ou cria) as estatísticas da sessão no BehaviorSubject.
  // Incrementa ackedChunks, timeouts e retransmits conforme o tipo do evento.
  // Marca a sessão como 'done' quando ackedChunks atingir totalChunks.
  private updateSessionStats(evt: TransferEvent): void {
    const map      = new Map(this.sessions$.value);
    const existing = map.get(evt.sessionId) ?? {
      sessionId:   evt.sessionId,
      fileName:    evt.fileName,
      direction:   evt.direction,
      ackedChunks: 0,
      totalChunks: evt.totalChunks,
      timeouts:    0,
      retransmits: 0,
      status:      'active' as const,
    };

    const newAcked = evt.event === 'ChunkAcked'
      ? existing.ackedChunks + 1
      : existing.ackedChunks;

    const updated: SessionStats = {
      ...existing,
      totalChunks: evt.totalChunks > 0 ? evt.totalChunks : existing.totalChunks,
      ackedChunks: newAcked,
      timeouts:    evt.event === 'Timeout'    ? existing.timeouts + 1    : existing.timeouts,
      retransmits: evt.event === 'Retransmit' ? existing.retransmits + 1 : existing.retransmits,
      status:      (evt.totalChunks > 0 && newAcked >= evt.totalChunks) ? 'done' : 'active',
    };

    map.set(evt.sessionId, updated);
    this.sessions$.next(map);
  }

  // Fecha a conexão SSE e completa o Subject quando o serviço é destruído
  ngOnDestroy(): void {
    this.sseSource?.close();
    this.events$.complete();
  }
}