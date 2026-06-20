// file-transfer.service.ts
import { Injectable, OnDestroy } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, Subject, BehaviorSubject } from 'rxjs';
import { tap } from 'rxjs/operators';

// ── DTOs ──────────────────────────────────────────────────────────────────

export interface ServerFile {
  name: string;
  size: number;
}

export interface DownloadMeta {
  fileName: string;
  size: number;
  udpPort: number;
}

export type EventKind =
  | 'ChunkSent'
  | 'ChunkAcked'
  | 'Timeout'
  | 'Retransmit'
  | 'Error';

export interface TransferEvent {
  sessionId:    string;
  fileName:     string;
  direction:    'Upload' | 'Download';
  currentChunk: number;
  totalChunks:  number;
  event:        EventKind;
  timestamp:    Date;
}

/** Shape do DTO retornado pelo endpoint GET /api/history */
export interface TransferHistoryDto {
  sessionId:    string;
  fileName:     string;
  direction:    string;
  currentChunk: number;
  totalChunks:  number;
  event:        string;
}

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

// ── Service ───────────────────────────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class FileTransferService implements OnDestroy {
  private readonly API = 'http://localhost:5000/api';

  /** All transfer events from the SSE stream. */
  readonly events$ = new Subject<TransferEvent>();

  /** Aggregated stats per active session. */
  readonly sessions$ = new BehaviorSubject<Map<string, SessionStats>>(new Map());

  private sseSource: EventSource | null = null;

  constructor(private http: HttpClient) {
    this.connectSse();
  }

  // ── API calls ─────────────────────────────────────────────────────────

  listFiles(): Observable<ServerFile[]> {
    return this.http.get<ServerFile[]>(`${this.API}/files`);
  }

  getHistory(): Observable<TransferHistoryDto[]> {
    return this.http.get<TransferHistoryDto[]>(`${this.API}/history`);
  }

  initiateDownload(fileName: string): Observable<DownloadMeta> {
    return this.http.post<DownloadMeta>(`${this.API}/download`, { fileName }).pipe(
      tap(meta => console.log('[Service] Download meta:', meta))
    );
  }

  // ── SSE stream ────────────────────────────────────────────────────────

  private connectSse(): void {
    this.sseSource = new EventSource(`http://localhost:5000/progress-hub/subscribe`);

    this.sseSource.onmessage = (raw: MessageEvent) => {
      try {
        const dto = JSON.parse(raw.data);
        const evt: TransferEvent = { ...dto, timestamp: new Date() };
        this.events$.next(evt);
        this.updateSessionStats(evt);
      } catch { /* malformed event */ }
    };

    this.sseSource.onerror = () => {
      setTimeout(() => this.connectSse(), 3000);
    };
  }

  private updateSessionStats(evt: TransferEvent): void {
    const map = new Map(this.sessions$.value);
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
      timeouts:    evt.event === 'Timeout'     ? existing.timeouts + 1    : existing.timeouts,
      retransmits: evt.event === 'Retransmit'  ? existing.retransmits + 1 : existing.retransmits,
      status: (evt.totalChunks > 0 && newAcked >= evt.totalChunks) ? 'done' : 'active',
    };

    map.set(evt.sessionId, updated);
    this.sessions$.next(map);
  }

  ngOnDestroy(): void {
    this.sseSource?.close();
    this.events$.complete();
  }
}
