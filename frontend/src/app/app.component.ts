import { Component, OnInit, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';

// Shape unificado usado internamente — sempre PascalCase após normalização
interface Sessao {
  SessionId:    string;
  FileName:     string;
  Direction:    string;
  CurrentChunk: number;
  TotalChunks:  number;
  Event:        string;
}

interface FileInfo {
  name: string;
  size: number;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss'
})
export class AppComponent implements OnInit {
  title = 'frontend';

  // Signal com o último evento recebido via SSE — atualiza a barra de progresso
  progressoTransferencia = signal<Sessao | null>(null);

  // Histórico acumulado de TODAS as sessões (uma entrada por SessionId)
  historicoTransferencias = signal<Sessao[]>([]);

  arquivosServidor = signal<FileInfo[]>([]);

  tempoRestante           = signal<string>('00:00');
  velocidadeTransferencia = signal<string>('0 KB/s');
  tipoArquivo             = signal<string>('TXT');

  private startTime: number = 0;
  private currentSessionId: string | null = null;
  private readonly chunkSizeInBytes = 4096;

  // Map local: chave = SessionId normalizado
  private historicoMap = new Map<string, Sessao>();

  constructor(private http: HttpClient) {}

  ngOnInit(): void {
    this.carregarArquivos();
    this.carregarHistorico();
    this.conectarAoHubDeProgresso();
  }

  carregarArquivos(): void {
    this.http.get<FileInfo[]>('http://localhost:5000/api/files').subscribe({
      next: (dados) => this.arquivosServidor.set(dados),
      error: (err)  => console.error('Erro ao carregar storage:', err)
    });
  }

  onFileSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    if (!input.files || input.files.length === 0) return;

    const file     = input.files[0];
    const formData = new FormData();
    formData.append('file', file);

    this.http.post('http://localhost:5000/api/upload', formData).subscribe({
      next: () => this.carregarArquivos(),
      error: (err) => console.error('Falha no upload de controle:', err)
    });
  }

  // Carrega histórico do backend (vem em camelCase via Results.Ok) e normaliza
  carregarHistorico(): void {
    this.http.get<any[]>('http://localhost:5000/api/history').subscribe({
      next: (dados) => {
        dados.forEach(d => {
          const s = this.normalizar(d);
          if (s.SessionId) this.historicoMap.set(s.SessionId, s);
        });
        this.historicoTransferencias.set([...this.historicoMap.values()]);
      },
      error: (err) => console.error('Erro ao buscar histórico:', err)
    });
  }

  private conectarAoHubDeProgresso(): void {
    const eventSource = new EventSource('http://localhost:5000/progress-hub/subscribe');

    eventSource.onmessage = (event) => {
      const raw  = JSON.parse(event.data);
      const data = this.normalizar(raw);

      if (!data.SessionId) return; // descarta se vier sem ID

      if (this.currentSessionId !== data.SessionId || data.CurrentChunk === 1) {
        this.startTime        = performance.now();
        this.currentSessionId = data.SessionId;
      }

      this.progressoTransferencia.set(data);

      const extensao = (data.FileName || '').split('.').pop()?.toUpperCase() || 'ARQUIVO';
      this.tipoArquivo.set(extensao);

      this.calcularMetricas(data.CurrentChunk, data.TotalChunks);

      // Acumula no Map e atualiza o signal do histórico
      this.historicoMap.set(data.SessionId, { ...data });
      this.historicoTransferencias.set([...this.historicoMap.values()]);

      if (data.CurrentChunk === data.TotalChunks) {
        this.tempoRestante.set('Concluído');
        this.carregarArquivos();
      }
    };

    eventSource.onerror = (err) => console.error('Erro na conexão SSE:', err);
  }

  /**
   * Normaliza um objeto vindo do backend para PascalCase,
   * aceitando tanto camelCase (Results.Ok) quanto PascalCase (JsonSerializer direto).
   */
  private normalizar(raw: any): Sessao {
    return {
      SessionId:    raw['SessionId']    ?? raw['sessionId']    ?? '',
      FileName:     raw['FileName']     ?? raw['fileName']     ?? '',
      Direction:    raw['Direction']    ?? raw['direction']    ?? '',
      CurrentChunk: raw['CurrentChunk'] ?? raw['currentChunk'] ?? 0,
      TotalChunks:  raw['TotalChunks']  ?? raw['totalChunks']  ?? 0,
      Event:        raw['Event']        ?? raw['event']        ?? '',
    };
  }

  getExtensao(fileName: string): string {
    return (fileName || '').split('.').pop()?.toUpperCase() || 'ARQUIVO';
  }

  isConcluido(sessao: Sessao): boolean {
    return sessao.TotalChunks > 0 && sessao.CurrentChunk >= sessao.TotalChunks;
  }

  private calcularMetricas(currentChunk: number, totalChunks: number): void {
    const duracaoSegundos = (performance.now() - this.startTime) / 1000;

    if (duracaoSegundos <= 0) {
      this.velocidadeTransferencia.set('Calculando...');
      this.tempoRestante.set('--:--');
      return;
    }

    const totalBytesTransmitidos = currentChunk * this.chunkSizeInBytes;
    const bytesPorSegundo        = totalBytesTransmitidos / duracaoSegundos;

    if (bytesPorSegundo > 1024 * 1024) {
      this.velocidadeTransferencia.set(`${(bytesPorSegundo / (1024 * 1024)).toFixed(2)} MB/s`);
    } else {
      this.velocidadeTransferencia.set(`${(bytesPorSegundo / 1024).toFixed(1)} KB/s`);
    }

    const bytesRestantes    = (totalChunks - currentChunk) * this.chunkSizeInBytes;
    const segundosRestantes = bytesPorSegundo > 0 ? bytesRestantes / bytesPorSegundo : 0;

    if (segundosRestantes <= 0) {
      this.tempoRestante.set('00:00');
    } else {
      const minutos  = Math.floor(segundosRestantes / 60);
      const segundos = Math.floor(segundosRestantes % 60);
      this.tempoRestante.set(
        `${minutos.toString().padStart(2, '0')}:${segundos.toString().padStart(2, '0')}`
      );
    }
  }

  formatSize(bytes: number): string {
    if (bytes === 0) return '0 Bytes';
    const k     = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i     = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
}