# 📡 Transferência Confiável de Arquivos com UDP

Sistema de transferência de arquivos construído sobre UDP puro, com confiabilidade implementada manualmente na camada de aplicação — sem nenhuma biblioteca de controle de fluxo ou entrega garantida pronta.

---

## 🗂 Estrutura do Projeto

```
/
├── backend/         → Servidor UDP + API HTTP (C# / .NET 10)
│   ├── UdpServer.cs
│   ├── UdpClient.cs
│   ├── FileTransferSession.cs
│   ├── DatagramPacket.cs
│   ├── Program.cs
│   └── appsettings.json
│
└── frontend/        → Painel de monitoramento em tempo real (Angular)
    ├── app.component.ts
    ├── app.component.html
    ├── app.component.scss
    └── file-transfer.service.ts
```

---

## ⚙️ Dependências

### Backend

| Ferramenta | Versão mínima | Link |
|---|---|---|
| .NET SDK | 10.0 | https://dotnet.microsoft.com/download |

> Verifique: `dotnet --version`

Todas as demais dependências são gerenciadas automaticamente pelo NuGet.

### Frontend

| Ferramenta | Versão mínima | Link |
|---|---|---|
| Node.js | 18.x | https://nodejs.org |
| Angular CLI | 17.x | via npm |

> Verifique:
> ```bash
> node --version
> ng version
> ```

Instale o Angular CLI globalmente (caso não tenha):
```bash
npm install -g @angular/cli
```

---

## 🚀 Como Rodar

### 1. Backend

```bash
# Entre na pasta do backend
cd backend

# Restaure as dependências (só na primeira vez)
dotnet restore

# Inicie o servidor
dotnet run
```

**O que aparece no console ao subir:**
```
[SERVER] UDP socket bound on :5000
[SERVER] Serving files from: ./ServerFiles
[SERVER] Loss simulation: ON (10%)
[SERVER] Receive loop started.
```

O backend sobe dois serviços simultaneamente:
- **Servidor UDP** na porta `5000` → protocolo de transferência de arquivos
- **API HTTP** na porta `5000` → endpoints consumidos pelo painel Angular

---

### 2. Frontend

> Abra um **novo terminal** — deixe o backend rodando no anterior.

```bash
# Entre na pasta do frontend
cd frontend

# Instale as dependências (só na primeira vez)
npm install

# Inicie o servidor de desenvolvimento
ng serve
```

Acesse no navegador:
```
http://localhost:4200
```

---

## 🖥️ Como Usar o Sistema

Com backend e frontend rodando:

1. Abra `http://localhost:4200` no navegador
2. Clique em **"Clique para selecionar e simular transferência"**
3. Escolha qualquer arquivo do seu computador
4. Acompanhe a transferência em tempo real no painel:

| Campo | O que mostra |
|---|---|
| Arquivo | Nome e extensão do arquivo |
| Fluxo | Direção (Upload / Download) |
| Velocidade Atual | KB/s ou MB/s calculado em tempo real |
| Tempo Restante | Estimativa baseada na velocidade atual |
| Pacotes Reassembrados | `chunk atual / total` (ex: 41 / 41) |
| Último Evento UDP | `ChunkAcked`, `Timeout`, `Retransmit`... |

5. O **Histórico** na parte inferior acumula todas as sessões da execução atual

---

## 🔧 Configuração (`appsettings.json`)

```json
{
  "FilesDirectory": "./ServerFiles",
  "SimulateLoss": true
}
```

| Chave | Descrição |
|---|---|
| `FilesDirectory` | Pasta onde o servidor armazena e serve os arquivos |
| `SimulateLoss` | `true` ativa 10% de perda de pacotes para demonstrar retransmissão |

Para desativar a simulação de perda:
```json
"SimulateLoss": false
```

---

## 📦 Parâmetros Técnicos do Protocolo

| Parâmetro | Valor |
|---|---|
| Tamanho de cada chunk | 4.096 bytes (4 KB) |
| Timeout por ACK | 500 ms |
| Máximo de retransmissões | 10 tentativas por chunk |
| Taxa de perda simulada | 10% |
| Porta UDP | 5000 |
| Estratégia de envio | Stop-and-Wait |

---

## 📐 Formato do Datagrama

Cada pacote enviado segue o cabeçalho customizado abaixo (big-endian):

```
 Byte 0      Byte 1      Bytes 2–5     Bytes 6–9      Bytes 10–N
┌──────────┬──────────┬─────────────┬──────────────┬─────────────────┐
│  Type    │  Flags   │   SeqNum    │  PayloadLen  │    Payload      │
│ (1 byte) │ (1 byte) │  (4 bytes)  │  (4 bytes)   │  (0 – 4096 B)  │
└──────────┴──────────┴─────────────┴──────────────┴─────────────────┘
```

### Tipos de mensagem (`Type`)

| Valor | Nome | Direção | Descrição |
|---|---|---|---|
| `0x01` | `RequestList` | Cliente → Servidor | Solicita lista de arquivos |
| `0x02` | `FileList` | Servidor → Cliente | Resposta com JSON da lista |
| `0x03` | `UploadInit` | Cliente → Servidor | Anuncia início de upload |
| `0x04` | `DownloadInit` | Cliente → Servidor | Solicita download de arquivo |
| `0x05` | `FileChunk` | Bidirecional | Fragmento de arquivo (dados) |
| `0x06` | `Ack` | Receptor → Emissor | Confirmação de chunk recebido |
| `0x07` | `TransferDone` | Emissor → Receptor | Todos os chunks foram enviados |
| `0xFF` | `Error` | Qualquer | Erro ocorreu (payload = mensagem) |

---

## 🔄 Fluxo de Transferência

### Upload (Cliente → Servidor)

```
Cliente                          Servidor
   │── UploadInit (fileName) ──▶│
   │                             │ (cria sessão)
   │── FileChunk Seq=0 ────────▶│
   │◀─ Ack Seq=0 ───────────────│
   │── FileChunk Seq=1 ────────▶│
   │◀─ Ack Seq=1 ───────────────│
   │         ...                 │
   │── TransferDone ────────────▶│
   │                             │ (grava arquivo em disco)
```

### Download (Servidor → Cliente)

```
Cliente                          Servidor
   │── DownloadInit (fileName) ▶│
   │                             │ (lê arquivo, divide em chunks)
   │◀─ FileChunk Seq=0 ─────────│
   │── Ack Seq=0 ──────────────▶│
   │◀─ FileChunk Seq=1 ─────────│
   │── Ack Seq=1 ──────────────▶│
   │         ...                 │
   │◀─ TransferDone ─────────────│
   │                             │
   │ (ordena chunks e reconstrói │
   │  o arquivo em disco)        │
```

### Retransmissão por Timeout

```
Cliente                          Servidor
   │── FileChunk Seq=5 ────────▶│
   │                             │ ✗ pacote descartado (10% loss)
   │   ... 500ms sem ACK ...     │
   │── FileChunk Seq=5 ────────▶│  (retransmissão, attempt 1/10)
   │◀─ Ack Seq=5 ───────────────│
   │── FileChunk Seq=6 ────────▶│
```

---

## 🦈 Wireshark — Capturando os Pacotes

1. Abra o **Wireshark**
2. Selecione a interface de rede (`Loopback` para testes locais)
3. No campo de filtro, digite:

```
udp.port == 5000
```

4. Inicie uma transferência pelo painel Angular
5. Os datagramas aparecem em tempo real

**O que observar:**

- **Camada de rede** → endereço IP de origem e destino
- **Camada de transporte** → porta de origem e porta `5000` de destino
- **Payload** → cabeçalho customizado de 10 bytes seguido dos dados do chunk
- **Padrão ACK** → para cada `FileChunk` enviado, um `Ack` de retorno
- **Retransmissões** → mesmo `SeqNum` aparecendo mais de uma vez

---

## 🗃️ Onde os Arquivos São Salvos

```
backend/
└── ServerFiles/
    ├── arquivo1.pdf        ← arquivos disponíveis para download
    ├── arquivo2.png
    └── uploads/
        ├── enviado1.pdf    ← arquivos recebidos via upload
        └── enviado2.zip
```

---

## 🛠️ Solução de Problemas

**Porta 5000 já está em uso**
```
Encerre o processo que ocupa a porta ou altere a porta
no launchSettings.json e no appsettings.json.
```

**`dotnet: command not found`**
```
O .NET SDK não está instalado ou não está no PATH.
Reinstale em: https://dotnet.microsoft.com/download
```

**`ng: command not found`**
```bash
npm install -g @angular/cli
```

**O painel não recebe eventos em tempo real**
```
Certifique-se de que o backend está rodando antes de
abrir o navegador. A conexão SSE é iniciada assim que
a página carrega.
```

**Arquivo não aparece na lista após upload**
```
Arquivos enviados vão para ServerFiles/uploads/ —
pasta separada dos arquivos disponíveis para download,
que ficam diretamente em ServerFiles/.
```

---

## 👥 Equipe

| Nome | Participação |
|---|---|
|  |  |
|  |  |
|  |  |

---

> Projeto desenvolvido para a disciplina de Redes de Computadores — implementação de confiabilidade sobre UDP com Stop-and-Wait, ACK, timeout e retransmissão.