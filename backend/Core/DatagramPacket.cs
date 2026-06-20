using System;
using System.Buffers.Binary;
using System.Text;

namespace UdpFileTransfer.Core;

// Tipos de mensagens trocadas entre cliente e servidor no protocolo customizado
public enum MessageType : byte
{
    RequestList  = 0x01,  // Client → Server: pede a lista de arquivos disponíveis
    FileList     = 0x02,  // Server → Client: resposta com JSON da lista de arquivos
    UploadInit   = 0x03,  // Client → Server: avisa que vai enviar um arquivo (payload = nome)
    DownloadInit = 0x04,  // Client → Server: solicita download de um arquivo (payload = nome)
    FileChunk    = 0x05,  // Bidirecional: fragmento de arquivo (payload = dados binários)
    Ack          = 0x06,  // Receptor → Emissor: confirmação de recebimento de um chunk
    TransferDone = 0x07,  // Emissor → Receptor: todos os chunks foram enviados
    Error        = 0xFF,  // Qualquer direção: erro ocorreu (payload = mensagem de texto)
}

// Representa um datagrama UDP com cabeçalho estruturado de 10 bytes.
//
// Formato (big-endian):
//  [0]     Type      (1 byte)
//  [1]     Flags     (1 byte, reservado)
//  [2..5]  SeqNum    (4 bytes)
//  [6..9]  PayloadLen(4 bytes)
//  [10..N] Payload   (0 a 4096 bytes)
public sealed class DatagramPacket
{
    public const int HeaderSize     = 10;
    public const int MaxPayloadSize = 4096;
    public const int MaxPacketSize  = HeaderSize + MaxPayloadSize;

    public MessageType Type    { get; init; }
    public byte        Flags   { get; init; } // Reservado para uso futuro
    public uint        SeqNum  { get; init; } // Número do chunk; nos ACKs indica qual chunk foi confirmado
    public byte[]      Payload { get; init; } = Array.Empty<byte>();

    public int PayloadLength => Payload.Length;

    // Serializa o pacote em bytes para envio via UDP
    public byte[] Serialize()
    {
        var buffer = new byte[HeaderSize + Payload.Length];
        var span   = buffer.AsSpan();

        span[0] = (byte)Type;
        span[1] = Flags;
        BinaryPrimitives.WriteUInt32BigEndian(span[2..6], SeqNum);
        BinaryPrimitives.WriteInt32BigEndian (span[6..10], Payload.Length);
        Payload.CopyTo(span[10..]);

        return buffer;
    }

    // Desserializa bytes recebidos em um DatagramPacket.
    // Lança InvalidDataException se o pacote estiver truncado ou com tamanho inválido.
    public static DatagramPacket Deserialize(ReadOnlySpan<byte> data)
    {
        if (data.Length < HeaderSize)
            throw new InvalidDataException(
                $"Packet too short: {data.Length} bytes (minimum {HeaderSize}).");

        var type       = (MessageType)data[0];
        var flags      = data[1];
        var seqNum     = BinaryPrimitives.ReadUInt32BigEndian(data[2..6]);
        var payloadLen = BinaryPrimitives.ReadInt32BigEndian(data[6..10]);

        if (payloadLen < 0 || payloadLen > MaxPayloadSize)
            throw new InvalidDataException(
                $"Invalid payload length declared in header: {payloadLen}.");

        if (data.Length < HeaderSize + payloadLen)
            throw new InvalidDataException(
                $"Truncated payload: expected {payloadLen} bytes but got {data.Length - HeaderSize}.");

        var payload = data.Slice(HeaderSize, payloadLen).ToArray();

        return new DatagramPacket
        {
            Type    = type,
            Flags   = flags,
            SeqNum  = seqNum,
            Payload = payload,
        };
    }

    // ── Factory helpers ────────────────────────────────────────────────────────

    // ACK sem payload — o SeqNum no cabeçalho já identifica qual chunk está sendo confirmado
    public static DatagramPacket CreateAck(uint seqNum) => new()
    {
        Type   = MessageType.Ack,
        SeqNum = seqNum,
    };

    // Pacote de erro com mensagem descritiva em texto
    public static DatagramPacket CreateError(string message) => new()
    {
        Type    = MessageType.Error,
        Payload = Encoding.UTF8.GetBytes(message),
    };

    // Fragmento de arquivo; usa Span para evitar cópia desnecessária do array de origem
    public static DatagramPacket CreateChunk(uint seqNum, ReadOnlySpan<byte> data) => new()
    {
        Type    = MessageType.FileChunk,
        SeqNum  = seqNum,
        Payload = data.ToArray(),
    };

    // Sinaliza fim de transferência; SeqNum carrega o total de chunks enviados
    public static DatagramPacket CreateDone(uint totalChunks) => new()
    {
        Type   = MessageType.TransferDone,
        SeqNum = totalChunks,
    };

    public string GetPayloadAsString() => Encoding.UTF8.GetString(Payload);

    public override string ToString() =>
        $"[{Type}] Seq={SeqNum} PayloadLen={PayloadLength}";
}