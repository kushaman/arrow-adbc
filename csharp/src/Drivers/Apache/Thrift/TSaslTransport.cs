using System.Threading.Tasks;
using System.Threading;
using Thrift.Transport;
using Thrift;
using System.Net;
using System.Security.Authentication;
using System.Text;
using System;
using Thrift.Transport.Client;
using Apache.Hive.Service.Rpc.Thrift;
using System.Net.Http.Headers;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    public class TSaslTransport : TEndpointTransport
    {
        private readonly TTransport _innerTransport;
        private readonly ISaslMechanism _saslMechanism;
        //private bool negotiated;
        protected const int MECHANISM_NAME_BYTES = 1;
        protected const int STATUS_BYTES = 1;
        protected const int PAYLOAD_LENGTH_BYTES = 4;

        public TSaslTransport(TTransport innerTransport, ISaslMechanism saslMechanism, TConfiguration config) : base(config)
        {
            _innerTransport = innerTransport;
            _saslMechanism = saslMechanism;
        }

        public override bool IsOpen => _innerTransport.IsOpen;

        public override async Task OpenAsync(CancellationToken cancellationToken = default)
        {
            await _innerTransport.OpenAsync(cancellationToken);
            await NegotiateAsync(cancellationToken);
        }

        public override void Close()
        {
            _innerTransport.Close();
        }

        public override ValueTask<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
        {
            return _innerTransport.ReadAsync(buffer, offset, length, cancellationToken);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
        {
            return _innerTransport.WriteAsync(buffer, offset, length, cancellationToken);
        }

        public override Task FlushAsync(CancellationToken cancellationToken = default)
        {
            return _innerTransport.FlushAsync(cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _innerTransport.Dispose();
            }
        }

        private async Task NegotiateAsync(CancellationToken cancellationToken)
        {
            // Step 1: Send the SASL mechanism name (PLAIN)
            await SendMechanismAsync(_saslMechanism.Name, cancellationToken);

            // Step 2: Send the authentication message (username, password)
            var authMessage = _saslMechanism.EvaluateChallenge(null);
            await SendSaslMessageAsync(NegotiationStatus.Complete, authMessage, cancellationToken);

            // Step 3: Receive server's response (authentication status)
            var serverResponse = await ReceiveSaslMessageAsync();

            // Step 4: Check if the authentication was successful
            if (serverResponse.status == null || serverResponse.status != NegotiationStatus.Complete)
            {
                throw new AuthenticationException("SASL PLAIN authentication failed.");
            }

            _saslMechanism.SetAuthenticated(true);
            //negotiated = true;
        }

        private async Task SendMechanismAsync(string mechanismName, CancellationToken cancellationToken)
        {
            // Send the mechanism name to the server
            byte[] mechanismNameBytes = Encoding.UTF8.GetBytes(mechanismName);
            await SendSaslMessageAsync(NegotiationStatus.Start, mechanismNameBytes, cancellationToken);
        }

        private byte[] messageHeader = new byte[STATUS_BYTES + PAYLOAD_LENGTH_BYTES];

        protected async Task SendSaslMessageAsync(NegotiationStatus status, byte[] payload, CancellationToken cancellationToken)
        {
            if (payload == null) payload = new byte[0];
            // Set status byte
            messageHeader[0] = status.value;
            // Encode payload length (big endian) at offset STATUS_BYTES (usually 1)
            EncodeBigEndian(payload.Length, messageHeader, STATUS_BYTES);
            await _innerTransport.WriteAsync(messageHeader, 0, messageHeader.Length);
            await _innerTransport.WriteAsync(payload, 0, payload.Length);
            await _innerTransport.FlushAsync(cancellationToken);
        }

        protected async Task<SaslResponse> ReceiveSaslMessageAsync(CancellationToken cancellationToken = default)
        {
            await _innerTransport.ReadAllAsync(messageHeader, 0, messageHeader.Length, cancellationToken);

            byte statusByte = messageHeader[0];

            NegotiationStatus? status = NegotiationStatus.ByValue(statusByte);
            if (status == null)
            {
                //throw exception
                throw new TTransportException($"Status is null");
            }

            int payloadBytes = DecodeBigEndian(messageHeader, STATUS_BYTES);
            if (payloadBytes < 0 || payloadBytes > new TConfiguration().MaxMessageSize)
            {
                throw new TTransportException($"out of range");
            }

            byte[] payload = new byte[payloadBytes];
            await _innerTransport.ReadAllAsync(payload, 0, payload.Length, cancellationToken);

            if (status == NegotiationStatus.Bad || status == NegotiationStatus.Error)
            {
                string remoteMessage = Encoding.UTF8.GetString(payload);
                throw new TTransportException($"Peer indicated failure: {remoteMessage}");
            }

            return new SaslResponse(status, payload);
        }

        public static int DecodeBigEndian(byte[] buf, int offset)
        {
            return ((buf[offset] & 0xff) << 24)
                 | ((buf[offset + 1] & 0xff) << 16)
                 | ((buf[offset + 2] & 0xff) << 8)
                 | (buf[offset + 3] & 0xff);
        }

        public static void EncodeBigEndian(int value, byte[] buf, int offset)
        {
            buf[offset] = (byte)((value >> 24) & 0xff);
            buf[offset + 1] = (byte)((value >> 16) & 0xff);
            buf[offset + 2] = (byte)((value >> 8) & 0xff);
            buf[offset + 3] = (byte)(value & 0xff);
        }
    }

    public class SaslResponse
    {
        public NegotiationStatus status;
        public byte[] payload;

        public SaslResponse(NegotiationStatus status, byte[] payload)
        {
            this.status = status;
            this.payload = payload;
        }
    }
}
