using System.Threading.Tasks;
using System.Threading;
using Thrift.Transport;
using Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    public class TSaslTransport : TEndpointTransport
    {
        private readonly TTransport _innerTransport;
        private readonly ISaslMechanism _saslMechanism;

        public TSaslTransport(TTransport innerTransport, ISaslMechanism saslMechanism, TConfiguration config): base(config)
        {
            _innerTransport = innerTransport;
            _saslMechanism = saslMechanism;
        }

        public override bool IsOpen => _innerTransport.IsOpen;

        public override async Task OpenAsync(CancellationToken cancellationToken = default)
        {
            await _innerTransport.OpenAsync(cancellationToken);
            await _saslMechanism.NegotiateAsync(_innerTransport, cancellationToken);
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
    }
}
