using System.Threading.Tasks;
using System.Threading;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    public interface ISaslMechanism
    {
        string Name { get; }
        Task NegotiateAsync(TTransport transport, CancellationToken cancellationToken = default);
    }
}
