using System.Threading.Tasks;
using System.Threading;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    public interface ISaslMechanism
    {
        string Name { get; } // returns "PLAIN"
        byte[] EvaluateChallenge(byte[]? challenge); // create authentication payload
        bool IsComplete { get; }
        bool IsAuthenticated { get; }
        void SetAuthenticated(bool success);
    }
}
