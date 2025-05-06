using System;
using System.Threading;
using Thrift.Transport;


namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class PlainSaslMechanism : ISaslMechanism
    {
        private readonly string _expectedUsername;
        private readonly string _expectedPassword;

        public string Name => "PLAIN";

        public PlainSaslMechanism(string expectedUsername, string expectedPassword)
        {
            _expectedUsername = expectedUsername;
            _expectedPassword = expectedPassword;
        }

        public async Task NegotiateAsync(TTransport transport, CancellationToken cancellationToken = default)
        {
            byte[] buffer = new byte[1024];
            int bytesRead = await transport.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
            string credentials = Encoding.UTF8.GetString(buffer, 0, bytesRead);

            string[] parts = credentials.Split('\0');
            if (parts.Length < 3)
            {
                throw new Exception("Authentication failed: Malformed credentials.");
            }

            string username = parts[1];
            string password = parts[2];

            if (username != _expectedUsername || password != _expectedPassword)
            {
                throw new Exception("Authentication failed: Invalid username or password.");
            }
        }
    }
}
