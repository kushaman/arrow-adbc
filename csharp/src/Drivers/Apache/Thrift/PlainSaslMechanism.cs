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
        private readonly string username;
        private readonly string password;
        private bool isComplete = false;
        private bool isAuthenticated = false;

        public PlainSaslMechanism(string username, string password)
        {
            this.username = username;
            this.password = password;
        }

        public string Name => "PLAIN";


        public byte[] EvaluateChallenge(byte[]? challenge)
        {
            if (isComplete)
                return Array.Empty<byte>(); // no more responses

            var authzid = ""; // empty authorization identity
            string message = $"{authzid}\0{username}\0{password}";
            isComplete = true; // only one message in PLAIN

            return System.Text.Encoding.UTF8.GetBytes(message);
        }

        public bool IsComplete => isComplete;
        public bool IsAuthenticated => isComplete && isAuthenticated;

        public void SetAuthenticated(bool success)
        {
            isAuthenticated = success;
        }
    }
}
