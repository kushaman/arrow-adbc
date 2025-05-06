using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    public class SaslNegotiator
    {
        private readonly Dictionary<string, ISaslMechanism> _mechanisms;

        public SaslNegotiator()
        {
            _mechanisms = new Dictionary<string, ISaslMechanism>();
        }

        public void RegisterMechanism(ISaslMechanism mechanism)
        {
            _mechanisms[mechanism.Name] = mechanism;
        }
    }
}
