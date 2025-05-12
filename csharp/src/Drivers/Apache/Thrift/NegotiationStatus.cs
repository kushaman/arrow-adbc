using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    public class NegotiationStatus
    {
        public static readonly NegotiationStatus Start = new NegotiationStatus(0x01);
        public static readonly NegotiationStatus Ok = new NegotiationStatus(0x02);
        public static readonly NegotiationStatus Bad = new NegotiationStatus(0x03);
        public static readonly NegotiationStatus Error = new NegotiationStatus(0x04);
        public static readonly NegotiationStatus Complete = new NegotiationStatus(0x05);

        public byte value { get; }

        private NegotiationStatus(byte value)
        {
            this.value = value;
        }

        public static NegotiationStatus? ByValue(byte value)
        {
            return value switch
            {
                0x01 => Start,
                0x02 => Ok,
                0x03 => Bad,
                0x04 => Error,
                0x05 => Complete,
                _ => null
            };
        }

        public byte GetValue()
        {
            return this.value;
        }
    }
}
