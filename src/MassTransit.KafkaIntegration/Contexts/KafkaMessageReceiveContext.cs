namespace MassTransit.KafkaIntegration.Contexts
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Confluent.Kafka;
    using Context;


    public sealed class KafkaMessageReceiveContext<TKey> :
        BaseReceiveContext,
        KafkaMessageContext
    {
        readonly Message<TKey, byte[]> _message;
        byte[] _body;

        public KafkaMessageReceiveContext(Uri inputAddress, Message<TKey, byte[]> message, ReceiveEndpointContext receiveEndpointContext)
            : base(inputAddress, false, receiveEndpointContext)
        {
            _message = message;

            GetOrAddPayload<KafkaMessageContext>(() => this);
        }

        protected override IHeaderProvider HeaderProvider => new HeaderProvider(_message.Headers);

        public override byte[] GetBody()
        {
            return _message.Value;
        }

        public override Stream GetBodyStream()
        {
            return new MemoryStream(GetBody(), false);
        }
    }
}
