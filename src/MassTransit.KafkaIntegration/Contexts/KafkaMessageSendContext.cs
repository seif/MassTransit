namespace MassTransit.KafkaIntegration.Contexts
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using Context;


    public class KafkaMessageSendContext<TKey,T> :
        BaseSendContext<T>
        where T : class
    {
        public KafkaMessageSendContext(T message, CancellationToken cancellationToken)
            : base(message, cancellationToken)
        {
        }

        public TKey PartitionKey { get; set; }
    }
}
