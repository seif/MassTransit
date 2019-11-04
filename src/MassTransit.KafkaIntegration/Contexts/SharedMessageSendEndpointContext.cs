namespace MassTransit.KafkaIntegration.Contexts
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using GreenPipes;


    public class SharedMessageSendEndpointContext<TKey, TBody> :
        MessageSendEndpointContext<TKey, TBody>
    {
        readonly MessageSendEndpointContext<TKey, TBody> _context;

        public SharedMessageSendEndpointContext(MessageSendEndpointContext<TKey, TBody> context, CancellationToken cancellationToken)
        {
            CancellationToken = cancellationToken;
            _context = context;
        }

        bool PipeContext.HasPayloadType(Type payloadType)
        {
            return _context.HasPayloadType(payloadType);
        }

        bool PipeContext.TryGetPayload<TPayload>(out TPayload payload)
        {
            return _context.TryGetPayload(out payload);
        }

        TPayload PipeContext.GetOrAddPayload<TPayload>(PayloadFactory<TPayload> payloadFactory)
        {
            return _context.GetOrAddPayload(payloadFactory);
        }

        T PipeContext.AddOrUpdatePayload<T>(PayloadFactory<T> addFactory, UpdatePayloadFactory<T> updateFactory)
        {
            return _context.AddOrUpdatePayload(addFactory, updateFactory);
        }

        public CancellationToken CancellationToken { get; }

        string MessageSendEndpointContext<TKey, TBody>.EntityPath => _context.EntityPath;
        public Task Send(Message<TKey, TBody> message)
        {
            return _context.Send(message);
        }
    }
}
