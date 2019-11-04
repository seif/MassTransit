namespace MassTransit.KafkaIntegration.Contexts
{
    using System.IO;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using GreenPipes;

    public interface MessageSendEndpointContext<TKey, TBody> :
        PipeContext
    {
        /// <summary>
        /// The path of the messaging entity
        /// </summary>
        string EntityPath { get; }

        /// <summary>
        /// Send the message to the messaging entity
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        Task Send(Message<TKey, TBody> message);
    }
}
