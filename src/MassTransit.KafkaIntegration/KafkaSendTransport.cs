// Copyright 2007-2018 Chris Patterson, Dru Sellers, Travis Smith, et. al.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
namespace MassTransit.KafkaIntegration
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Contexts;
    using GreenPipes;
    using GreenPipes.Agents;
    using Logging;
    using Pipeline.Observables;
    using Transports;


    /// <summary>
    /// Send message to an Event Hub
    /// </summary>
    public class KafkaSendTransport<TKey> :
        Supervisor,
        ISendTransport
    {
        static readonly ILog _log = Logger.Get<KafkaSendTransport<TKey>>();
        readonly Uri _address;
        readonly SendObservable _observers;

        readonly IPipeContextSource<MessageSendEndpointContext<TKey, byte[]>> _source;

        public KafkaSendTransport(IPipeContextSource<MessageSendEndpointContext<TKey, byte[]>> source, Uri address)
        {
            _source = source;
            _address = address;
            _observers = new SendObservable();
        }

        Task ISendTransport.Send<T>(T value, IPipe<SendContext<T>> pipe, CancellationToken cancellationToken)
        {
            IPipe<MessageSendEndpointContext<TKey, byte[]>> clientPipe = Pipe.ExecuteAsync<MessageSendEndpointContext<TKey, Byte[]>>(async clientContext =>
            {
                var context = new KafkaMessageSendContext<TKey, T>(value, cancellationToken);

                try
                {
                    await pipe.Send(context).ConfigureAwait(false);

                    await _observers.PreSend(context).ConfigureAwait(false);

                    var message = new Message<TKey, byte[]>
                    {
                        Key = context.PartitionKey,
                        Value = context.Body
                    };

                    message.Headers.AddTextHeadersAsByteArray(context.Headers);
                    await clientContext.Send(message).ConfigureAwait(false);
                    context.LogSent();
                    await _observers.PostSend(context).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await _observers.SendFault(context, ex).ConfigureAwait(false);

                    throw;
                }
            });

            return _source.Send(clientPipe, cancellationToken);
        }

        public ConnectHandle ConnectSendObserver(ISendObserver observer)
        {
            return _observers.Connect(observer);
        }

        protected override Task StopSupervisor(StopSupervisorContext context)
        {
            if (_log.IsDebugEnabled)
                _log.DebugFormat("Stopping transport: {0}", _address);

            return base.StopSupervisor(context);
        }
    }


    public static class HeadersExtensions
    {
        public static void AddTextHeadersAsByteArray(this Headers headers, SendHeaders contextHeaders)
        {
            foreach (var header in contextHeaders.GetAll())
            {
                if (header.Value == null)
                {
                    if (headers.Any(h => h.Key == header.Key))
                        headers.Remove(header.Key);

                    continue;
                }

                if (headers.Any(h => h.Key == header.Key))
                    continue;

                if (header.Value is string stringValue)
                {
                    headers.Add(header.Key, Encoding.UTF8.GetBytes(stringValue));
                }
                else if (header.Value is IFormattable formatValue && formatValue.GetType().IsValueType)
                {
                    headers.Add(header.Key, Encoding.UTF8.GetBytes(formatValue.ToString()));
                }
            }
        }
    }
}
