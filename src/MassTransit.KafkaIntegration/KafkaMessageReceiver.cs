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
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Context;
    using Contexts;
    using GreenPipes;
    using GreenPipes.Agents;
    using Transports;


    /// <summary>
    /// </summary>
    public class KafkaMessageReceiver<TKey> :
        Supervisor,
        IReceiveTransport
    {
        readonly Uri _inputAddress;
        readonly ReceiveEndpointContext _receiveEndpointContext;

        public KafkaMessageReceiver(Uri inputAddress, ReceiveEndpointContext receiveEndpointContext)
        {
            _inputAddress = inputAddress;
            _receiveEndpointContext = receiveEndpointContext;
        }

        void IProbeSite.Probe(ProbeContext context)
        {
            var scope = context.CreateScope("receiver");
            scope.Add("type", "brokeredMessage");
        }

        ConnectHandle IReceiveObserverConnector.ConnectReceiveObserver(IReceiveObserver observer)
        {
            return _receiveEndpointContext.ConnectReceiveObserver(observer);
        }

        ConnectHandle IPublishObserverConnector.ConnectPublishObserver(IPublishObserver observer)
        {
            return _receiveEndpointContext.ConnectPublishObserver(observer);
        }

        ConnectHandle ISendObserverConnector.ConnectSendObserver(ISendObserver observer)
        {
            return _receiveEndpointContext.ConnectSendObserver(observer);
        }

        async Task Reciever(Message<TKey, byte[]> message, Action<ReceiveContext> contextCallback)
        {
            var context = new KafkaMessageReceiveContext<TKey>(_inputAddress, message, _receiveEndpointContext);
            contextCallback?.Invoke(context);

            try
            {
                await _receiveEndpointContext.ReceiveObservers.PreReceive(context).ConfigureAwait(false);
                await _receiveEndpointContext.ReceivePipe.Send(context).ConfigureAwait(false);

                await context.ReceiveCompleted.ConfigureAwait(false);

                await _receiveEndpointContext.ReceiveObservers.PostReceive(context).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await _receiveEndpointContext.ReceiveObservers.ReceiveFault(context, ex).ConfigureAwait(false);
            }
            finally
            {
                context.Dispose();
            }
        }

        public ConnectHandle ConnectReceiveTransportObserver(IReceiveTransportObserver observer)
        {
            return _receiveEndpointContext.ConnectReceiveTransportObserver(observer);
        }

        public ReceiveTransportHandle Start()
        {
            // todo fix this
            // create a consumer, passing in the reciever SEIF
            return new Handle(this);
        }


        public class Handle : ReceiveTransportHandle
        {
            IAgent _agent;

            public Handle(IAgent agent)
            {
                _agent = agent;
            }

            public Task Stop(CancellationToken cancellationToken = default(CancellationToken))
            {
                return _agent.Stop("Stop receive transport", cancellationToken);
            }
        }
    }
}
