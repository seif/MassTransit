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
    using Contexts;
    using Logging;
    using Transports;


    public class KafkaSendTransportProvider<TKey> :
        ISendTransportProvider
    {
        //readonly IBinder _binder;
        readonly CancellationToken _cancellationToken;
        readonly ProducerBuilder<TKey, byte[]> _builder;
        readonly ILog _log;

        public KafkaSendTransportProvider(ILog log, CancellationToken cancellationToken)
        {
            _log = log;
            _cancellationToken = cancellationToken;
        }

        async Task<ISendTransport> ISendTransportProvider.GetSendTransport(Uri address)
        {
            var eventHubName = address.AbsolutePath.Trim('/');

            var producerConfig = new ProducerConfig(new ClientConfig()
            {
                BootstrapServers = address.ToString(),
                Acks = Acks.All
            });
            var producerBuilder = new ProducerBuilder<TKey, byte[]>(producerConfig);
            var client = new ProducerSendEndpointContext<TKey>(eventHubName, producerBuilder.Build());


            var source = new CollectorEventDataSendEndpointContextSource<TKey, byte[]>(client);

            var transport = new KafkaSendTransport<TKey>(source, address);

            return transport;
        }
    }
}
