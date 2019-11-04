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
namespace MassTransit.KafkaIntegration.Contexts
{
    using System.Threading;
    using Confluent.Kafka;
    using Context;
    using KafkaIntegration;
    using Logging;
    using MassTransit.Configuration;


    public class KafkaReceiveEndpointContext<TKey> :
        BaseReceiveEndpointContext
    {
        readonly CancellationToken _cancellationToken;
        readonly ILog _log;
        readonly ProducerBuilder<TKey, byte[]> _builder;

        public KafkaReceiveEndpointContext(IReceiveEndpointConfiguration configuration, ILog log, ProducerBuilder<TKey, byte[]> builder,
            CancellationToken cancellationToken)
            : base(configuration)
        {
            _cancellationToken = cancellationToken;
            _log = log;
            _builder = builder;
        }

        protected override ISendTransportProvider CreateSendTransportProvider()
        {
            return new KafkaSendTransportProvider<TKey>(_log, _cancellationToken);
        }

        protected override IPublishTransportProvider CreatePublishTransportProvider()
        {
            return new KafkaPublishTransportProvider(SendTransportProvider);
        }
    }
}
