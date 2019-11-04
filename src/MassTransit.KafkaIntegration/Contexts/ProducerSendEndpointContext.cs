﻿// Copyright 2007-2018 Chris Patterson, Dru Sellers, Travis Smith, et. al.
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
    using System.IO;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using GreenPipes;

    public class ProducerSendEndpointContext<TKey> :
        BasePipeContext,
        MessageSendEndpointContext<TKey, byte[]>
    {
        readonly IProducer<TKey, byte[]> _producer;

        public ProducerSendEndpointContext(string path, IProducer<TKey, byte[]> producer)
        {
            _producer = producer;
            EntityPath = path;
        }

        public string EntityPath { get; }
        public Task Send(Message<TKey, byte[]> message)
        {
            return _producer.ProduceAsync(EntityPath, message);
        }
    }
}
