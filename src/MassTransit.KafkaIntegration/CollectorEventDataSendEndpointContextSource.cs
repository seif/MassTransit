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
    using Contexts;
    using GreenPipes;


    public class CollectorEventDataSendEndpointContextSource<TKey, TBody> :
        IPipeContextSource<MessageSendEndpointContext<TKey, TBody>>
    {
        readonly MessageSendEndpointContext<TKey, TBody> _context;

        public CollectorEventDataSendEndpointContextSource(MessageSendEndpointContext<TKey, TBody> context)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            _context = context;
        }

        public Task Send(IPipe<MessageSendEndpointContext<TKey, TBody>> pipe, CancellationToken cancellationToken = default)
        {
            var sharedContext = new SharedMessageSendEndpointContext<TKey, TBody>(_context, cancellationToken);

            return pipe.Send(sharedContext);
        }

        public void Probe(ProbeContext context)
        {
            context.CreateFilterScope("binderSource");
        }
    }
}
