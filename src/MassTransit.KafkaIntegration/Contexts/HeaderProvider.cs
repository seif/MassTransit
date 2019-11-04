namespace MassTransit.KafkaIntegration.Contexts
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Confluent.Kafka;
    using Context;
    using Transports;


    public class HeaderProvider : IHeaderProvider
    {
        readonly Headers _headers;

        public HeaderProvider(Headers headers)
        {
            _headers = headers;
        }

        public IEnumerable<KeyValuePair<string, object>> GetAll()
        {
            return _headers.Select(x => (new KeyValuePair<string, object>(x.Key, x.GetValueBytes())));
        }

        public bool TryGetHeader(string key, out object value)
        {
            value = default;
            byte[] byteValue;
            if (_headers.TryGetLastBytes(key, out byteValue))
            {
                value = byteValue;
                return true;
            }

            return false;
        }
    }
}
