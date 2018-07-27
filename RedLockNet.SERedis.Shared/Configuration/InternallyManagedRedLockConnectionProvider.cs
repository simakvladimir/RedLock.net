using System;
using System.Collections.Generic;
using System.Linq;
using log4net;
using RedLockNet.SERedis.Internal;
using RedLockNet.SERedis.Util;
using StackExchange.Redis;

namespace RedLockNet.SERedis.Configuration
{
    /// <summary>
    /// A connection provider that manages its own connections to Redis
    /// </summary>
    public class InternallyManagedRedLockConnectionProvider : RedLockConnectionProvider
	{
        private readonly ILog logger = LogManager.GetLogger(typeof(InternallyManagedRedLockConnectionProvider));
        public IList<RedLockEndPoint> EndPoints { get; set; }

		private ICollection<RedisConnection> connections;

		private const int DefaultConnectionTimeout = 100;
		private const int DefaultSyncTimeout = 1000;
		private const int DefaultConfigCheckSeconds = 10;


	    public InternallyManagedRedLockConnectionProvider()
	    {
	        this.EndPoints = new List<RedLockEndPoint>();
        }

        internal override ICollection<RedisConnection> CreateRedisConnections()
		{
			if (this.EndPoints == null || !this.EndPoints.Any())
			{
				throw new ArgumentException("No endpoints specified");
			}

			connections = new List<RedisConnection>(this.EndPoints.Count);

			foreach (var endPoint in this.EndPoints)
			{
				var redisConfig = new ConfigurationOptions
				{
					AbortOnConnectFail = false,
					ConnectTimeout = endPoint.ConnectionTimeout ?? DefaultConnectionTimeout,
					SyncTimeout = endPoint.SyncTimeout ?? DefaultSyncTimeout,
					Ssl = endPoint.Ssl,
					Password = endPoint.Password,
					ConfigCheckSeconds = endPoint.ConfigCheckSeconds ?? DefaultConfigCheckSeconds
				};

				foreach (var e in endPoint.EndPoints)
				{
					redisConfig.EndPoints.Add(e);
				}

				var redisConnection = new RedisConnection
				{
					ConnectionMultiplexer = ConnectionMultiplexer.Connect(redisConfig),
					RedisDatabase = endPoint.RedisDatabase ?? DefaultRedisDatabase,
					RedisKeyFormat = string.IsNullOrEmpty(endPoint.RedisKeyFormat) ? DefaultRedisKeyFormat : endPoint.RedisKeyFormat
				};

				redisConnection.ConnectionMultiplexer.ConnectionFailed += (sender, args) =>
				{
					logger.Warn($"ConnectionFailed: {args.EndPoint.GetFriendlyName()} ConnectionType: {args.ConnectionType} FailureType: {args.FailureType}");
				};

				redisConnection.ConnectionMultiplexer.ConnectionRestored += (sender, args) =>
				{
					logger.Warn($"ConnectionRestored: {args.EndPoint.GetFriendlyName()} ConnectionType: {args.ConnectionType} FailureType: {args.FailureType}");
				};

				redisConnection.ConnectionMultiplexer.ConfigurationChanged += (sender, args) =>
				{
					logger.Warn($"ConfigurationChanged: {args.EndPoint.GetFriendlyName()}");
				};

				redisConnection.ConnectionMultiplexer.ConfigurationChangedBroadcast += (sender, args) =>
				{
					logger.Warn($"ConfigurationChangedBroadcast: {args.EndPoint.GetFriendlyName()}");
				};

				redisConnection.ConnectionMultiplexer.ErrorMessage += (sender, args) =>
				{
					logger.Warn($"ErrorMessage: {args.EndPoint.GetFriendlyName()} Message: {args.Message}");
				};

				connections.Add(redisConnection);
			}

			return connections;
		}

		internal override void DisposeConnections()
		{
			foreach (var connection in this.connections)
			{
				connection.ConnectionMultiplexer.Dispose();
			}
		}
	}
}