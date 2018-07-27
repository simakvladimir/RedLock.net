using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RedLockNet.SERedis.Configuration;
using RedLockNet.SERedis.Internal;

namespace RedLockNet.SERedis
{
	public class RedLockFactory : IDistributedLockFactory, IDisposable
	{
		private readonly RedLockConfiguration configuration;
		private readonly ICollection<RedisConnection> redisCaches;

		/// <summary>
		/// Create a RedLockFactory using a list of RedLockEndPoints (ConnectionMultiplexers will be internally managed by RedLock.net)
		/// </summary>
		public static RedLockFactory Create(IList<RedLockEndPoint> endPoints)
		{
			var configuration = new RedLockConfiguration(endPoints);
			return new RedLockFactory(configuration);
		}

		/// <summary>
		/// Create a RedLockFactory using existing StackExchange.Redis ConnectionMultiplexers
		/// </summary>
		public static RedLockFactory Create(IList<RedLockMultiplexer> existingMultiplexers)
		{
			var configuration = new RedLockConfiguration(
				new ExistingMultiplexersRedLockConnectionProvider
				{
					Multiplexers = existingMultiplexers
				});

			return new RedLockFactory(configuration);
		}

		/// <summary>
		/// Create a RedLockFactory using the specified configuration
		/// </summary>
		public RedLockFactory(RedLockConfiguration configuration)
		{
			this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration), "Configuration must not be null");
			this.redisCaches = configuration.ConnectionProvider.CreateRedisConnections();
		}

		public IRedLock CreateLock(string resource, TimeSpan expiryTime)
		{
			return RedLock.Create(
				redisCaches,
				resource,
				expiryTime);
		}

#if !NET40
		public async Task<IRedLock> CreateLockAsync(string resource, TimeSpan expiryTime)
		{
			return await RedLock.CreateAsync(
				redisCaches,
				resource,
				expiryTime).ConfigureAwait(false);
		}
#endif

		public IRedLock CreateLock(string resource, TimeSpan expiryTime, TimeSpan waitTime, TimeSpan retryTime, CancellationToken? cancellationToken = null)
		{
			return RedLock.Create(
				redisCaches,
				resource,
				expiryTime,
				waitTime,
				retryTime,
				cancellationToken ?? CancellationToken.None);
		}

#if !NET40
        public async Task<IRedLock> CreateLockAsync(string resource, TimeSpan expiryTime, TimeSpan waitTime, TimeSpan retryTime, CancellationToken? cancellationToken = null)
		{
			return await RedLock.CreateAsync(
				redisCaches,
				resource,
				expiryTime,
				waitTime,
				retryTime,
				cancellationToken ?? CancellationToken.None).ConfigureAwait(false);
		}
#endif

		public void Dispose()
		{
			this.configuration.ConnectionProvider.DisposeConnections();
		}
	}
}